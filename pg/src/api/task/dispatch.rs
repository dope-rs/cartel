use std::{mem, num::NonZero, pin::Pin, task::Poll};

use cartel_core::deadline;
use dope::{
    core::driver::{
        route::KeyTag,
        schedule::clock::{self, timer},
    },
    manifold::client::conn,
};
use dope_fiber::{context, wait};
use o3::base::time::Positive;

use crate::{api, port::requests, reply::Error, schema};

use crate::api::task::{State, lease};

impl<
    'app,
    'g,
    'r,
    's,
    'b,
    'c,
    'd: 'app,
    B: ?Sized,
    I: schema::QuerySet + 'd,
    const ID: u8,
    S,
    A: lease::Target<'d, ID>,
    P: lease::Policy,
    H,
> State<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID, S, A, P, H>
{
    pub(super) fn new(
        target: Option<api::Target<'d, ID>>,
        next: Result<api::Pending<'d, S>, crate::Error>,
    ) -> Self {
        match next {
            Ok(request) => State::Connecting { target, request },
            Err(error) => State::Failed(error),
        }
    }

    pub(super) fn phase(&self) -> deadline::RequestPhase {
        match self {
            State::Admitted(..) => deadline::RequestPhase::Response,
            _ => deadline::RequestPhase::Admission,
        }
    }

    pub(super) fn cancel(&mut self, error: crate::Error) -> Error<'d, ID> {
        match mem::replace(self, State::Done) {
            State::Failed(error) => return error.into(),
            State::Admitted(lease, _) => lease.abort(error),
            _ => {}
        }
        error.into()
    }

    pub(super) fn poll_with<'target>(
        &mut self,
        client: &api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
        mut cx: Pin<&mut context::Context<'_, 'd>>,
        waiter: Pin<&wait::Waiter<'target, 'd>>,
        prepare: impl FnOnce(
            &mut Self,
            api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
            NonZero<usize>,
            requests::ReplyContext<'_, 'app, 'd, ID>,
        ) -> Result<(), (crate::Error, conn::Id<'d, KeyTag<ID>>)>,
    ) -> Poll<Result<(), Error<'d, ID>>>
    where
        'app: 'target,
        'd: 'target,
    {
        let (client, target, request) = match &*self {
            State::Connecting { target, request } => (*client, target, request),
            State::Admitted(..) => return Poll::Ready(Ok(())),
            State::Failed(_) | State::Done => {
                return Poll::Ready(Err(self.cancel(crate::Error::Closed)));
            }
        };
        client
            .port
            .maintain(cx.as_ref().work(), cx.as_mut().region_token());
        let context = 'selected: {
            let error = 'failed: {
                if let Err(error) = api::check_open(client) {
                    break 'failed error;
                }
                let ready = &client.port.shared.ready;
                match ready.select(target, request.boundary, cx.as_mut().region_token()) {
                    Ok(Some(context)) => break 'selected context,
                    Ok(None) => {}
                    Err(target) => break 'failed Error::Pinned(target),
                }
                if target.is_none()
                    && let Some(count) = ready.blocking()
                {
                    break 'failed client.port.backpressure(count).into();
                }
                match ready.try_register(waiter, cx.as_ref()) {
                    Ok(()) => return Poll::Pending,
                    Err(error) => Error::Wait(error),
                }
            };
            let request = mem::replace(self, State::Done);
            waiter.unregister();
            drop(request);
            return Poll::Ready(Err(error));
        };
        let weight = request.frame.geometry().wire().widen();
        let (error, conn) = match prepare(self, client, weight, context) {
            Ok(()) => {
                waiter.unregister();
                return Poll::Ready(Ok(()));
            }
            Err(failure) => failure,
        };
        let throttle = match api::Throttle::new(conn, error) {
            Ok(throttle) => throttle,
            Err(error) => {
                drop(mem::replace(self, State::Done));
                waiter.unregister();
                return Poll::Ready(Err(error.into()));
            }
        };
        if let Err(error) =
            client
                .port
                .shared
                .signals
                .egress
                .try_register(&throttle.cause, waiter, cx.as_ref())
        {
            let error = throttle.fail(error);
            drop(mem::replace(self, State::Done));
            waiter.unregister();
            return Poll::Ready(Err(error));
        }
        // No order was published: a later poll must revalidate the original
        // target before selecting a connection that may now belong to a transaction.
        Poll::Pending
    }
}

enum BudgetState<'d> {
    Fresh(Positive),
    Armed(clock::Deadline<'d>),
    Elapsed,
}

pub(in crate::api) struct Budget<'d> {
    timer: timer::Timer<'d>,
    state: BudgetState<'d>,
}

impl<'d> Budget<'d> {
    pub(in crate::api) fn new(source: deadline::Source<'d>) -> Self {
        let (timer, window) = source.into_parts();
        Self {
            timer,
            state: BudgetState::Fresh(window),
        }
    }

    pub(in crate::api) fn guard(&self) -> Result<timer::Alarm<'d>, timer::Exhausted> {
        self.timer.try_alarm()
    }

    pub(in crate::api) fn poll_expired(
        &mut self,
        guard: Pin<&mut timer::Alarm<'d>>,
        mut ctx: Pin<&mut context::Context<'_, 'd>>,
        phase: deadline::RequestPhase,
    ) -> Poll<deadline::Elapsed> {
        let now = ctx.as_mut().driver().driver_ref().clock().now();
        let due = match self.state {
            BudgetState::Elapsed => return Poll::Ready(deadline::Elapsed::new(phase)),
            BudgetState::Armed(due) => due,
            BudgetState::Fresh(window) => {
                let due = now.clamp_add(window);
                self.state = BudgetState::Armed(due);
                due
            }
        };
        let alarm = guard.get_mut();
        if now >= due {
            self.state = BudgetState::Elapsed;
            return Poll::Ready(deadline::Elapsed::new(phase));
        }
        let wake = ctx.as_ref().completion();
        if !alarm.is_armed() {
            alarm.arm(due, wake);
            return Poll::Pending;
        }
        if alarm.poll(now, wake).is_ready() {
            self.state = BudgetState::Elapsed;
            Poll::Ready(deadline::Elapsed::new(phase))
        } else {
            Poll::Pending
        }
    }
}
