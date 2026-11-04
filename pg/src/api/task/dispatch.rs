use std::{mem, pin::Pin, task::Poll};

use cartel_core::{deadline, reply::recv::attach::Attach};
use dope::{
    core::driver::{
        route::KeyTag,
        schedule::clock::{self, timer},
    },
    manifold::client::conn,
};
use dope_fiber::{context, wait};
use o3::base::{cell::region, time::Positive};

use crate::{
    api,
    port::{replies, requests},
    reply::Error,
    schema,
};

use crate::api::task::{State, lease};

pub(super) struct Dispatch<
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
    H = (),
> {
    pub(super) deadline: Budget<'d>,
    pub(super) state: State<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID, S, A, P, H>,
}

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
> Dispatch<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID, S, A, P, H>
{
    pub(super) fn new(
        client: api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
        target: Option<api::Target<'d, ID>>,
        next: api::Outcome<'d, ID, S>,
    ) -> Self {
        let state = match next {
            api::Outcome::NoConn { request } => State::Connecting {
                client,
                target,
                request,
            },
            api::Outcome::Failed(error) => State::Failed(error),
        };
        let (timer, window) = client.port.lifecycle.deadline.into_parts();
        Self {
            deadline: Budget {
                timer,
                window,
                state: BudgetState::Fresh,
            },
            state,
        }
    }

    pub(super) fn phase(&self) -> deadline::RequestPhase {
        match self.state {
            State::Admitted(..) => deadline::RequestPhase::Response,
            _ => deadline::RequestPhase::Admission,
        }
    }

    pub(super) fn cancel(&mut self, error: crate::Error) -> Error<'d, ID> {
        match mem::replace(&mut self.state, State::Done) {
            State::Failed(error) => return error,
            State::Admitted(lease, _) => lease.abort(error),
            _ => {}
        }
        error.into()
    }

    pub(super) fn poll_with<'target>(
        &mut self,
        mut cx: Pin<&mut context::Context<'_, 'd>>,
        waiter: Pin<&wait::Waiter<'target, 'd>>,
        prepare: impl FnOnce(
            &mut Self,
            api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
            conn::Id<'d, KeyTag<ID>>,
            api::Pending<'d, S>,
            &mut region::Token<region::Identity<'d>>,
        ) -> Result<(), (crate::Error, api::Pending<'d, S>)>,
    ) -> Poll<Result<(), Error<'d, ID>>>
    where
        'app: 'target,
        'd: 'target,
    {
        let (client, target, request) = match mem::replace(&mut self.state, State::Done) {
            State::Connecting {
                client,
                target,
                request,
            } => (client, target, request),
            State::Admitted(lease, held) => {
                self.state = State::Admitted(lease, held);
                return Poll::Ready(Ok(()));
            }
            State::Failed(error) => return Poll::Ready(Err(error)),
            State::Done => return Poll::Ready(Err(crate::Error::Closed.into())),
        };
        let conn = match client.select(target, request.boundary) {
            Ok(Some(conn)) => conn,
            Ok(None) => {
                if let Err(error) = client.port.shared.ready.try_register(waiter, cx.as_ref()) {
                    waiter.unregister();
                    return Poll::Ready(Err(Error::Wait(error)));
                }
                self.state = State::Connecting {
                    client,
                    target,
                    request,
                };
                return Poll::Pending;
            }
            Err(error) => {
                waiter.unregister();
                return Poll::Ready(Err(error));
            }
        };
        let (error, request) =
            match prepare(self, client, conn, request, cx.as_mut().region_token()) {
                Ok(()) => {
                    waiter.unregister();
                    return Poll::Ready(Ok(()));
                }
                Err(failure) => failure,
            };
        let throttle = match api::Throttle::new(conn, error, request) {
            Ok(throttle) => throttle,
            Err(error) => {
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
            waiter.unregister();
            return Poll::Ready(Err(error));
        }
        // No order was published: a later poll must revalidate the original
        // target before selecting a connection that may now belong to a transaction.
        self.state = State::Connecting {
            client,
            target,
            request: throttle.request,
        };
        Poll::Pending
    }
}

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
> Dispatch<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID, S, A, P>
{
    pub(super) fn poll_admit<'target>(
        &mut self,
        cx: Pin<&mut context::Context<'_, 'd>>,
        waiter: Pin<&wait::Waiter<'target, 'd>>,
        reply: &mut impl Attach<
            'app,
            'b,
            'c,
            'd,
            replies::Row<'d>,
            replies::ReplyFailure<'d>,
            replies::Tag<'g, 'r, 's, 'b, 'c, 'd>,
        >,
        seal: impl Copy
        + Fn(
            S,
            api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
            conn::Id<'d, KeyTag<ID>>,
        ) -> lease::Lease<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, A, P, ID>,
    ) -> Poll<Result<(), Error<'d, ID>>>
    where
        'app: 'target,
        'd: 'target,
    {
        self.poll_with(cx, waiter, |this, client, conn, request, region| {
            let api::Pending {
                frame,
                boundary,
                state,
            } = request;
            let context = requests::ReplyContext::new(conn, boundary, region);
            client
                .port
                .request()
                .try_reply(frame, (this, state), reply, context, |(this, state)| {
                    this.state = State::Admitted(seal(state, client, conn), ())
                })
                .map_err(|(error, frame, (_, state))| {
                    (
                        error,
                        api::Pending {
                            frame,
                            boundary,
                            state,
                        },
                    )
                })
        })
    }
}

enum BudgetState<'d> {
    Fresh,
    Armed(clock::Deadline<'d>),
    Elapsed,
}

pub(super) struct Budget<'d> {
    timer: timer::Timer<'d>,
    window: Positive,
    state: BudgetState<'d>,
}

impl<'d> Budget<'d> {
    pub(super) fn guard(&self) -> Result<timer::Alarm<'d>, timer::Exhausted> {
        self.timer.try_alarm()
    }

    pub(super) fn poll_expired(
        &mut self,
        guard: Pin<&mut timer::Alarm<'d>>,
        mut ctx: Pin<&mut context::Context<'_, 'd>>,
        phase: deadline::RequestPhase,
    ) -> Poll<deadline::Elapsed> {
        let wake = ctx.as_ref().completion();
        let now = ctx.as_mut().driver().driver_ref().clock().now();
        let due = match self.state {
            BudgetState::Elapsed => return Poll::Ready(deadline::Elapsed::new(phase)),
            BudgetState::Armed(due) => due,
            BudgetState::Fresh => {
                let due = now.clamp_add(self.window);
                self.state = BudgetState::Armed(due);
                due
            }
        };
        let alarm = guard.get_mut();
        if now >= due {
            alarm.clear();
            self.state = BudgetState::Elapsed;
            return Poll::Ready(deadline::Elapsed::new(phase));
        }
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
