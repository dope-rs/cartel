use std::{mem, pin::Pin, task::Poll};

use cartel_core::deadline;
use dope::core::driver::schedule::clock::{self, timer};
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
    R: Copy + Into<Option<api::Target<'d, ID>>>,
    S,
    M: Copy + Into<requests::Boundary>,
    A: lease::Target<'d, ID>,
    P: lease::Policy,
    H,
>
    State<
        api::Context<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
        R,
        api::Pending<'d, S, M>,
        lease::Lease<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, A, P, ID>,
        H,
    >
{
    pub(super) fn cancel(&mut self, error: crate::Error) -> Error<'d, ID> {
        match mem::replace(self, State::Done) {
            State::Failed(error) => return error.into(),
            State::Admitted(lease, _) => lease.abort(error),
            _ => {}
        }
        error.into()
    }

    pub(super) fn poll_with<'this, 'target, O>(
        &'this mut self,
        mut cx: Pin<&mut context::Context<'_, 'd>>,
        waiter: Pin<&wait::Waiter<'target, 'd>>,
        ready: impl FnOnce(&'this mut H) -> O,
        mut prepare: impl FnMut(
            &'this mut Self,
            api::Context<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
            requests::ReplyContext<'_, 'app, 'd, ID>,
        ) -> Result<
            O,
            (
                requests::Rejection,
                requests::Source<'app, 'd, ID>,
                &'this mut Self,
            ),
        >,
    ) -> Poll<Result<O, Error<'d, ID>>>
    where
        'app: 'target,
        'd: 'target,
    {
        match self {
            State::Admitted(_, item) => Poll::Ready(Ok(ready(item))),
            this => {
                let (client, target, request) = match &*this {
                    State::Connecting {
                        client,
                        target,
                        request,
                    } => (*client, (*target).into(), request),
                    State::Admitted(..) => unreachable!("admitted"),
                    State::Failed(_) | State::Done => {
                        return Poll::Ready(Err(this.cancel(crate::Error::Closed)));
                    }
                };
                client
                    .port
                    .maintain(cx.as_ref().work(), cx.as_mut().region_token());
                let unpinned = target.is_none();
                let boundary = request.boundary.into();
                let mut this = this;
                let error = 'failed: {
                    if let Err(error) = api::check_open(client) {
                        break 'failed error;
                    }
                    let ready = &client.port.shared;
                    let mut selected = match ready.select(&target) {
                        Ok(source) => source,
                        Err(target) => break 'failed Error::Pinned(target),
                    };
                    let mut candidates = None;
                    let mut pressure = None;
                    while let Some(source) = selected {
                        let listed = match source {
                            requests::Source::Shared(listed)
                                if unpinned
                                    && matches!(boundary, requests::Boundary::Group)
                                    && !listed.slot().is_synced() =>
                            {
                                listed
                            }
                            source => {
                                let (error, source, next) = match prepare(
                                    this,
                                    client,
                                    source.context(boundary, cx.as_mut().region_token()),
                                ) {
                                    Ok(output) => {
                                        waiter.unregister();
                                        return Poll::Ready(Ok(output));
                                    }
                                    Err(failure) => failure,
                                };
                                this = next;
                                let cause = match error {
                                    requests::Rejection::Pressure(cause) => cause,
                                    requests::Rejection::Failed(error) => {
                                        drop(mem::replace(this, State::Done));
                                        waiter.unregister();
                                        return Poll::Ready(Err(error.into()));
                                    }
                                };
                                pressure = Some((cause, source.connection()));
                                if !unpinned || matches!(cause, crate::reply::Pressure::Full { .. })
                                {
                                    break;
                                }
                                let Some(listed) = source.into_listed() else {
                                    break;
                                };
                                listed
                            }
                        };
                        selected = candidates
                            .get_or_insert_with(|| listed.remaining())
                            .next()
                            .map(requests::Source::Shared);
                    }
                    if let Some((cause, conn)) = pressure {
                        if let Err(error) = ready.signals.egress.try_register(waiter, cx.as_ref()) {
                            let error = cause.fail(conn, error);
                            drop(mem::replace(this, State::Done));
                            waiter.unregister();
                            return Poll::Ready(Err(error));
                        }
                        return Poll::Pending;
                    }
                    if unpinned && let Some(count) = ready.blocking() {
                        break 'failed client.port.backpressure(count).into();
                    }
                    match ready.try_register(waiter, cx.as_ref()) {
                        Ok(()) => return Poll::Pending,
                        Err(error) => Error::Wait(error),
                    }
                };
                let request = mem::replace(this, State::Done);
                waiter.unregister();
                drop(request);
                Poll::Ready(Err(error))
            }
        }
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
    pub(in crate::api) fn guard(&self) -> Result<timer::Alarm<'d>, timer::Exhausted> {
        self.timer.try_alarm()
    }

    pub(in crate::api) fn poll_expired(
        &mut self,
        guard: Pin<&mut timer::Alarm<'d>>,
        mut ctx: Pin<&mut context::Context<'_, 'd>>,
    ) -> Poll<()> {
        let now = ctx.as_mut().driver().driver_ref().clock().now();
        let due = match self.state {
            BudgetState::Elapsed => return Poll::Ready(()),
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
            return Poll::Ready(());
        }
        let wake = ctx.as_ref().completion();
        if alarm.poll_or_arm(due, now, wake).is_ready() {
            self.state = BudgetState::Elapsed;
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl<'d> From<deadline::Source<'d>> for Budget<'d> {
    fn from(source: deadline::Source<'d>) -> Self {
        let (timer, window) = source.into_parts();
        Self {
            timer,
            state: BudgetState::Fresh(window),
        }
    }
}

impl<'d> From<deadline::Guard<'d>> for Budget<'d> {
    fn from(guard: deadline::Guard<'d>) -> Self {
        let Some((alarm, window)) = guard.into_parts() else {
            unreachable!("deadline")
        };
        let (timer, state) = alarm.into_parts();
        let state = match (window, state) {
            (Some(window), timer::heap::State::Idle) => BudgetState::Fresh(window),
            (None, timer::heap::State::Armed(due)) => BudgetState::Armed(due),
            (None, timer::heap::State::Fired) => BudgetState::Elapsed,
            _ => unreachable!("deadline"),
        };
        Self { timer, state }
    }
}
