use crate::{api, reply::Error, schema};
use crate::{port::pool::access, protocol::runtime::tx::poll};
use api::{
    request,
    task::{
        Core, Done,
        lease::{Lease, Recover},
        tx::Held,
    },
};
use cartel_core::deadline;
use dope::core::driver::schedule::clock::timer;
use dope_fiber::{abi, context, wait};
use std::{mem, pin, task::Poll};

#[derive(Clone, Copy)]
pub(super) enum End {
    Commit,
    Rollback,
}

enum State<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, const ID: u8>
where
    I: schema::QuerySet + 'd,
{
    Holding(Held<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>),
    Dispatching(
        Core<
            'app,
            'g,
            'r,
            's,
            'b,
            'c,
            'd,
            B,
            I,
            api::Response<'app, 'g, 'r, 's, 'b, 'c, 'd, api::ExtractUnit<ID>>,
            ID,
            Held<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
            access::Finalization<'d, ID>,
            Recover,
        >,
    ),
    Settling {
        finalizing:
            Lease<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, access::Finalization<'d, ID>, Recover, ID>,
        outcome: Result<(), Error<'d, ID>>,
    },
    Done,
}

/// Settles one worker-local transaction; wait registration only queues wakes.
#[pin_project::pin_project]
pub(super) struct Finish<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, const ID: u8>
where
    I: schema::QuerySet + 'd,
{
    state: State<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
    end: End,
    #[pin]
    deadline: Result<deadline::Guard<'d>, timer::Exhausted>,
    #[pin]
    waiter: wait::Waiter<'app, 'd>,
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    Finish<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    pub(super) fn new(lease: Held<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>, end: End) -> Self {
        Self {
            deadline: lease.client().port.lifecycle.deadline.guard(),
            state: State::Holding(lease),
            end,
            waiter: wait::Waiter::new(),
        }
    }

    fn fail(
        state: &mut State<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
        error: crate::Error,
    ) -> Error<'d, ID> {
        match mem::replace(state, State::Done) {
            State::Holding(lease) => lease.abort(error),
            State::Dispatching(mut call) => return call.abort(error),
            State::Settling {
                finalizing: lease,
                outcome,
            } => {
                lease.abort(error);
                if let Err(error) = outcome {
                    return error;
                }
            }
            State::Done => {}
        }
        error.into()
    }
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    abi::Fiber<'d> for Finish<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    type Output = Result<(), Error<'d, ID>>;
    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, mut cx) = call.into_parts();
        let mut this = this.project();
        let end = *this.end;
        let waiter = this.waiter.as_ref();
        let mut timer = match this.deadline.as_mut().get_mut() {
            Ok(timer) => pin::Pin::new(timer),
            Err(error) => {
                let error = Self::fail(this.state, crate::Error::Deadline(*error));
                waiter.unregister();
                return Poll::Ready(Err(error));
            }
        };
        loop {
            let phase = match &*this.state {
                State::Dispatching(call) => call.phase(),
                State::Holding(_) | State::Settling { .. } | State::Done => {
                    deadline::RequestPhase::Settlement
                }
            };
            if let Poll::Ready(time) = timer.as_mut().poll_expired(cx.as_mut(), phase) {
                let error = Self::fail(this.state, crate::Error::RequestTimeout(time));
                waiter.unregister();
                return Poll::Ready(Err(error));
            }
            match mem::replace(this.state, State::Done) {
                State::Holding(lease) => {
                    let owner = lease.client();
                    let tx = lease.target();
                    match owner.port.shared.transaction().current(tx.conn) {
                        access::Current::Held(current) if current == tx => {}
                        access::Current::Stale
                        | access::Current::Handshaking
                        | access::Current::Shared
                        | access::Current::Reserved(_)
                        | access::Current::Held(_)
                        | access::Current::Finalizing(_)
                        | access::Current::Draining
                        | access::Current::Closing => {
                            waiter.unregister();
                            timer.as_ref().complete();
                            return Poll::Ready(Err(crate::Error::Closed.into()));
                        }
                    }
                    if owner.port.load(tx.conn, cx.as_mut().region_token()) != Some(0) {
                        match owner
                            .port
                            .shared
                            .transaction()
                            .try_register(tx, waiter, cx.as_ref())
                        {
                            poll::Register::Registered => {}
                            poll::Register::Stale => {
                                waiter.unregister();
                                timer.as_ref().complete();
                                return Poll::Ready(Err(crate::Error::Closed.into()));
                            }
                            poll::Register::Failed(error) => {
                                waiter.unregister();
                                timer.as_ref().complete();
                                return Poll::Ready(Err(crate::reply::Error::Wait(error)));
                            }
                        }
                        *this.state = State::Holding(lease);
                        return Poll::Pending;
                    }
                    let req = match end {
                        End::Commit => api::Pending::commit(owner.port),
                        End::Rollback => {
                            api::Pending::raw(owner.port, request::Request::raw("ROLLBACK"))
                        }
                    };
                    let req = match req {
                        Ok(req) => req,
                        Err(error) => {
                            waiter.unregister();
                            timer.as_ref().complete();
                            return Poll::Ready(Err(error.into()));
                        }
                    };
                    let call = owner
                        .pending::<api::ExtractUnit<ID>, _, access::Finalization<'d, ID>, Recover>(
                            Some(api::Target::from_tx(tx)),
                            req.map_state(|()| lease),
                        );
                    *this.state = State::Dispatching(call);
                }
                State::Dispatching(mut call) => {
                    match call.poll_done(cx.as_mut(), waiter, |held, _client, _conn| held.begin()) {
                        Poll::Pending => {
                            *this.state = State::Dispatching(call);
                            return Poll::Pending;
                        }
                        Poll::Ready(Done::AdmissionFailed(error)) => {
                            waiter.unregister();
                            timer.as_ref().complete();
                            return Poll::Ready(Err(error));
                        }
                        Poll::Ready(Done::Response {
                            outcome: next,
                            committed: lease,
                        }) => {
                            *this.state = State::Settling {
                                finalizing: lease,
                                outcome: next,
                            };
                        }
                    }
                }
                State::Settling {
                    finalizing: lease,
                    outcome: next,
                } => {
                    let owner = lease.client();
                    let mode = match lease.settle() {
                        access::Finalization::Pending(target) => loop {
                            match owner.port.shared.transaction().settled(target) {
                                poll::Settlement::Complete(mode) => break mode,
                                poll::Settlement::Pending => {
                                    match owner.port.shared.transaction().try_register(
                                        target,
                                        waiter,
                                        cx.as_ref(),
                                    ) {
                                        poll::Register::Registered => {}
                                        poll::Register::Stale => continue,
                                        poll::Register::Failed(error) => {
                                            waiter.unregister();
                                            timer.as_ref().complete();
                                            let next = next.and(Err(Error::Wait(error)));
                                            if owner
                                                .port
                                                .shared
                                                .transaction()
                                                .is_current::<true>(target)
                                            {
                                                owner.abort(target.conn, crate::Error::Closed);
                                            }
                                            return Poll::Ready(next);
                                        }
                                    }
                                    *this.state = State::Settling {
                                        finalizing: Lease::new(
                                            owner,
                                            access::Finalization::Pending(target),
                                        ),
                                        outcome: next,
                                    };
                                    return Poll::Pending;
                                }
                            }
                        },
                        access::Finalization::Reusable(_) => poll::Disposition::Reusable,
                        access::Finalization::Lost(_) => poll::Disposition::Lost,
                    };
                    waiter.unregister();
                    timer.as_ref().complete();
                    let next = match mode {
                        poll::Disposition::Reusable => next,
                        poll::Disposition::Lost => next.and(Err(crate::Error::Closed.into())),
                    };
                    return Poll::Ready(next);
                }
                State::Done => {
                    waiter.unregister();
                    timer.as_ref().complete();
                    return Poll::Ready(Err(crate::Error::Closed.into()));
                }
            }
        }
    }
}
