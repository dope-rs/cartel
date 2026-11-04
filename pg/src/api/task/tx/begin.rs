use crate::reply::Error;
use crate::{
    api::{self, Response, request},
    schema,
};
use cartel_core::{deadline, reply::recv::attach::Attach};
use dope::core::driver::schedule::clock::timer;
use dope_fiber::{abi, context, wait};
use std::{mem, pin, task};

enum State<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, const ID: u8>
where
    I: schema::QuerySet + 'd,
{
    Pending {
        lease: super::Held<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
    },
    Waiting(api::Pending<'d>),
    Failed(Error<'d, ID>),
    Done,
}

#[pin_project::pin_project]
/// Transaction-begin task retaining its exact admission state.
pub(in crate::api) struct Begin<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, const ID: u8 = 0>
where
    I: schema::QuerySet + 'd,
{
    client: api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
    reply: Response<'app, 'g, 'r, 's, 'b, 'c, 'd, api::ExtractUnit<ID>>,
    state: State<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
    #[pin]
    deadline: Result<deadline::Guard<'d>, timer::Exhausted>,
    #[pin]
    waiter: wait::Waiter<'app, 'd>,
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    Begin<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    pub(in crate::api) fn new(
        client: api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
        begin: request::Begin,
    ) -> Self {
        let reply = Response::new();
        let state = match api::Pending::raw(client.port, request::Request::begin(begin)) {
            Ok(request) => State::Waiting(request),
            Err(error) => State::Failed(error.into()),
        };
        Self {
            client,
            reply,
            state,
            deadline: client.port.lifecycle.deadline.guard(),
            waiter: wait::Waiter::new(),
        }
    }

    fn fail(
        state: &mut State<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
        error: crate::Error,
    ) -> Error<'d, ID> {
        match mem::replace(state, State::Done) {
            State::Failed(error) => return error,
            State::Pending { lease } => lease.abort(error),
            _ => {}
        }
        error.into()
    }
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    abi::Fiber<'d> for Begin<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    type Output = Result<super::Held<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>, Error<'d, ID>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, mut ctx) = call.into_parts();
        let mut this = this.project();
        let client = *this.client;
        let waiter = this.waiter.as_ref();
        let mut guard = match this.deadline.as_mut().get_mut() {
            Ok(guard) => pin::Pin::new(guard),
            Err(error) => {
                Attach::detach(this.reply);
                let error = Self::fail(this.state, crate::Error::Deadline(*error));
                waiter.unregister();
                return task::Poll::Ready(Err(error));
            }
        };
        loop {
            let phase = match &*this.state {
                State::Pending { .. } => deadline::RequestPhase::Response,
                State::Waiting(_) | State::Failed(_) | State::Done => {
                    deadline::RequestPhase::Admission
                }
            };
            if let task::Poll::Ready(time) = guard.as_mut().poll_expired(ctx.as_mut(), phase) {
                Attach::detach(this.reply);
                let error = Self::fail(this.state, crate::Error::RequestTimeout(time));
                waiter.unregister();
                return task::Poll::Ready(Err(error));
            }
            match mem::replace(this.state, State::Done) {
                State::Waiting(req) => match client.retry_tx(ctx.as_mut(), waiter, this.reply, req)
                {
                    api::TxRetry::Waiting(req) => {
                        *this.state = State::Waiting(req);
                        return task::Poll::Pending;
                    }
                    api::TxRetry::Enqueued { lease } => {
                        *this.state = State::Pending { lease };
                    }
                    api::TxRetry::Failed(error) => {
                        guard.as_ref().complete();
                        return task::Poll::Ready(Err(error));
                    }
                },
                State::Pending { lease } => {
                    match pin::Pin::new(&mut *this.reply).poll_reply(ctx.as_mut()) {
                        task::Poll::Pending => {
                            *this.state = State::Pending { lease };
                            return task::Poll::Pending;
                        }
                        task::Poll::Ready(Ok(())) => {
                            guard.as_ref().complete();
                            let target = lease.target();
                            if !matches!(
                                client.port.shared.transaction().current(target.conn),
                                crate::port::pool::access::Current::Held(current) if current == target
                            ) {
                                let error = crate::Error::Protocol(
                                    crate::protocol::Reason::TransactionLease,
                                );
                                lease.abort(error);
                                return task::Poll::Ready(Err(error.into()));
                            }
                            return task::Poll::Ready(Ok(lease));
                        }
                        task::Poll::Ready(Err(error)) => {
                            guard.as_ref().complete();
                            return task::Poll::Ready(Err(error));
                        }
                    }
                }
                State::Failed(error) => {
                    guard.as_ref().complete();
                    return task::Poll::Ready(Err(error));
                }
                State::Done => {
                    guard.as_ref().complete();
                    return task::Poll::Ready(Err(crate::Error::Closed.into()));
                }
            }
        }
    }
}
