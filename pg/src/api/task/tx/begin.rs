use crate::reply::Error;
use crate::{
    api::{self, Response, request},
    port::requests,
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
    Failed(crate::Error),
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
            Err(error) => State::Failed(error),
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
            State::Failed(error) => return error.into(),
            State::Pending { lease } => lease.abort(error),
            _ => {}
        }
        error.into()
    }

    fn poll_admit(
        state: &mut State<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
        client: api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
        mut ctx: pin::Pin<&mut context::Context<'_, 'd>>,
        waiter: pin::Pin<&wait::Waiter<'app, 'd>>,
        reply: &mut Response<'app, 'g, 'r, 's, 'b, 'c, 'd, api::ExtractUnit<ID>>,
        request: api::Pending<'d>,
    ) -> task::Poll<Result<(), Error<'d, ID>>> {
        client
            .port
            .maintain(ctx.as_ref().work(), ctx.as_mut().region_token());
        if let Err(error) = api::check_open(client) {
            drop(request);
            waiter.unregister();
            return task::Poll::Ready(Err(error));
        }
        let shared = &client.port.shared;
        let hold = loop {
            let Some(listed) = shared.ready.pick_begin() else {
                break None;
            };
            if let Some(hold) = shared.transaction().reserve(listed, |conn| {
                client.port.load(conn, ctx.as_mut().region_token())
            }) {
                break Some(hold);
            }
        };
        let Some(hold) = hold else {
            if let Some(count) = shared.ready.blocking() {
                let error = client.port.backpressure(count);
                drop(request);
                waiter.unregister();
                return task::Poll::Ready(Err(error.into()));
            }
            waiter.unregister();
            if let Some(error) = shared.failure() {
                return task::Poll::Ready(Err(error));
            }
            shared
                .ready
                .try_register(waiter, ctx.as_ref())
                .map_err(Error::Wait)?;
            *state = State::Waiting(request);
            return task::Poll::Pending;
        };
        let api::Pending {
            frame,
            boundary,
            state: (),
        } = request;
        let context =
            requests::ReplyContext::reserved(&hold, boundary, ctx.as_mut().region_token());
        match client.port.request().try_reply(
            frame.geometry().wire().widen(),
            (frame, hold),
            reply,
            context,
            |(frame, hold), _| (frame, super::Held::new(client, hold.commit())),
        ) {
            Ok(lease) => {
                waiter.unregister();
                *state = State::Pending { lease };
                task::Poll::Ready(Ok(()))
            }
            Err((error, _, (frame, hold))) => {
                let throttle =
                    api::Throttle::new(hold.id().conn, error).map(|throttle| (throttle, frame));
                shared.transaction().release(hold);
                waiter.unregister();
                let (throttle, frame) = throttle?;
                if let Some(error) = shared.failure() {
                    return task::Poll::Ready(Err(error));
                }
                if let Err(error) =
                    shared
                        .signals
                        .egress
                        .try_register(&throttle.cause, waiter, ctx.as_ref())
                {
                    return task::Poll::Ready(Err(throttle.fail(error)));
                }
                *state = State::Waiting(api::Pending {
                    frame,
                    boundary,
                    state: (),
                });
                task::Poll::Pending
            }
        }
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
            if matches!(this.state, State::Pending { .. }) {
                let task::Poll::Ready(next) =
                    pin::Pin::new(&mut *this.reply).poll_reply(ctx.as_mut())
                else {
                    return task::Poll::Pending;
                };
                return match mem::replace(this.state, State::Done) {
                    State::Pending { lease } => {
                        guard.as_mut().complete();
                        task::Poll::Ready(match next {
                            Ok(()) => {
                                let target = lease.target();
                                if matches!(
                                    client.port.shared.transaction().current(target.conn),
                                    crate::port::pool::access::Current::Held(current) if current == target
                                ) {
                                    Ok(lease)
                                } else {
                                    let error = crate::Error::Protocol(
                                        crate::protocol::Reason::TransactionLease,
                                    );
                                    lease.abort(error);
                                    Err(error.into())
                                }
                            }
                            Err(error) => Err(error),
                        })
                    }
                    _ => unreachable!("pending"),
                };
            }
            match mem::replace(this.state, State::Done) {
                State::Waiting(req) => {
                    match Self::poll_admit(
                        this.state,
                        client,
                        ctx.as_mut(),
                        waiter,
                        this.reply,
                        req,
                    ) {
                        task::Poll::Ready(Ok(())) => {}
                        task::Poll::Ready(Err(error)) => {
                            guard.as_mut().complete();
                            return task::Poll::Ready(Err(error));
                        }
                        task::Poll::Pending => return task::Poll::Pending,
                    }
                }
                State::Pending { .. } => unreachable!("pending"),
                State::Failed(error) => {
                    guard.as_mut().complete();
                    return task::Poll::Ready(Err(error.into()));
                }
                State::Done => {
                    guard.as_mut().complete();
                    return task::Poll::Ready(Err(crate::Error::Closed.into()));
                }
            }
        }
    }
}
