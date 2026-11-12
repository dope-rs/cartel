use crate::{api::task::State, reply::Error};
use crate::{
    api::{self, Response, request},
    port::requests,
    schema,
};
use cartel_core::{deadline, reply::recv::attach::Attach};
use dope::core::driver::schedule::clock::timer;
use dope_fiber::{abi, context, wait};
use std::{mem, pin, task};

type Admission<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, const ID: u8> =
    State<(), (), api::Pending<'d>, super::Held<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>, ()>;

#[pin_project::pin_project]
/// Transaction-begin task retaining its exact admission state.
pub(in crate::api) struct Begin<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, const ID: u8 = 0>
where
    I: schema::QuerySet + 'd,
{
    client: api::Context<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
    reply: Response<'app, 'g, 'r, 's, 'b, 'c, 'd, api::ExtractUnit<ID>>,
    state: Admission<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
    #[pin]
    deadline: Result<deadline::Guard<'d>, timer::Exhausted>,
    #[pin]
    waiter: wait::Waiter<'app, 'd>,
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    Begin<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    pub(in crate::api) fn new(
        client: api::Context<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
        begin: request::Begin,
    ) -> Self {
        let reply = Response::new();
        let state = State::new(
            (),
            (),
            api::Pending::raw(
                client.port,
                request::Request::begin(begin),
                o3::buffer::pool::Pool::acquire,
            ),
        );
        Self {
            client,
            reply,
            state,
            deadline: client.port.lifecycle.deadline.guard(),
            waiter: wait::Waiter::new(),
        }
    }

    fn poll_admit(
        state: &mut Admission<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
        client: api::Context<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
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
        let api::Pending {
            mut frame,
            boundary,
            state: (),
        } = request;
        let mut selected = shared.pick();
        let mut candidates: Option<crate::protocol::runtime::ready::Remaining<'_, 'd, ID>> = None;
        let mut pressure = None;
        while let Some(listed) = selected {
            if !listed.slot().can_begin() {
                selected = candidates.get_or_insert_with(|| listed.remaining()).next();
                continue;
            }
            let Some(hold) = shared.transaction().reserve(listed, |conn| {
                client.port.load(conn, ctx.as_mut().region_token())
            }) else {
                selected = match &mut candidates {
                    Some(candidates) => candidates.next(),
                    None => shared.pick(),
                };
                continue;
            };
            let context =
                requests::ReplyContext::reserved(&hold, boundary, ctx.as_mut().region_token());
            match client.port.request().try_reply(
                (frame, hold),
                reply,
                context,
                |(frame, hold), _| (frame, super::Held::new(client, hold.commit())),
            ) {
                Ok(lease) => {
                    waiter.unregister();
                    *state = State::Admitted(lease, ());
                    return task::Poll::Ready(Ok(()));
                }
                Err((error, source, (returned, hold))) => {
                    frame = returned;
                    let cause = match error {
                        requests::Rejection::Pressure(cause) => cause,
                        requests::Rejection::Failed(error) => {
                            waiter.unregister();
                            return task::Poll::Ready(Err(error.into()));
                        }
                    };
                    pressure = Some((cause, source.connection()));
                    if matches!(cause, crate::reply::Pressure::Full { .. }) {
                        break;
                    }
                    selected = candidates
                        .get_or_insert_with(|| hold.into_listed().remaining())
                        .next();
                }
            }
        }
        waiter.unregister();
        if let Some((cause, conn)) = pressure {
            if let Some(error) = shared.failure() {
                return task::Poll::Ready(Err(error));
            }
            shared
                .signals
                .egress
                .try_register(waiter, ctx.as_ref())
                .map_err(|error| cause.fail(conn, error))?;
        } else {
            if let Some(count) = shared.blocking() {
                let error = client.port.backpressure(count);
                return task::Poll::Ready(Err(error.into()));
            }
            if let Some(error) = shared.failure() {
                return task::Poll::Ready(Err(error));
            }
            shared
                .try_register(waiter, ctx.as_ref())
                .map_err(Error::Wait)?;
        }
        *state = State::Connecting {
            client: (),
            target: (),
            request: api::Pending {
                frame,
                boundary,
                state: (),
            },
        };
        task::Poll::Pending
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
                let error = this
                    .state
                    .cancel(crate::Error::Deadline(*error), super::Held::abort);
                waiter.unregister();
                return task::Poll::Ready(Err(error));
            }
        };
        loop {
            if guard.as_mut().poll_expired(ctx.as_mut()).is_ready() {
                let time = deadline::Elapsed::new(this.state.phase());
                Attach::detach(this.reply);
                let error = this
                    .state
                    .cancel(crate::Error::RequestTimeout(time), super::Held::abort);
                waiter.unregister();
                return task::Poll::Ready(Err(error));
            }
            if matches!(this.state, State::Admitted(..)) {
                let task::Poll::Ready(next) =
                    pin::Pin::new(&mut *this.reply).poll_reply(ctx.as_mut())
                else {
                    return task::Poll::Pending;
                };
                return match mem::replace(this.state, State::Done) {
                    State::Admitted(lease, ()) => {
                        guard.as_mut().complete();
                        task::Poll::Ready(match next {
                            Ok(()) => {
                                let target = lease.target();
                                if matches!(
                                    client.port.shared.transaction().current(target.conn),
                                    crate::port::pool::access::Current::Held(current) if current == target.generation
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
                State::Connecting { request, .. } => {
                    match Self::poll_admit(
                        this.state,
                        client,
                        ctx.as_mut(),
                        waiter,
                        this.reply,
                        request,
                    ) {
                        task::Poll::Ready(Ok(())) => {}
                        task::Poll::Ready(Err(error)) => {
                            guard.as_mut().complete();
                            return task::Poll::Ready(Err(error));
                        }
                        task::Poll::Pending => return task::Poll::Pending,
                    }
                }
                State::Admitted(..) => unreachable!("pending"),
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
