use crate::api::{self, Response, request, task::State};
use crate::port::{Instance, requests::Complete};
use cartel_core::deadline;
use dope::core::driver::schedule::clock::timer;
use dope_fiber::wait;

#[pin_project::pin_project]
/// Transaction-begin task retaining its exact admission state.
pub(in crate::api) struct Begin<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, const ID: u8 = 0>
where
    I: Instance<Queries: 'd> + 'd,
{
    reply: Response<'app, 'g, 'r, 's, 'b, 'c, 'd, api::ExtractUnit<ID>>,
    state: State<
        api::Context<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
        (),
        Complete<'d>,
        super::Held<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
        (),
    >,
    #[pin]
    deadline: Result<deadline::Guard<'d>, timer::Exhausted>,
    #[pin]
    waiter: wait::Waiter<'app, 'd>,
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: Instance<Queries: 'd> + 'd, const ID: u8>
    Begin<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    pub(in crate::api) fn new(
        client: api::Context<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
        begin: request::Begin,
    ) -> Self {
        Self {
            reply: Response::new(),
            state: State::new(
                client,
                (),
                client
                    .port
                    .request()
                    .encode(client.port.requests.acquire(), |frame| begin.encode(frame)),
            ),
            deadline: client.port.lifecycle.deadline.guard(),
            waiter: wait::Waiter::new(),
        }
    }
}

mod raw {
    use super::{Begin, Instance, State, api, deadline};
    use crate::{api::task::tx::Held, port::requests, reply::Error};
    use cartel_core::reply::recv::{Extract as _, attach::Attach};
    use dope_fiber::{abi, context};
    use std::{
        pin,
        ptr::{self, NonNull},
        task,
    };

    impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: Instance<Queries: 'd> + 'd, const ID: u8>
        abi::Fiber<'d> for Begin<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
    {
        type Output = Result<Held<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>, Error<'d, ID>>;

        fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
            let (this, mut ctx) = call.into_parts();
            let mut this = this.project();
            let waiter = this.waiter.as_ref();
            let cause = match this.deadline.as_mut().get_mut() {
                Err(error) => crate::Error::Deadline(*error),
                Ok(guard) => 'poll: loop {
                    let mut guard = pin::Pin::new(&mut *guard);
                    if guard.as_mut().poll_expired(ctx.as_mut()).is_ready() {
                        break crate::Error::RequestTimeout(deadline::Elapsed::new(this.state.phase()));
                    }
                    let mut destination = NonNull::from(&mut *this.state);
                    // SAFETY: this poll exclusively borrows the state. Each field loan ends
                    // before a successful transition overwrites its original phase.
                    let error = match unsafe { destination.as_mut() } {
                        State::Connecting {
                            client,
                            request: frame,
                            ..
                        } => 'admission: {
                            let client = *client;
                            client
                                .port
                                .maintain(ctx.as_ref().work(), ctx.as_mut().region_token());
                            if let Err(error) = api::check_open(client) {
                                *this.state = State::Done;
                                waiter.unregister();
                                break 'admission error;
                            }
                            let shared = &client.port.shared;
                            let frame = NonNull::from(frame);
                            let mut selected = shared.pick();
                            let mut candidates: Option<
                                crate::protocol::runtime::ready::Remaining<'_, 'd, ID, I::Policy>,
                            > = None;
                            let mut pressure = None;
                            while let Some(listed) = selected {
                                if !listed.slot().can_begin() {
                                    selected =
                                        candidates.get_or_insert_with(|| listed.remaining()).next();
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
                                let context = requests::ReplyContext::reserved(
                                    &hold,
                                    requests::Boundary::Begin,
                                    ctx.as_mut().region_token(),
                                );
                                match client.port.request().try_reply(
                                    hold,
                                    this.reply,
                                    context,
                                    |hold, _| {
                                        let admitted =
                                            State::Admitted(Held::new(client, hold.commit()), ());
                                        // SAFETY: both grants succeeded. Only the matched frame owns
                                        // storage in Connecting, and success leaves the candidate loop.
                                        let bytes = unsafe {
                                            let bytes = frame.as_ptr().read();
                                            destination.as_ptr().write(admitted);
                                            bytes
                                        };
                                        (bytes, ())
                                    },
                                ) {
                                    Ok(()) => {
                                        waiter.unregister();
                                        continue 'poll;
                                    }
                                    Err((error, source, hold)) => {
                                        let cause = match error {
                                            requests::Rejection::Pressure(cause) => cause,
                                            requests::Rejection::Failed(error) => {
                                                waiter.unregister();
                                                break 'admission error.into();
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
                            let registered = if let Some((cause, conn)) = pressure {
                                if let Some(error) = shared.failure() {
                                    break 'admission error;
                                }
                                shared
                                    .signals
                                    .egress
                                    .try_register(waiter, ctx.as_ref())
                                    .map_err(|error| cause.fail(conn, error))
                            } else {
                                if let Some(count) = shared.blocking() {
                                    break 'admission client.port.backpressure(count).into();
                                }
                                if let Some(error) = shared.failure() {
                                    break 'admission error;
                                }
                                shared
                                    .try_register(waiter, ctx.as_ref())
                                    .map_err(Error::Wait)
                            };
                            match registered {
                                Ok(()) => return task::Poll::Pending,
                                Err(error) => error,
                            }
                        }
                        State::Admitted(lease, ()) => {
                            let task::Poll::Ready(next) = pin::Pin::new(&mut *this.reply).poll_reply(
                                ctx.as_mut(),
                                api::ExtractUnit::<ID>::extract,
                                std::convert::identity,
                            ) else {
                                return task::Poll::Pending;
                            };
                            // SAFETY: the reply completed and the matched lease is the only
                            // owner in Admitted. The empty phase has no remaining fields to drop.
                            let lease = unsafe {
                                let lease = ptr::read(lease);
                                destination.as_ptr().write(State::Done);
                                lease
                            };
                            guard.as_mut().complete();
                            return task::Poll::Ready(match next {
                                Ok(()) => {
                                    if lease
                                        .client()
                                        .port
                                        .shared
                                        .transaction()
                                        .held(lease.target())
                                        .is_some()
                                    {
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
                            });
                        }
                        State::Failed(error) => (*error).into(),
                        State::Done => crate::Error::Closed.into(),
                    };
                    *this.state = State::Done;
                    guard.as_mut().complete();
                    return task::Poll::Ready(Err(error));
                },
            };
            Attach::detach(this.reply);
            let error = this.state.cancel(cause, |_| true, Held::abort);
            waiter.unregister();
            task::Poll::Ready(Err(error))
        }
    }
}
