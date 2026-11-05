use std::{marker, mem, pin, task::Poll};

use cartel_core::deadline;
use dope::{core::driver::route, manifold::client::conn};
use dope_fiber::{abi, context, wait};
use o3::base::cell::region;

use crate::{
    client::ReplyError,
    client::{self, dispatch::replies},
    port::schedule,
    request,
    request::encode,
    value,
};

const _: () = {
    assert!(
        mem::size_of::<Pooled<'static, 'static, 'static, 'static, 'static, (), 0>>()
            == mem::size_of::<client::Redis<'static, 'static, 'static, 'static, 'static, (), 0>>()
    );
};

#[doc(hidden)]
pub struct Pooled<'app, 's, 'b, 'c, 'd: 'app, B: ?Sized, const ID: u8>(
    client::Redis<'app, 's, 'b, 'c, 'd, B, ID>,
);

impl<'app, 's, 'b, 'c, 'd: 'app, B: ?Sized, const ID: u8> Pooled<'app, 's, 'b, 'c, 'd, B, ID> {
    pub(in crate::client) const fn new(redis: client::Redis<'app, 's, 'b, 'c, 'd, B, ID>) -> Self {
        Self(redis)
    }
}

pub(in crate::client) trait Target<'app, 's, 'b, 'c, 'd: 'app, const ID: u8>:
    Sized
{
    type Provider: ?Sized;

    fn redis(&self) -> client::Redis<'app, 's, 'b, 'c, 'd, Self::Provider, ID>;

    fn try_admit(
        self,
        frame: encode::Complete,
        reply: &mut replies::Admitted<'app, 'b, 'c, 's, 'd, ID>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Attempt<'d, Self, ID>;
}

impl<'app, 's, 'b, 'c, 'd: 'app, B: ?Sized, const ID: u8> Target<'app, 's, 'b, 'c, 'd, ID>
    for Pooled<'app, 's, 'b, 'c, 'd, B, ID>
{
    type Provider = B;

    fn redis(&self) -> client::Redis<'app, 's, 'b, 'c, 'd, B, ID> {
        self.0
    }

    fn try_admit(
        self,
        frame: encode::Complete,
        reply: &mut replies::Admitted<'app, 'b, 'c, 's, 'd, ID>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Attempt<'d, Self, ID> {
        let redis = self.0;
        match redis
            .port
            .schedule
            .try_pool(region, frame, self, reply, |target, token| Committed {
                target,
                token,
            }) {
            Ok(sent) => Attempt::Sent(sent),
            Err(schedule::Reject::Blocked {
                cause: schedule::Block::Pool,
                frame,
                admit,
            }) => Attempt::Blocked {
                cause: crate::Block::Pool,
                target: admit,
                frame,
            },
            Err(schedule::Reject::Blocked {
                cause: schedule::Block::Queue(cause),
                frame,
                admit,
            }) => Attempt::Blocked {
                cause: crate::Block::Queue(cause),
                target: admit,
                frame,
            },
            Err(schedule::Reject::Blocked {
                cause: schedule::Block::Unavailable,
                frame,
                admit,
            }) => match redis.port.activity.fatal_cause(region) {
                Some(cause) => Attempt::Failed(cause),
                None => Attempt::Blocked {
                    cause: crate::Block::Inactive,
                    target: admit,
                    frame,
                },
            },
            Err(schedule::Reject::Unavailable) => Attempt::Failed(crate::Error::Unavailable),
            Err(schedule::Reject::Reply(error)) => Attempt::Failed(crate::Error::Reply(error)),
        }
    }
}

impl<'lease, 'app, 's, 'b, 'c, 'd: 'app, B: ?Sized, const ID: u8> Target<'app, 's, 'b, 'c, 'd, ID>
    for &'lease mut schedule::Lease<'app, 's, 'b, 'c, 'd, B, ID>
{
    type Provider = B;

    fn redis(&self) -> client::Redis<'app, 's, 'b, 'c, 'd, B, ID> {
        schedule::Lease::redis(self)
    }

    fn try_admit(
        self,
        frame: encode::Complete,
        reply: &mut replies::Admitted<'app, 'b, 'c, 's, 'd, ID>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Attempt<'d, Self, ID> {
        let redis = schedule::Lease::redis(self);
        let token = self.target();
        match redis.port.schedule.try_dispatch_lease(
            region,
            frame,
            self,
            reply,
            token,
            |target, token| Committed { target, token },
        ) {
            Ok(sent) => Attempt::Sent(sent),
            Err(schedule::Reject::Blocked {
                cause,
                frame,
                admit,
            }) => Attempt::Blocked {
                cause: match cause {
                    schedule::Block::Pool => crate::Block::Pool,
                    schedule::Block::Queue(cause) => crate::Block::Queue(cause),
                    schedule::Block::Unavailable => {
                        return Attempt::Failed(crate::Error::Unavailable);
                    }
                },
                target: admit,
                frame,
            },
            Err(schedule::Reject::Unavailable) => Attempt::Failed(crate::Error::Unavailable),
            Err(schedule::Reject::Reply(error)) => Attempt::Failed(crate::Error::Reply(error)),
        }
    }
}

pub(in crate::client) struct Committed<'d, T, const ID: u8> {
    target: T,
    token: conn::Id<'d, route::KeyTag<ID>>,
}

pub(in crate::client) enum Attempt<'d, T, const ID: u8> {
    Sent(Committed<'d, T, ID>),
    Blocked {
        cause: crate::Block,
        target: T,
        frame: encode::Complete,
    },
    Failed(crate::Error<'d, ID>),
}

enum State<'app, 's, 'b, 'c, 'd, T, const ID: u8> {
    Admission {
        target: T,
        frame: encode::Complete,
        reply: replies::Admitted<'app, 'b, 'c, 's, 'd, ID>,
    },
    Response {
        sent: Committed<'d, T, ID>,
        reply: replies::Admitted<'app, 'b, 'c, 's, 'd, ID>,
    },
    Failed(crate::Error<'d, ID>),
    Done,
}

#[pin_project::pin_project]
struct Core<'app, 's, 'b, 'c, 'd: 'app + 's, T, const ID: u8> {
    state: State<'app, 's, 'b, 'c, 'd, T, ID>,
    #[pin]
    deadline: Option<deadline::Guard<'d>>,
    #[pin]
    waiter: wait::Waiter<'app, 'd>,
}

impl<'app, 's, 'b, 'c, 'd: 'app + 's, T, const ID: u8> Core<'app, 's, 'b, 'c, 'd, T, ID>
where
    T: Target<'app, 's, 'b, 'c, 'd, ID>,
{
    fn poll_raw(
        self: pin::Pin<&mut Self>,
        mut cx: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> Poll<Result<value::Reply<'s, 'b, 'c, 'd>, ReplyError<'b, 'c, 'd, ID>>> {
        let mut this = self.project();
        loop {
            match (
                mem::replace(this.state, State::Done),
                this.deadline.as_mut().as_pin_mut(),
            ) {
                (
                    State::Admission {
                        target,
                        frame,
                        mut reply,
                    },
                    Some(mut guard),
                ) => {
                    if let Poll::Ready(due) = guard
                        .as_mut()
                        .poll_expired(cx.as_mut(), deadline::RequestPhase::Admission)
                    {
                        this.waiter.as_ref().unregister();
                        return Poll::Ready(Err(crate::Error::from(due).into()));
                    }
                    let redis = target.redis();
                    let sent = match target.try_admit(frame, &mut reply, cx.as_mut().region_token())
                    {
                        Attempt::Sent(sent) => sent,
                        Attempt::Blocked {
                            cause,
                            target,
                            frame,
                        } => {
                            *this.state = State::Admission {
                                target,
                                frame,
                                reply,
                            };
                            if let Err(error) = redis
                                .port
                                .activity
                                .try_dispatch(this.waiter.as_ref(), cx.as_ref())
                            {
                                this.waiter.as_ref().unregister();
                                *this.state = State::Done;
                                guard.as_ref().complete();
                                return Poll::Ready(Err(crate::Error::Dispatch {
                                    blocked: cause,
                                    wait: error,
                                }
                                .into()));
                            }
                            return Poll::Pending;
                        }
                        Attempt::Failed(error) => {
                            this.waiter.as_ref().unregister();
                            guard.as_ref().complete();
                            return Poll::Ready(Err(error.into()));
                        }
                    };
                    this.waiter.as_ref().unregister();
                    *this.state = State::Response { sent, reply };
                }
                (State::Response { sent, mut reply }, Some(mut guard)) => {
                    if let Poll::Ready(due) = guard
                        .as_mut()
                        .poll_expired(cx.as_mut(), deadline::RequestPhase::Response)
                    {
                        this.waiter.as_ref().unregister();
                        let redis = sent.target.redis();
                        redis.port.schedule.reconnect(
                            sent.token,
                            client::Failure::RequestTimeout(due),
                            cx.as_mut().region_token(),
                            &redis.port.activity,
                        );
                        return Poll::Ready(Err(crate::Error::from(due).into()));
                    }
                    let poll = pin::Pin::new(&mut reply).poll_reply(cx.as_mut());
                    if poll.is_pending() {
                        *this.state = State::Response { sent, reply };
                        return Poll::Pending;
                    }
                    guard.as_ref().complete();
                    return poll;
                }
                (State::Failed(error), _) => return Poll::Ready(Err(error.into())),
                (State::Done, _) => return Poll::Pending,
                (State::Admission { .. } | State::Response { .. }, None) => {
                    unreachable!("deadline")
                }
            }
        }
    }
}

/// Projects one completed retained reply without duplicating protocol state.
#[pin_project::pin_project]
#[doc(hidden)]
pub struct Command<'app, 's, 'b, 'c, 'd: 'app + 's, T, D, const ID: u8> {
    #[pin]
    core: Core<'app, 's, 'b, 'c, 'd, T, ID>,
    decoder: marker::PhantomData<fn(D) -> D>,
}

impl<'app, 's, 'b, 'c, 'd: 'app + 's, T, D, const ID: u8> Command<'app, 's, 'b, 'c, 'd, T, D, ID> {
    pub(in crate::client) fn new(target: T, frame: Result<encode::Complete, request::Error>) -> Self
    where
        T: Target<'app, 's, 'b, 'c, 'd, ID>,
    {
        let redis = target.redis();
        let ready = frame.map_err(crate::Error::Request).and_then(|frame| {
            Ok((
                frame,
                redis
                    .port
                    .deadline
                    .guard()
                    .map_err(crate::Error::Deadline)?,
            ))
        });
        let (state, deadline) = match ready {
            Ok((frame, deadline)) => (
                State::Admission {
                    target,
                    frame,
                    reply: replies::Admitted::new(),
                },
                Some(deadline),
            ),
            Err(error) => (State::Failed(error), None),
        };
        Self {
            core: Core {
                state,
                deadline,
                waiter: wait::Waiter::new(),
            },
            decoder: marker::PhantomData,
        }
    }
}

impl<'app, 's, 'b, 'c, 'd: 'app + 's, T, D, const ID: u8> abi::Fiber<'d>
    for Command<'app, 's, 'b, 'c, 'd, T, D, ID>
where
    T: Target<'app, 's, 'b, 'c, 'd, ID>,
    D: value::Decode<'s, 'b, 'c, 'd, ID>,
{
    type Output = Result<D::Output, ReplyError<'b, 'c, 'd, ID>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, context) = call.into_parts();
        let reply = match this.project().core.poll_raw(context) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Ready(Ok(reply)) => reply,
        };
        Poll::Ready(D::decode(reply))
    }
}
