use std::{marker, mem, pin, task::Poll};

use cartel_core::{
    deadline,
    reply::{order, recv::attach::Attach},
};
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
    type T = Pooled<'static, 'static, 'static, 'static, 'static, (), 0>;
    let word = mem::size_of::<usize>();
    assert!(mem::size_of::<T>() == word);
    assert!(mem::size_of::<State<'static, 'static, 'static, 'static, 'static, 0>>() == 6 * word);
    assert!(
        mem::size_of::<Core<'static, 'static, 'static, 'static, 'static, T, 0>>()
            == 7 * word
                + mem::size_of::<Option<deadline::Guard<'static>>>()
                + mem::size_of::<wait::Waiter<'static, 'static>>()
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

impl<'app, 's, 'b, 'c, 'd: 'app, B: ?Sized, const ID: u8>
    AsRef<crate::Port<'s, 'app, 'b, 'c, 'd, ID>> for Pooled<'app, 's, 'b, 'c, 'd, B, ID>
{
    fn as_ref(&self) -> &crate::Port<'s, 'app, 'b, 'c, 'd, ID> {
        &self.0.port
    }
}

pub(in crate::client) trait Target<'app, 's: 'b, 'b: 'app, 'c: 'b, 'd: 's + 'c, const ID: u8>:
    Sized + AsRef<crate::Port<'s, 'app, 'b, 'c, 'd, ID>>
{
    type Provider: ?Sized;

    fn redis(&self) -> client::Redis<'app, 's, 'b, 'c, 'd, Self::Provider, ID>;

    fn try_admit(
        &self,
        frame: encode::Complete,
        reply: &mut replies::Admitted<'app, 'b, 'c, 's, 'd, ID>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Result<conn::Id<'d, route::KeyTag<ID>>, schedule::Reject<'d, ID>>;
}

impl<'app, 's, 'b, 'c, 'd: 'app, B: ?Sized, const ID: u8> Target<'app, 's, 'b, 'c, 'd, ID>
    for Pooled<'app, 's, 'b, 'c, 'd, B, ID>
{
    type Provider = B;

    fn redis(&self) -> client::Redis<'app, 's, 'b, 'c, 'd, B, ID> {
        self.0
    }

    fn try_admit(
        &self,
        frame: encode::Complete,
        reply: &mut replies::Admitted<'app, 'b, 'c, 's, 'd, ID>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Result<conn::Id<'d, route::KeyTag<ID>>, schedule::Reject<'d, ID>> {
        let redis = self.0;
        match redis.port.schedule.try_pool(region, frame, reply) {
            Err(schedule::Reject::Blocked {
                cause: crate::Block::Inactive,
                frame,
            }) => match redis.port.activity.fatal_cause(region) {
                Some(cause) => Err(schedule::Reject::Failed(cause)),
                None => Err(schedule::Reject::Blocked {
                    cause: crate::Block::Inactive,
                    frame,
                }),
            },
            result => result,
        }
    }
}

impl<'app, 's, 'b, 'c, 'd: 'app, B: ?Sized, const ID: u8> Target<'app, 's, 'b, 'c, 'd, ID>
    for &mut schedule::Lease<'app, 's, 'b, 'c, 'd, B, ID>
{
    type Provider = B;

    fn redis(&self) -> client::Redis<'app, 's, 'b, 'c, 'd, B, ID> {
        schedule::Lease::redis(self)
    }

    fn try_admit(
        &self,
        frame: encode::Complete,
        reply: &mut replies::Admitted<'app, 'b, 'c, 's, 'd, ID>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Result<conn::Id<'d, route::KeyTag<ID>>, schedule::Reject<'d, ID>> {
        let redis = schedule::Lease::redis(self);
        let token = self.target();
        redis
            .port
            .schedule
            .try_dispatch_lease(region, frame, reply, token)
    }
}

enum State<'app, 's, 'b, 'c, 'd, const ID: u8> {
    Admission(encode::Complete),
    Response {
        token: conn::Id<'d, route::KeyTag<ID>>,
        reply: replies::Admitted<'app, 'b, 'c, 's, 'd, ID>,
    },
    Request(request::Error),
    Deadline(dope::core::driver::schedule::clock::timer::Exhausted),
    Done,
}

#[pin_project::pin_project(PinnedDrop)]
struct Core<'app, 's: 'b, 'b: 'app, 'c: 'b, 'd: 's + 'c, T, const ID: u8>
where
    T: AsRef<crate::Port<'s, 'app, 'b, 'c, 'd, ID>>,
{
    target: T,
    state: State<'app, 's, 'b, 'c, 'd, ID>,
    #[pin]
    deadline: Option<deadline::Guard<'d>>,
    #[pin]
    waiter: wait::Waiter<'app, 'd>,
}

#[pin_project::pinned_drop]
impl<'app, 's: 'b, 'b: 'app, 'c: 'b, 'd: 's + 'c, T, const ID: u8> PinnedDrop
    for Core<'app, 's, 'b, 'c, 'd, T, ID>
where
    T: AsRef<crate::Port<'s, 'app, 'b, 'c, 'd, ID>>,
{
    fn drop(self: pin::Pin<&mut Self>) {
        let this = self.project();
        if matches!(this.state, State::Admission(_)) {
            this.target
                .as_ref()
                .discard(mem::replace(this.state, State::Done));
        }
    }
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
            let guard = this.deadline.as_mut().as_pin_mut();
            let error = match &mut *this.state {
                State::Response { token, reply } => {
                    let Some(mut guard) = guard else {
                        unreachable!("deadline")
                    };
                    if guard.as_mut().poll_expired(cx.as_mut()).is_ready() {
                        let due = deadline::Elapsed::new(deadline::RequestPhase::Response);
                        let redis = this.target.redis();
                        if matches!(
                            reply.detach_order(cx.as_mut().region_token()),
                            order::State::Linked
                        ) {
                            redis.port.schedule.reconnect(
                                *token,
                                client::Failure::RequestTimeout(due),
                                cx.as_mut().region_token(),
                                &redis.port.activity,
                            );
                        }
                        *this.state = State::Done;
                        return Poll::Ready(Err(crate::Error::from(due).into()));
                    }
                    let poll = pin::Pin::new(reply).poll_reply(cx.as_mut());
                    if poll.is_ready() {
                        *this.state = State::Done;
                    }
                    return poll;
                }
                State::Admission(_) => {
                    let frame = match mem::replace(this.state, State::Done) {
                        State::Admission(frame) => frame,
                        State::Response { reply: _reply, .. } => unreachable!("response"),
                        _ => unreachable!("admission"),
                    };
                    let Some(mut guard) = guard else {
                        unreachable!("deadline")
                    };
                    if guard.as_mut().poll_expired(cx.as_mut()).is_ready() {
                        let due = deadline::Elapsed::new(deadline::RequestPhase::Admission);
                        this.waiter.as_ref().unregister();
                        this.target.as_ref().discard(frame);
                        return Poll::Ready(Err(crate::Error::from(due).into()));
                    }
                    let redis = this.target.redis();
                    let mut reply = replies::Admitted::new();
                    let token =
                        match this
                            .target
                            .try_admit(frame, &mut reply, cx.as_mut().region_token())
                        {
                            Ok(token) => token,
                            Err(schedule::Reject::Blocked { cause, frame }) => {
                                *this.state = State::Admission(frame);
                                if let Err(error) = redis
                                    .port
                                    .activity
                                    .try_dispatch(this.waiter.as_ref(), cx.as_ref())
                                {
                                    this.waiter.as_ref().unregister();
                                    this.target
                                        .as_ref()
                                        .discard(mem::replace(this.state, State::Done));
                                    return Poll::Ready(Err(crate::Error::Dispatch {
                                        blocked: cause,
                                        wait: error,
                                    }
                                    .into()));
                                }
                                return Poll::Pending;
                            }
                            Err(schedule::Reject::Failed(error)) => {
                                this.waiter.as_ref().unregister();
                                this.target.as_ref().activity.wake_send();
                                return Poll::Ready(Err(error.into()));
                            }
                        };
                    this.waiter.as_ref().unregister();
                    *this.state = State::Response { token, reply };
                    continue;
                }
                State::Request(error) => crate::Error::Request(*error),
                State::Deadline(error) => crate::Error::Deadline(*error),
                State::Done => return Poll::Pending,
            };
            *this.state = State::Done;
            return Poll::Ready(Err(error.into()));
        }
    }
}

/// Projects one completed retained reply without duplicating protocol state.
#[pin_project::pin_project]
#[doc(hidden)]
pub struct Command<'app, 's, 'b, 'c, 'd: 'app + 's, T, D, const ID: u8>
where
    T: AsRef<crate::Port<'s, 'app, 'b, 'c, 'd, ID>>,
{
    #[pin]
    core: Core<'app, 's, 'b, 'c, 'd, T, ID>,
    decoder: marker::PhantomData<fn(D) -> D>,
}

impl<'app, 's, 'b, 'c, 'd: 'app + 's, T, D, const ID: u8> Command<'app, 's, 'b, 'c, 'd, T, D, ID>
where
    T: AsRef<crate::Port<'s, 'app, 'b, 'c, 'd, ID>>,
{
    pub(in crate::client) fn new(
        target: T,
        frame: Result<encode::Complete, request::Error>,
        deadline: impl FnOnce() -> Result<
            deadline::Guard<'d>,
            dope::core::driver::schedule::clock::timer::Exhausted,
        >,
    ) -> Self {
        let (state, deadline) = match frame {
            Err(error) => (State::Request(error), None),
            Ok(frame) => match deadline() {
                Ok(deadline) => (State::Admission(frame), Some(deadline)),
                Err(error) => {
                    target.as_ref().discard(frame);
                    (State::Deadline(error), None)
                }
            },
        };
        Self {
            core: Core {
                target,
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
        let mut this = this.project();
        let Poll::Ready(reply) = this.core.as_mut().poll_raw(context) else {
            return Poll::Pending;
        };
        this.core.project().deadline.set(None);
        Poll::Ready(D::decode(reply?))
    }
}
