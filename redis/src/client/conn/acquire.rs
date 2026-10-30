use crate::client::{self, conn};
use cartel_core::deadline;
use dope::core::driver::schedule::clock::timer;
use dope_fiber::{abi, context, wait};
use std::{mem, pin, task};

const _: () = assert!(
    mem::size_of::<Result<deadline::Guard<'static>, timer::Exhausted>>()
        == mem::size_of::<Option<deadline::Guard<'static>>>()
);

#[pin_project::pin_project]
pub(in crate::client) struct Acquire<'app, 's, 'b, 'c, 'd: 'app, B: ?Sized, const ID: u8> {
    redis: client::Redis<'app, 's, 'b, 'c, 'd, B, ID>,
    #[pin]
    deadline: Result<deadline::Guard<'d>, timer::Exhausted>,
    #[pin]
    waiter: wait::Waiter<'app, 'd>,
}

impl<'app, 's, 'b, 'c, 'd: 'app, B: ?Sized, const ID: u8> Acquire<'app, 's, 'b, 'c, 'd, B, ID> {
    pub(in crate::client) fn new(redis: client::Redis<'app, 's, 'b, 'c, 'd, B, ID>) -> Self {
        Self {
            redis,
            deadline: redis.port.deadline.guard(),
            waiter: wait::Waiter::new(),
        }
    }
}

impl<'app, 's, 'b, 'c, 'd: 'app, B: ?Sized, const ID: u8> abi::Fiber<'d>
    for Acquire<'app, 's, 'b, 'c, 'd, B, ID>
{
    type Output = Result<conn::Connection<'app, 's, 'b, 'c, 'd, B, ID>, crate::Error<'d, ID>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, mut ctx) = call.into_parts();
        let mut this = this.project();
        let mut guard = match this.deadline.as_mut().get_mut() {
            Ok(guard) => pin::Pin::new(guard),
            Err(error) => {
                this.waiter.as_ref().unregister();
                return task::Poll::Ready(Err(crate::Error::Deadline(*error)));
            }
        };
        if let task::Poll::Ready(due) = guard
            .as_mut()
            .poll_expired(ctx.as_mut(), deadline::RequestPhase::Lease)
        {
            this.waiter.as_ref().unregister();
            return task::Poll::Ready(Err(due.into()));
        }
        let redis = *this.redis;
        if let Some(permit) = redis
            .port
            .schedule
            .try_lease(redis, ctx.as_mut().region_token())
        {
            this.waiter.as_ref().unregister();
            guard.as_ref().complete();
            return task::Poll::Ready(Ok(conn::Connection { lease: permit }));
        }
        if !redis.port.is_active()
            && let Some(cause) = redis.port.activity.fatal_cause(ctx.as_mut().region_token())
        {
            this.waiter.as_ref().unregister();
            guard.as_ref().complete();
            return task::Poll::Ready(Err(cause));
        }
        if let Err(error) = redis
            .port
            .activity
            .try_lease(this.waiter.as_ref(), ctx.as_ref())
        {
            this.waiter.as_ref().unregister();
            guard.as_ref().complete();
            return task::Poll::Ready(Err(crate::Error::Wait(error)));
        }
        task::Poll::Pending
    }
}
