use crate::reply::Error;
use crate::{api::Response, port::replies};
use crate::{api::task, schema};
use cartel_core::{deadline, reply};
use dope::core::driver::schedule::clock::timer;
use dope_fiber::{abi, context, wait};
use reply::recv;
use std::{pin, task::Poll};

#[pin_project::pin_project]
pub struct Call<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, X, const ID: u8>
where
    I: schema::QuerySet + 'd,
    X: recv::Extract<'app, 'b, 'c, 'd, replies::Row<'d>, replies::ReplyFailure<'d>>,
{
    core: task::Core<
        'app,
        'g,
        'r,
        's,
        'b,
        'c,
        'd,
        B,
        I,
        Response<'app, 'g, 'r, 's, 'b, 'c, 'd, X>,
        ID,
    >,
    #[pin]
    deadline: Result<deadline::Guard<'d>, timer::Exhausted>,
    #[pin]
    waiter: wait::Waiter<'app, 'd>,
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, X, const ID: u8>
    Call<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, X, ID>
where
    I: schema::QuerySet + 'd,
    X: recv::Extract<'app, 'b, 'c, 'd, replies::Row<'d>, replies::ReplyFailure<'d>>,
{
    pub(in crate::api) fn from_core(
        core: task::Core<
            'app,
            'g,
            'r,
            's,
            'b,
            'c,
            'd,
            B,
            I,
            Response<'app, 'g, 'r, 's, 'b, 'c, 'd, X>,
            ID,
        >,
        source: deadline::Source<'d>,
    ) -> Self {
        Self {
            core,
            deadline: source.guard(),
            waiter: wait::Waiter::new(),
        }
    }
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, T, X, const ID: u8> abi::Fiber<'d>
    for Call<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, X, ID>
where
    I: schema::QuerySet + 'd,
    X: recv::Extract<
            'app,
            'b,
            'c,
            'd,
            replies::Row<'d>,
            replies::ReplyFailure<'d>,
            Output = Result<T, Error<'d, ID>>,
        >,
{
    type Output = Result<T, Error<'d, ID>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, mut cx) = call.into_parts();
        let mut this = this.project();
        let mut guard = match this.deadline.as_mut().get_mut() {
            Ok(guard) => pin::Pin::new(guard),
            Err(error) => {
                this.waiter.as_ref().unregister();
                return Poll::Ready(Err(this.core.abort(crate::Error::Deadline(*error))));
            }
        };
        let phase = this.core.phase();
        if let Poll::Ready(time) = guard.as_mut().poll_expired(cx.as_mut(), phase) {
            this.waiter.as_ref().unregister();
            return Poll::Ready(Err(this.core.fail(
                crate::Error::RequestTimeout(time),
                cx.as_mut().region_token(),
            )));
        }
        let next = this.core.poll(
            cx.as_mut(),
            this.waiter.as_ref(),
            task::lease::Lease::admit,
            |_, outcome| outcome,
        );
        if next.is_ready() {
            guard.as_mut().complete();
        }
        next
    }
}
