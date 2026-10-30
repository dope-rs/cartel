use crate::reply::Error;
use crate::{api::Response, port::replies};
use crate::{api::task, schema};
use cartel_core::reply;
use dope::core::driver::schedule::clock::timer;
use dope_fiber::{abi, context, wait};
use reply::recv;
use std::{pin, task::Poll};

/// Projects a completed reply after its original call has released its resources.
#[pin_project::pin_project]
pub(in crate::api) struct Map<F, M> {
    #[pin]
    task: abi::Task<F>,
    map: M,
}

impl<F, M> Map<F, M> {
    pub(in crate::api) const fn new(fiber: F, map: M) -> Self {
        Self {
            task: abi::Task::new(fiber),
            map,
        }
    }
}

impl<'d, F: abi::Fiber<'d>, M, T> abi::Fiber<'d> for Map<F, M>
where
    M: FnMut(F::Output) -> T,
{
    type Output = T;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<T> {
        let (this, admit) = call.into_admit();
        let this = this.project();
        admit.poll(this.task).0.map(this.map)
    }
}

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
    deadline: Result<timer::Alarm<'d>, timer::Exhausted>,
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
    ) -> Self {
        let guard = core.guard();
        Self {
            core,
            deadline: guard,
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
                return Poll::Ready(Err(this.core.no_timer(*error)));
            }
        };
        let phase = this.core.phase();
        if let Poll::Ready(time) = this.core.poll_time(guard.as_mut(), cx.as_mut(), phase) {
            this.waiter.as_ref().unregister();
            return Poll::Ready(Err(this.core.expire(time)));
        }
        let next =
            match this
                .core
                .poll_done(cx.as_mut(), this.waiter.as_ref(), task::lease::Lease::admit)
            {
                Poll::Pending => Poll::Pending,
                Poll::Ready(task::Done::AdmissionFailed(error)) => Poll::Ready(Err(error)),
                Poll::Ready(task::Done::Response { outcome, .. }) => Poll::Ready(outcome),
            };
        if next.is_ready() {
            guard.as_ref().clear();
        }
        next
    }
}
