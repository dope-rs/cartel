use crate::api::task::{Budget, Core, State, lease};
use crate::port::Instance;
use crate::port::{replies, requests};
use crate::{api, reply::Error};
use cartel_core::reply;
use dope::core::driver::schedule::clock::timer;
use dope_fiber::{abi, context, wait};
use reply::recv::{self, attach::Attach};
use std::{pin::Pin, task::Poll};

pub(in crate::api) struct Stream<
    'app,
    'g,
    'r,
    's,
    'b,
    'c,
    'd: 'app,
    B: ?Sized,
    I,
    X,
    const ID: u8,
    R,
    M = requests::Boundary,
> where
    I: Instance<Queries: 'd> + 'd,
    X: recv::Extract<'app, 'b, 'c, 'd, replies::Row<'d>, replies::ReplyFailure<'d>>,
{
    core: Core<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, X, ID, R, M>,
    budget: Budget<'d>,
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, X, const ID: u8, R, M>
    Stream<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, X, ID, R, M>
where
    I: Instance<Queries: 'd> + 'd,
    R: Copy + Into<Option<api::Target<'d, ID>>>,
    M: Copy + Into<requests::Boundary>,
    X: recv::Extract<'app, 'b, 'c, 'd, replies::Row<'d>, replies::ReplyFailure<'d>, Context = ()>,
{
    pub(in crate::api) fn new(
        core: Core<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, X, ID, R, M>,
        budget: impl Into<Budget<'d>>,
    ) -> Self {
        Self {
            core,
            budget: budget.into(),
        }
    }
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, T, X, const ID: u8, R, M>
    Stream<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, X, ID, R, M>
where
    I: Instance<Queries: 'd> + 'd,
    R: Copy + Into<Option<api::Target<'d, ID>>>,
    M: Copy + Into<requests::Boundary>,
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
    pub(in crate::api) fn next<'a, F, U>(
        &'a mut self,
        map: F,
    ) -> impl abi::Fiber<'d, Output = U> + 'a
    where
        F: FnMut(Result<Option<T>, Error<'d, ID>>) -> U + 'a,
    {
        Next {
            deadline: self.core.try_alarm::<true>(&self.budget),
            stream: self,
            waiter: wait::Waiter::new(),
            map,
        }
    }

    fn poll_stream<'target>(
        &mut self,
        mut cx: Pin<&mut context::Context<'_, 'd>>,
        waiter: Pin<&wait::Waiter<'target, 'd>>,
    ) -> Poll<Result<Option<T>, Error<'d, ID>>>
    where
        'app: 'target,
        'd: 'target,
    {
        let reply = match self
            .core
            .poll_admit(cx.as_mut(), waiter, lease::Lease::admit)
        {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Ready(Ok(reply)) => reply,
        };
        let next = Pin::new(&mut *reply).poll_next(cx).map(Option::transpose);
        if matches!(&next, Poll::Ready(Ok(None)) | Poll::Ready(Err(_))) {
            reply.detach();
            self.core.admission = State::Done;
        }
        next
    }
}

#[pin_project::pin_project]
struct Next<'borrow, 'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, X, F, const ID: u8, R, M>
where
    I: Instance<Queries: 'd> + 'd,
    X: recv::Extract<'app, 'b, 'c, 'd, replies::Row<'d>, replies::ReplyFailure<'d>>,
{
    stream: &'borrow mut Stream<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, X, ID, R, M>,
    deadline: Result<Option<timer::Alarm<'d>>, timer::Exhausted>,
    #[pin]
    waiter: wait::Waiter<'borrow, 'd>,
    map: F,
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, T, X, F, U, const ID: u8, R, M>
    abi::Fiber<'d> for Next<'_, 'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, X, F, ID, R, M>
where
    I: Instance<Queries: 'd> + 'd,
    R: Copy + Into<Option<api::Target<'d, ID>>>,
    M: Copy + Into<requests::Boundary>,
    X: recv::Extract<
            'app,
            'b,
            'c,
            'd,
            replies::Row<'d>,
            replies::ReplyFailure<'d>,
            Output = Result<T, Error<'d, ID>>,
        >,
    F: FnMut(Result<Option<T>, Error<'d, ID>>) -> U,
{
    type Output = U;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, mut cx) = call.into_parts();
        let this = this.project();
        let waiter = this.waiter.as_ref();
        match this.stream.core.check_deadline(
            &mut this.stream.budget,
            this.deadline,
            waiter,
            cx.as_mut(),
        ) {
            Ok(true) => {}
            Ok(false) => return Poll::Ready((this.map)(Ok(None))),
            Err(error) => return Poll::Ready((this.map)(Err(error))),
        }
        let next = this.stream.poll_stream(cx, waiter);
        if next.is_ready() {
            *this.deadline = Ok(None);
        }
        next.map(this.map)
    }
}
