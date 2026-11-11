use crate::port::{replies, requests};
use crate::reply::Error;
use crate::{
    api::{self, task},
    schema,
};
use cartel_core::{deadline, reply};
use dope::{
    core::driver::{route::KeyTag, schedule::clock::timer},
    manifold::client::conn,
};
use dope_fiber::{abi, context, wait};
use reply::recv;
use std::{pin, task::Poll};

#[pin_project::pin_project]
pub(in crate::api) struct Call<'app, 'd, C, F> {
    core: C,
    finish: F,
    #[pin]
    deadline: Result<deadline::Guard<'d>, timer::Exhausted>,
    #[pin]
    waiter: wait::Waiter<'app, 'd>,
}

impl<'app, 'd, C, F> Call<'app, 'd, C, F> {
    pub(in crate::api) fn new<T, U, E>(
        core: C,
        deadline: Result<deadline::Guard<'d>, timer::Exhausted>,
        finish: F,
    ) -> Self
    where
        F: FnMut(&mut C, Result<T, E>) -> Result<U, E>,
    {
        Self {
            core,
            finish,
            deadline,
            waiter: wait::Waiter::new(),
        }
    }
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, T, U, X, const ID: u8, R, M, P, F>
    abi::Fiber<'d>
    for Call<
        'app,
        'd,
        task::Core<
            'app,
            'g,
            'r,
            's,
            'b,
            'c,
            'd,
            B,
            I,
            X,
            ID,
            R,
            M,
            (),
            conn::Id<'d, KeyTag<ID>>,
            P,
        >,
        F,
    >
where
    I: schema::QuerySet + 'd,
    R: Copy + Into<Option<api::Target<'d, ID>>>,
    M: Copy + Into<requests::Boundary>,
    P: task::lease::Policy,
    X: recv::Extract<
            'app,
            'b,
            'c,
            'd,
            replies::Row<'d>,
            replies::ReplyFailure<'d>,
            Output = Result<T, Error<'d, ID>>,
        >,
    F: FnMut(
        &mut task::Core<
            'app,
            'g,
            'r,
            's,
            'b,
            'c,
            'd,
            B,
            I,
            X,
            ID,
            R,
            M,
            (),
            conn::Id<'d, KeyTag<ID>>,
            P,
        >,
        Result<T, Error<'d, ID>>,
    ) -> Result<U, Error<'d, ID>>,
{
    type Output = Result<U, Error<'d, ID>>;

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
        if guard.as_mut().poll_expired(cx.as_mut()).is_ready() {
            let time = deadline::Elapsed::new(this.core.phase());
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
            this.finish,
        );
        if next.is_ready() {
            guard.as_mut().complete();
        }
        next
    }
}
