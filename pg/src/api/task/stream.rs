use crate::api::task::{Core, State, lease};
use crate::port::replies;
use crate::{api, reply::Error, schema};
use cartel_core::reply;
use dope::core::driver::schedule::clock::timer;
use dope_fiber::{abi, context, wait};
use reply::recv::{self, attach::Attach};
use std::{pin::Pin, task::Poll};

pub(in crate::api) struct Stream<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, X, const ID: u8>
where
    I: schema::QuerySet + 'd,
    X: recv::Extract<'app, 'b, 'c, 'd, replies::Row<'d>, replies::ReplyFailure<'d>>,
{
    core: Core<
        'app,
        'g,
        'r,
        's,
        'b,
        'c,
        'd,
        B,
        I,
        recv::Stream<
            'app,
            'b,
            'c,
            'd,
            replies::Row<'d>,
            X,
            replies::ReplyFailure<'d>,
            replies::Tag<'g, 'r, 's, 'b, 'c, 'd>,
        >,
        ID,
    >,
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, X, const ID: u8>
    Stream<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, X, ID>
where
    I: schema::QuerySet + 'd,
    X: recv::Extract<'app, 'b, 'c, 'd, replies::Row<'d>, replies::ReplyFailure<'d>>,
{
    pub(in crate::api) fn new(
        client: api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
        target: Option<api::Target<'d, ID>>,
        reply: recv::Stream<
            'app,
            'b,
            'c,
            'd,
            replies::Row<'d>,
            X,
            replies::ReplyFailure<'d>,
            replies::Tag<'g, 'r, 's, 'b, 'c, 'd>,
        >,
        next: Result<api::Pending<'d>, crate::Error>,
    ) -> Self {
        Self {
            core: Core::new(client, target, reply, next),
        }
    }
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, T, X, const ID: u8>
    Stream<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, X, ID>
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
    pub(in crate::api) fn next<'a, M, U>(
        &'a mut self,
        map: M,
    ) -> impl abi::Fiber<'d, Output = U> + 'a
    where
        M: FnMut(Result<Option<T>, Error<'d, ID>>) -> U + 'a,
    {
        Next {
            deadline: match self.core.admission.state {
                State::Done => Ok(None),
                _ => self.core.guard().map(Some),
            },
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
        match self.core.admission.poll_admit(
            cx.as_mut(),
            waiter,
            &mut self.core.reply,
            lease::Lease::admit,
        ) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => {}
        }
        let next = Pin::new(&mut self.core.reply)
            .poll_next(cx)
            .map(Option::transpose);
        if matches!(&next, Poll::Ready(Ok(None)) | Poll::Ready(Err(_))) {
            self.core.reply.detach();
            self.core.admission.state = State::Done;
        }
        next
    }
}

#[pin_project::pin_project]
struct Next<'borrow, 'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, X, M, const ID: u8>
where
    I: schema::QuerySet + 'd,
    X: recv::Extract<'app, 'b, 'c, 'd, replies::Row<'d>, replies::ReplyFailure<'d>>,
{
    stream: &'borrow mut Stream<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, X, ID>,
    #[pin]
    deadline: Result<Option<timer::Alarm<'d>>, timer::Exhausted>,
    #[pin]
    waiter: wait::Waiter<'borrow, 'd>,
    map: M,
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, T, X, M, U, const ID: u8> abi::Fiber<'d>
    for Next<'_, 'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, X, M, ID>
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
    M: FnMut(Result<Option<T>, Error<'d, ID>>) -> U,
{
    type Output = U;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, mut cx) = call.into_parts();
        let mut this = this.project();
        let waiter = this.waiter.as_ref();
        let mut guard = match this.deadline.as_mut().get_mut() {
            Ok(Some(guard)) => Pin::new(guard),
            Ok(None) => return Poll::Ready((this.map)(Ok(None))),
            Err(error) => {
                waiter.unregister();
                return Poll::Ready((this.map)(Err(this.stream.core.no_timer(*error))));
            }
        };
        let phase = this.stream.core.phase();
        if let Poll::Ready(time) = this
            .stream
            .core
            .poll_time(guard.as_mut(), cx.as_mut(), phase)
        {
            waiter.unregister();
            return Poll::Ready((this.map)(Err(this.stream.core.expire(time))));
        }
        let next = this.stream.poll_stream(cx.as_mut(), waiter);
        if next.is_ready() {
            guard.as_ref().clear();
        }
        next.map(this.map)
    }
}
