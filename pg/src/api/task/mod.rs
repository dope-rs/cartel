use crate::{
    api::{self, Response},
    port::replies,
    reply::Error,
    schema,
};
use cartel_core::{deadline, reply};
use dope::{
    core::driver::{route::KeyTag, schedule::clock::timer},
    manifold::client::conn,
};
use dope_fiber::context;
use dope_fiber::wait;
use o3::base::cell::region;
use reply::recv::{self, attach::Attach};
use std::{mem, pin::Pin, task::Poll};

mod call;
mod dispatch;
pub(in crate::api) mod group;
pub(crate) mod lease;
pub(super) mod stream;
pub(in crate::api) mod tx;

pub(in crate::api) use dispatch::Budget;

pub use call::Call;

type Completed<'d, T, L, const ID: u8> = Result<(T, L), Error<'d, ID>>;

enum State<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, const ID: u8, S, A, P, H = ()>
where
    I: schema::QuerySet + 'd,
    A: lease::Target<'d, ID>,
    P: lease::Policy,
{
    Admitted(
        lease::Lease<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, A, P, ID>,
        H,
    ),
    Connecting {
        target: Option<api::Target<'d, ID>>,
        request: api::Pending<'d, S>,
    },
    Failed(crate::Error),
    Done,
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, const ID: u8, S, A, P, H>
    State<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID, S, A, P, H>
where
    I: schema::QuerySet + 'd,
    A: lease::Target<'d, ID>,
    P: lease::Policy,
{
    fn admitted(&self) -> Option<&lease::Lease<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, A, P, ID>> {
        match self {
            Self::Admitted(lease, _) => Some(lease),
            _ => None,
        }
    }

    fn take(&mut self) -> Option<lease::Lease<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, A, P, ID>> {
        match mem::replace(self, Self::Done) {
            Self::Admitted(lease, _) => Some(lease),
            _ => None,
        }
    }
}

pub(super) struct Core<
    'app,
    'g,
    'r,
    's,
    'b,
    'c,
    'd: 'app,
    B: ?Sized,
    I,
    R,
    const ID: u8,
    S = (),
    A = conn::Id<'d, KeyTag<ID>>,
    P = lease::Detached,
> where
    I: schema::QuerySet + 'd,
    A: lease::Target<'d, ID>,
    P: lease::Policy,
{
    client: api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
    admission: State<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID, S, A, P>,
    reply: R,
}

const _: () = assert!(
    mem::size_of::<Result<timer::Alarm<'static>, timer::Exhausted>>()
        == mem::size_of::<Option<timer::Alarm<'static>>>()
);

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, R, const ID: u8, S, A, P>
    Core<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, R, ID, S, A, P>
where
    I: schema::QuerySet + 'd,
    R: Attach<
            'app,
            'b,
            'c,
            'd,
            replies::Row<'d>,
            replies::ReplyFailure<'d>,
            replies::Tag<'g, 'r, 's, 'b, 'c, 'd>,
        >,
    A: lease::Target<'d, ID>,
    P: lease::Policy,
{
    pub(super) fn new(
        client: api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
        target: Option<api::Target<'d, ID>>,
        reply: R,
        next: Result<api::Pending<'d, S>, crate::Error>,
    ) -> Self {
        Self {
            client,
            admission: State::new(target, next),
            reply,
        }
    }

    pub(super) fn poll_admit<'target>(
        &mut self,
        cx: Pin<&mut context::Context<'_, 'd>>,
        waiter: Pin<&wait::Waiter<'target, 'd>>,
        seal: impl Copy
        + Fn(
            S,
            api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
            conn::Id<'d, KeyTag<ID>>,
        ) -> lease::Lease<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, A, P, ID>,
    ) -> Poll<Result<(), Error<'d, ID>>>
    where
        'app: 'target,
        'd: 'target,
    {
        self.admission
            .poll_with(&self.client, cx, waiter, |this, client, weight, context| {
                client
                    .port
                    .request()
                    .try_reply(weight, this, &mut self.reply, context, |this, conn| {
                        match mem::replace(this, State::Done) {
                            State::Connecting { request, .. } => {
                                let api::Pending { frame, state, .. } = request;
                                *this = State::Admitted(seal(state, client, conn), ());
                                (frame, ())
                            }
                            _ => unreachable!("connecting"),
                        }
                    })
                    .map_err(|(error, conn, _)| (error, conn))
            })
    }

    pub(super) fn phase(&self) -> deadline::RequestPhase {
        self.admission.phase()
    }

    pub(super) fn admitted(
        &self,
    ) -> Option<&lease::Lease<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, A, P, ID>> {
        self.admission.admitted()
    }

    pub(super) fn fail(
        &mut self,
        error: crate::Error,
        token: &region::Token<region::Identity<'d>>,
    ) -> Error<'d, ID> {
        if !P::RECOVER
            && self.reply.detach_order(token) == reply::order::State::Detached
            && matches!(self.admission, State::Admitted(..))
        {
            self.admission = State::Done;
        }
        self.abort(error)
    }

    pub(super) fn abort(&mut self, error: crate::Error) -> Error<'d, ID> {
        self.reply.detach();
        self.admission.cancel(error)
    }
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, T, X, const ID: u8, S, A, P>
    Core<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, Response<'app, 'g, 'r, 's, 'b, 'c, 'd, X>, ID, S, A, P>
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
    A: lease::Target<'d, ID>,
    P: lease::Policy,
{
    pub(super) fn poll<'target, U>(
        &mut self,
        mut cx: Pin<&mut context::Context<'_, 'd>>,
        waiter: Pin<&wait::Waiter<'target, 'd>>,
        seal: impl Copy
        + Fn(
            S,
            api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
            conn::Id<'d, KeyTag<ID>>,
        ) -> lease::Lease<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, A, P, ID>,
        finish: fn(&mut Self, Result<T, Error<'d, ID>>) -> Result<U, Error<'d, ID>>,
    ) -> Poll<Result<U, Error<'d, ID>>>
    where
        'app: 'target,
        'd: 'target,
    {
        match self.poll_admit(cx.as_mut(), waiter, seal) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => {}
        }
        match Pin::new(&mut self.reply).poll_reply(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(outcome) => Poll::Ready(finish(self, outcome)),
        }
    }

    pub(super) fn complete(
        &mut self,
        outcome: Result<T, Error<'d, ID>>,
    ) -> Completed<'d, X::Output, lease::Lease<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, A, P, ID>, ID>
    {
        match self.admission.take() {
            Some(lease) => Ok((outcome, lease)),
            None => Err(crate::Error::Closed.into()),
        }
    }
}
