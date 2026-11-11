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

pub(in crate::api) use call::Call;

type Completed<'d, T, L, const ID: u8> = Result<(T, L), Error<'d, ID>>;

enum State<C, R, Q, L, H> {
    Admitted(L, H),
    Connecting { client: C, target: R, request: Q },
    Failed(crate::Error),
    Done,
}

impl<C, R, Q, L, H> State<C, R, Q, L, H> {
    fn new(client: C, target: R, next: Result<Q, crate::Error>) -> Self {
        match next {
            Ok(request) => Self::Connecting {
                client,
                target,
                request,
            },
            Err(error) => Self::Failed(error),
        }
    }

    fn phase(&self) -> deadline::RequestPhase {
        match self {
            Self::Admitted(..) => deadline::RequestPhase::Response,
            _ => deadline::RequestPhase::Admission,
        }
    }

    fn admitted(&self) -> Option<&L> {
        match self {
            Self::Admitted(lease, _) => Some(lease),
            _ => None,
        }
    }

    fn take(&mut self) -> Option<L> {
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
    X,
    const ID: u8,
    R = api::Pooled,
    M = crate::port::requests::Boundary,
    S = (),
    A = conn::Id<'d, KeyTag<ID>>,
    P = lease::Detached,
> where
    I: schema::QuerySet + 'd,
    X: recv::Extract<'app, 'b, 'c, 'd, replies::Row<'d>, replies::ReplyFailure<'d>>,
    A: lease::Target<'d, ID>,
    P: lease::Policy,
{
    admission: State<
        api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
        R,
        api::Pending<'d, (S, X::Context), M>,
        lease::Lease<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, A, P, ID>,
        Response<'app, 'g, 'r, 's, 'b, 'c, 'd, X>,
    >,
}

const _: () = assert!(
    mem::size_of::<Result<timer::Alarm<'static>, timer::Exhausted>>()
        == mem::size_of::<Option<timer::Alarm<'static>>>()
);

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, X, const ID: u8, R, M, S, A, P>
    Core<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, X, ID, R, M, S, A, P>
where
    I: schema::QuerySet + 'd,
    X: recv::Extract<'app, 'b, 'c, 'd, replies::Row<'d>, replies::ReplyFailure<'d>>,
    A: lease::Target<'d, ID>,
    P: lease::Policy,
    R: Copy + Into<Option<api::Target<'d, ID>>>,
    M: Copy + Into<crate::port::requests::Boundary>,
{
    pub(super) fn new(
        client: api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
        target: R,
        context: X::Context,
        next: Result<api::Pending<'d, S, M>, crate::Error>,
    ) -> Self {
        Self {
            admission: State::new(
                client,
                target,
                next.map(|request| request.map_state(|state| (state, context))),
            ),
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
    ) -> Poll<Result<&mut Response<'app, 'g, 'r, 's, 'b, 'c, 'd, X>, Error<'d, ID>>>
    where
        'app: 'target,
        'd: 'target,
    {
        self.admission.poll_with(
            cx,
            waiter,
            |reply| reply,
            |this, client, weight, context| {
                client.port.request().try_with(
                    context,
                    weight,
                    this,
                    |region, lane, tag, this| {
                        Response::<X>::try_with(region, lane, tag, this, |this| match mem::replace(
                            this,
                            State::Done,
                        ) {
                            State::Connecting { request, .. } => {
                                let api::Pending {
                                    frame,
                                    state: (state, context),
                                    ..
                                } = request;
                                (context, (this, frame, state))
                            }
                            _ => unreachable!("connecting"),
                        })
                        .map(|(depth, reply, input)| (depth, (reply, input)))
                    },
                    |(reply, (this, frame, state)), conn| {
                        let admitted = State::Admitted(seal(state, client, conn), reply);
                        // Registration retained this exclusive, empty Done slot.
                        mem::forget(mem::replace(this, admitted));
                        let State::Admitted(_, reply) = this else {
                            unreachable!("admitted");
                        };
                        (frame, reply)
                    },
                )
            },
        )
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
            && let State::Admitted(_, reply) = &mut self.admission
            && reply.detach_order(token) == reply::order::State::Detached
        {
            self.admission = State::Done;
        }
        self.abort(error)
    }

    pub(super) fn abort(&mut self, error: crate::Error) -> Error<'d, ID> {
        if let State::Admitted(_, reply) = &mut self.admission {
            reply.detach();
        }
        self.admission.cancel(error)
    }
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, T, X, const ID: u8, R, M, S, A, P>
    Core<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, X, ID, R, M, S, A, P>
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
    R: Copy + Into<Option<api::Target<'d, ID>>>,
    M: Copy + Into<crate::port::requests::Boundary>,
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
        finish: impl FnOnce(&mut Self, Result<T, Error<'d, ID>>) -> Result<U, Error<'d, ID>>,
    ) -> Poll<Result<U, Error<'d, ID>>>
    where
        'app: 'target,
        'd: 'target,
    {
        let reply = match self.poll_admit(cx.as_mut(), waiter, seal) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Ready(Ok(reply)) => reply,
        };
        match Pin::new(reply).poll_reply(cx) {
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
