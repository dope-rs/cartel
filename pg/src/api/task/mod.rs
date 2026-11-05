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
use reply::recv::{self, attach::Attach};
use std::{mem, pin::Pin, task::Poll};

mod call;
mod dispatch;
mod done;
pub(in crate::api) mod group;
pub(crate) mod lease;
mod progress;
pub(super) mod stream;
pub(in crate::api) mod tx;

use dispatch::Dispatch;

pub use call::Call;
pub(super) use call::Map;
pub(super) use done::Done;
pub(super) use progress::Progress;

type Completion<'a, 'd, T, L, const ID: u8> =
    Poll<Result<Progress<'a, 'd, T, L, ID>, Error<'d, ID>>>;

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
        client: api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
        target: Option<api::Target<'d, ID>>,
        request: api::Pending<'d, S>,
    },
    Failed(Error<'d, ID>),
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
    admission: Dispatch<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID, S, A, P>,
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
            admission: Dispatch::new(client, target, next),
            reply,
        }
    }

    pub(super) fn guard(&self) -> Result<timer::Alarm<'d>, timer::Exhausted> {
        self.admission.deadline.guard()
    }

    pub(super) fn poll_time(
        &mut self,
        guard: Pin<&mut timer::Alarm<'d>>,
        ctx: Pin<&mut context::Context<'_, 'd>>,
        phase: deadline::RequestPhase,
    ) -> Poll<deadline::Elapsed> {
        self.admission.deadline.poll_expired(guard, ctx, phase)
    }

    pub(super) fn phase(&self) -> deadline::RequestPhase {
        self.admission.phase()
    }

    pub(super) fn expire(&mut self, time: deadline::Elapsed) -> Error<'d, ID> {
        self.abort(crate::Error::RequestTimeout(time))
    }

    pub(super) fn no_timer(&mut self, error: timer::Exhausted) -> Error<'d, ID> {
        self.abort(crate::Error::Deadline(error))
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
    pub(super) fn poll<'core, 'target>(
        &'core mut self,
        mut cx: Pin<&mut context::Context<'_, 'd>>,
        waiter: Pin<&wait::Waiter<'target, 'd>>,
        seal: impl Copy
        + Fn(
            S,
            api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
            conn::Id<'d, KeyTag<ID>>,
        ) -> lease::Lease<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, A, P, ID>,
    ) -> Completion<'core, 'd, T, lease::Lease<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, A, P, ID>, ID>
    where
        'app: 'target,
        'd: 'target,
    {
        match self
            .admission
            .poll_admit(cx.as_mut(), waiter, &mut self.reply, seal)
        {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => {}
        }
        let next = Pin::new(&mut self.reply).poll_reply(cx);
        match next {
            Poll::Pending => match self.admission.state.admitted() {
                Some(lease) => Poll::Ready(Ok(Progress::Admitted(lease))),
                None => Poll::Ready(Err(crate::Error::Closed.into())),
            },
            Poll::Ready(next) => {
                let Some(lease) = self.admission.state.take() else {
                    return Poll::Ready(Err(crate::Error::Closed.into()));
                };
                Poll::Ready(Ok(Progress::Complete {
                    outcome: next,
                    committed: lease,
                }))
            }
        }
    }

    pub(super) fn poll_done<'target>(
        &mut self,
        cx: Pin<&mut context::Context<'_, 'd>>,
        waiter: Pin<&wait::Waiter<'target, 'd>>,
        seal: impl Copy
        + Fn(
            S,
            api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
            conn::Id<'d, KeyTag<ID>>,
        ) -> lease::Lease<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, A, P, ID>,
    ) -> Poll<Done<'d, T, lease::Lease<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, A, P, ID>, ID>>
    where
        'app: 'target,
        'd: 'target,
    {
        match self.poll(cx, waiter, seal) {
            Poll::Pending | Poll::Ready(Ok(Progress::Admitted(_))) => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Done::AdmissionFailed(error)),
            Poll::Ready(Ok(Progress::Complete {
                outcome: next,
                committed: lease,
            })) => Poll::Ready(Done::Response {
                outcome: next,
                committed: lease,
            }),
        }
    }
}
