use crate::{
    api::{self, Response},
    port::replies,
    reply::Error,
};
use cartel_core::{deadline, reply};
use dope::{
    core::driver::{route::KeyTag, schedule::clock::timer},
    manifold::client::conn,
};
use dope_fiber::context;
use dope_fiber::wait;
use o3::base::cell::region;
use reply::recv::attach::Attach;
use std::{mem, pin::Pin, task::Poll};

mod call;
mod dispatch;
pub(in crate::api) mod group;
pub(crate) mod lease;
pub(super) mod stream;
pub(in crate::api) mod tx;

pub(in crate::api) use dispatch::Budget;

pub(in crate::api) use call::{Call, Read, Receiver};

type Completed<'d, T, L, const ID: u8> = Result<(T, L), Error<'d, ID>>;

enum State<C, R, Q, L, H> {
    Admitted(L, H),
    Connecting { client: C, target: R, request: Q },
    Failed(crate::Error),
    Done,
}

mod raw {
    use super::{Error, State, deadline};
    use std::{mem, ptr};

    impl<C, R, Q, L, H> State<C, R, Q, L, H> {
        pub(super) fn new(client: C, target: R, next: Result<Q, crate::Error>) -> Self {
            match next {
                Ok(request) => Self::Connecting {
                    client,
                    target,
                    request,
                },
                Err(error) => Self::Failed(error),
            }
        }

        pub(super) fn phase(&self) -> deadline::RequestPhase {
            match self {
                Self::Admitted(..) => deadline::RequestPhase::Response,
                _ => deadline::RequestPhase::Admission,
            }
        }

        pub(super) fn take(&mut self) -> Option<L> {
            match mem::replace(self, Self::Done) {
                Self::Admitted(lease, _) => Some(lease),
                _ => None,
            }
        }

        pub(super) fn take_error(&mut self) -> Option<crate::Error> {
            let Self::Failed(error) = *self else {
                return None;
            };
            // Failed owns only this Copy error.
            mem::forget(mem::replace(self, Self::Done));
            Some(error)
        }

        pub(super) fn cancel<'d, const ID: u8>(
            &mut self,
            error: crate::Error,
            linked: impl FnOnce(&mut H) -> bool,
            abort: impl FnOnce(L, crate::Error),
        ) -> Error<'d, ID> {
            let slot = ptr::from_mut(self);
            // SAFETY: the slot is exclusive. Retire the reply in place and consume
            // the disjoint lease once before overwriting the empty variant.
            unsafe {
                match &mut *slot {
                    Self::Admitted(lease, reply) => {
                        let aborting = linked(reply);
                        ptr::drop_in_place(reply);
                        let lease = ptr::read(lease);
                        if aborting {
                            abort(lease, error);
                        }
                        slot.write(Self::Done);
                    }
                    state @ Self::Connecting { .. } => *state = Self::Done,
                    Self::Failed(first) => {
                        let failure = Error::from(*first);
                        slot.write(Self::Done);
                        return failure;
                    }
                    Self::Done => return error.into(),
                }
            }
            error.into()
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
    I: crate::port::Instance<Queries: 'd> + 'd,
    X: Read<'app, 'b, 'c, 'd>,
    A: lease::Target<'d, ID>,
    P: lease::Policy,
{
    admission: State<
        api::Context<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
        R,
        api::Pending<'d, (S, X::Context), M>,
        lease::Lease<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, A, P, ID>,
        Response<'app, 'g, 'r, 's, 'b, 'c, 'd, X::Spec>,
    >,
}

const _: () = assert!(
    mem::size_of::<Result<timer::Alarm<'static>, timer::Exhausted>>()
        == mem::size_of::<Option<timer::Alarm<'static>>>()
);

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, X, const ID: u8, R, M, S, A, P>
    Core<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, X, ID, R, M, S, A, P>
where
    I: crate::port::Instance<Queries: 'd> + 'd,
    X: Read<'app, 'b, 'c, 'd>,
    A: lease::Target<'d, ID>,
    P: lease::Policy,
    R: Copy + Into<Option<api::Target<'d, ID>>>,
    M: Copy + Into<crate::port::requests::Boundary>,
{
    pub(super) fn new(
        client: api::Context<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
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

    pub(in crate::api) fn try_alarm<const EOF: bool>(
        &self,
        budget: &Budget<'d>,
    ) -> Result<Option<timer::Alarm<'d>>, timer::Exhausted> {
        match &self.admission {
            State::Failed(_) => Ok(None),
            State::Done if EOF => Ok(None),
            _ => budget.timer.try_alarm().map(Some),
        }
    }

    #[inline(always)]
    pub(in crate::api) fn check_deadline(
        &mut self,
        budget: &mut Budget<'d>,
        alarm: &mut Result<Option<timer::Alarm<'d>>, timer::Exhausted>,
        waiter: Pin<&wait::Waiter<'_, 'd>>,
        mut cx: Pin<&mut context::Context<'_, 'd>>,
    ) -> Result<bool, Error<'d, ID>> {
        let cause = match alarm {
            Ok(Some(timer)) => {
                if budget.poll_expired(timer, cx.as_mut()).is_pending() {
                    return Ok(true);
                }
                crate::Error::RequestTimeout(deadline::Elapsed::new(self.phase()))
            }
            Err(error) => crate::Error::Deadline(*error),
            Ok(None) => return self.admission.take_error().map_or(Ok(false), |error| Err(error.into())),
        };
        waiter.unregister();
        let error = self.fail(cause, cx.as_mut().region_token());
        *alarm = Ok(None);
        Err(error)
    }

    pub(super) fn poll_admit<'target>(
        &mut self,
        cx: Pin<&mut context::Context<'_, 'd>>,
        waiter: Pin<&wait::Waiter<'target, 'd>>,
        seal: impl Copy
        + Fn(
            S,
            api::Context<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
            conn::Id<'d, KeyTag<ID>>,
        ) -> lease::Lease<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, A, P, ID>,
    ) -> Poll<Result<&mut Response<'app, 'g, 'r, 's, 'b, 'c, 'd, X::Spec>, Error<'d, ID>>>
    where
        'app: 'target,
        'd: 'target,
    {
        self.admission.poll_with(
            cx,
            waiter,
            |reply| reply,
            |this, client, context| {
                client.port.request().try_with(
                    context,
                    this,
                    |region, lane, batch, boundary, this| {
                        let tag = replies::Tag::new(batch, boundary);
                        Response::<X::Spec>::try_with(region, lane, tag, this, |this| {
                            match mem::replace(this, State::Done) {
                                State::Connecting { request, .. } => {
                                    let api::Pending {
                                        frame,
                                        state: (state, context),
                                        ..
                                    } = request;
                                    (context, (this, frame, state))
                                }
                                _ => unreachable!("connecting"),
                            }
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
        match &self.admission {
            State::Admitted(lease, _) => Some(lease),
            _ => None,
        }
    }

    pub(super) fn fail(
        &mut self,
        error: crate::Error,
        token: &region::Token<region::Identity<'d>>,
    ) -> Error<'d, ID> {
        self.admission.cancel(
            error,
            |reply| P::RECOVER || reply.detach_order(token) == reply::order::State::Linked,
            lease::Lease::abort,
        )
    }

    pub(super) fn abort(&mut self, error: crate::Error) -> Error<'d, ID> {
        self.admission.cancel(error, |_| true, lease::Lease::abort)
    }
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, T, X, const ID: u8, R, M, S, A, P>
    Core<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, X, ID, R, M, S, A, P>
where
    I: crate::port::Instance<Queries: 'd> + 'd,
    X: Read<'app, 'b, 'c, 'd, Output = Result<T, Error<'d, ID>>>,
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
            api::Context<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
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
        match X::poll(Pin::new(reply), cx, std::convert::identity) {
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
