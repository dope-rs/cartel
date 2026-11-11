use std::{array, marker::PhantomData, mem, ops::ControlFlow, pin::Pin, task::Poll};

use cartel_core::{deadline, reply};
use dope::{
    core::driver::{route::KeyTag, schedule::clock::timer},
    manifold::client::conn,
};
use dope_fiber::{abi, context, wait};
use o3::{base::cell::region, depot::array::init};
use reply::{
    Slot,
    recv::{
        self,
        attach::{self, Attach},
        outcome::Terminal,
    },
};

use crate::{
    api::{
        self, Response,
        results::{
            GroupFailure, GroupResult, Row,
            group::{Diagnostics, Outcome},
        },
    },
    port::replies::{self, groups},
    reply::{Error, group::Ledger},
    schema,
};

use crate::api::task::{State, lease};

type Output<'r, 's, 'b, 'c, 'd, Q, const ID: u8, const N: usize> =
    GroupResult<'d, Row<replies::Payload<'b, 'c, 'd>, Q>, Ledger<'r, 's>, N, ID>;

struct First<Q>(PhantomData<Q>);

impl<'app, 'b, 'c: 'b, 'd: 'c, Q>
    recv::Extract<'app, 'b, 'c, 'd, replies::Row<'d>, replies::ReplyFailure<'d>> for First<Q>
where
    Q: schema::TypedQuery<Result = schema::result::Rows>,
{
    type Output = Outcome<Row<replies::Payload<'b, 'c, 'd>, Q>, replies::ReplyFailure<'d>>;
    type Context = ();
    const ACCEPTANCE: recv::Acceptance = recv::Acceptance::First;

    fn extract(
        _: &(),
        slot: &mut Slot<'_, 'app, 'b, 'c, 'd, replies::Row<'d>, replies::ReplyFailure<'d>>,
    ) -> Option<Self::Output> {
        Some(match slot.terminal()? {
            Terminal::Complete(mut done) => {
                match Row::required(
                    done.try_node()
                        .map(|payload| payload.map(replies::Payload::from_node))
                        .map_err(crate::Error::ResponseRetain),
                ) {
                    Ok(row) => Outcome::Completed(row),
                    Err(error) => Outcome::Failed(replies::ReplyFailure::Failed(error)),
                }
            }
            Terminal::Failed(failure) => match failure.into_error() {
                error @ replies::ReplyFailure::Server(_) => Outcome::Failed(error),
                replies::ReplyFailure::GroupServer => Outcome::Server,
                replies::ReplyFailure::Failed(crate::Error::Skipped) => Outcome::Skipped,
                error @ replies::ReplyFailure::Failed(_) => Outcome::Interrupted(error),
            },
        })
    }
}

type Admit<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, const ID: u8, R> = State<
    api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
    R,
    api::Pending<'d, groups::Draft<'g, 'r, 's, 'b, 'c, 'd>>,
    lease::Lease<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, conn::Id<'d, KeyTag<ID>>, lease::Detached, ID>,
    groups::Owner<'g, 'r, 's, 'b, 'c, 'd>,
>;

enum Stop<'d, const ID: u8> {
    Deadline(crate::Error),
    Admission(Error<'d, ID>),
    Sync,
}

fn take<'app, 'g, 'r, 's, 'b, 'c: 'b, 'd: 'c, Q>(
    command: &mut Response<'app, 'g, 'r, 's, 'b, 'c, 'd, First<Q>>,
    token: &mut region::Token<region::Identity<'d>>,
) -> Outcome<Row<replies::Payload<'b, 'c, 'd>, Q>, replies::ReplyFailure<'d>>
where
    Q: schema::TypedQuery<Result = schema::result::Rows>,
{
    match command.try_take(token) {
        Some(outcome) => outcome,
        None => {
            command.detach();
            Outcome::Unobserved
        }
    }
}

#[pin_project::pin_project]
pub(in crate::api) struct Group<
    'app,
    'g,
    'r,
    's,
    'b,
    'c: 'b,
    'd: 'c + 'app,
    B: ?Sized,
    I: schema::QuerySet + 'd,
    Q: schema::TypedQuery<Result = schema::result::Rows>,
    const ID: u8,
    const N: usize,
    R,
> {
    admission: Admit<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID, R>,
    commands: [Response<'app, 'g, 'r, 's, 'b, 'c, 'd, First<Q>>; N],
    sync: Response<'app, 'g, 'r, 's, 'b, 'c, 'd, api::ExtractUnit<ID>>,
    #[pin]
    deadline: Result<deadline::Guard<'d>, timer::Exhausted>,
    #[pin]
    waiter: wait::Waiter<'app, 'd>,
}

impl<
    'app,
    'g,
    'r,
    's,
    'b,
    'c: 'b,
    'd: 'c + 'app,
    B: ?Sized,
    I: schema::QuerySet + 'd,
    Q: schema::TypedQuery<Result = schema::result::Rows>,
    const ID: u8,
    const N: usize,
    R,
> Group<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, Q, ID, N, R>
where
    R: Copy + Into<Option<api::Target<'d, ID>>>,
{
    pub(in crate::api) fn new(
        client: api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
        target: R,
        next: Result<api::Pending<'d, groups::Draft<'g, 'r, 's, 'b, 'c, 'd>>, crate::Error>,
        clock: impl Into<Result<deadline::Guard<'d>, timer::Exhausted>>,
    ) -> Self {
        const { assert!(N > 0 && N < u32::MAX as usize) };
        let admit = State::new(client, target, next);
        let deadline = clock.into();
        Self {
            admission: admit,
            commands: array::from_fn(|_| Response::new()),
            sync: Response::new(),
            deadline,
            waiter: wait::Waiter::new(),
        }
    }
}

impl<
    'app,
    'g,
    'r,
    's,
    'b,
    'c: 'b,
    'd: 'c + 'app,
    B: ?Sized,
    I: schema::QuerySet + 'd,
    Q: schema::TypedQuery<Result = schema::result::Rows>,
    const ID: u8,
    const N: usize,
    R,
> abi::Fiber<'d> for Group<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, Q, ID, N, R>
where
    R: Copy + Into<Option<api::Target<'d, ID>>>,
{
    type Output = Output<'r, 's, 'b, 'c, 'd, Q, ID, N>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, mut cx) = call.into_parts();
        let mut this = this.project();
        let mut sync = None;
        let expired = match this.deadline.as_mut().get_mut() {
            Ok(guard) => match Pin::new(guard).poll_expired(cx.as_mut()) {
                Poll::Ready(()) => Some(Stop::Deadline(crate::Error::RequestTimeout(
                    deadline::Elapsed::new(this.admission.phase()),
                ))),
                Poll::Pending => None,
            },
            Err(error) => Some(Stop::Deadline(crate::Error::Deadline(*error))),
        };
        let stop = match expired {
            Some(stop) => stop,
            None => {
                match this.admission.poll_with(
                    cx.as_mut(),
                    this.waiter.as_ref(),
                    |_| (),
                    |admit, client, weight, context| {
                        client.port.request().try_with(
                            context,
                            weight,
                            admit,
                            |token, lane, tag, admit| {
                                let State::Connecting { request, .. } = &*admit else {
                                    unreachable!("connecting");
                                };
                                let reserved = match lane.try_reserve(request.state.usage()) {
                                    Ok(reserved) => reserved,
                                    Err(error) => {
                                        return Err((attach::Error::Payload(error), admit));
                                    }
                                };
                                replies::try_group_by(
                                    token,
                                    lane,
                                    this.commands,
                                    this.sync,
                                    tag.batch(),
                                    admit,
                                    move |admit| match mem::replace(admit, State::Done) {
                                        State::Connecting { request, .. } => {
                                            let api::Pending { frame, state, .. } = request;
                                            let (owner, sync) = state.admit(reserved);
                                            (sync, (admit, frame, owner))
                                        }
                                        _ => unreachable!("connecting"),
                                    },
                                )
                            },
                            |(admit, frame, owner), conn| {
                                *admit =
                                    State::Admitted(lease::Lease::admit((), client, conn), owner);
                                (frame, ())
                            },
                        )
                    },
                ) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => Stop::Admission(error),
                    Poll::Ready(Ok(())) => {
                        let Poll::Ready(observed) =
                            Pin::new(&mut *this.sync).poll_reply(cx.as_mut())
                        else {
                            return Poll::Pending;
                        };
                        sync = Some(observed);
                        Stop::Sync
                    }
                }
            }
        };
        let token = cx.region_token();
        let sync = sync.or_else(|| this.sync.try_take(token));
        let mut storage = mem::MaybeUninit::uninit();
        let mut failures = mem::MaybeUninit::uninit();
        let mut rows = init::Guard::<_, N>::new(&mut storage);
        let collected = loop {
            let vacant = match rows.next() {
                ControlFlow::Break(full) => break Ok(full),
                ControlFlow::Continue(vacant) => vacant,
            };
            match take(vacant.get_mut(this.commands), token) {
                Outcome::Completed(row) => rows = vacant.insert(row),
                outcome => {
                    let mut prefix = vacant
                        .into_iter()
                        .map(Outcome::Completed)
                        .chain(Some(outcome));
                    let mut outcomes = init::Guard::<_, N>::new(&mut failures);
                    let outcomes = loop {
                        match outcomes.next() {
                            ControlFlow::Break(full) => break full,
                            ControlFlow::Continue(vacant) => {
                                let outcome = prefix
                                    .next()
                                    .unwrap_or_else(|| take(vacant.get_mut(this.commands), token));
                                outcomes = vacant.insert(outcome);
                            }
                        }
                    };
                    // Vacant owns fewer than N rows, so N visits consume this prefix and error.
                    mem::forget(prefix);
                    break Err(outcomes);
                }
            }
        };
        this.waiter.as_ref().unregister();
        // Snapshot before detachment and cancellation can retire another order.
        let diagnostics = match &*this.admission {
            State::Admitted(_, owner) => owner.snapshot(),
            _ => Ledger::empty(),
        };
        this.sync.detach();
        let cause = match stop {
            Stop::Deadline(error) => Some(this.admission.cancel(error)),
            stop => {
                this.admission.take();
                if let Ok(guard) = this.deadline.get_mut() {
                    guard.complete();
                }
                if let Stop::Admission(error) = stop {
                    Some(error)
                } else {
                    None
                }
            }
        };
        let outcomes = match collected {
            Ok(rows)
                if matches!(sync, Some(Ok(()))) && cause.is_none() && diagnostics.is_empty() =>
            {
                return Poll::Ready(Ok(rows.into_array()));
            }
            Ok(rows) => rows.map_with([(); N], |row, ()| Outcome::Completed(row)),
            Err(outcomes) => outcomes.into_array(),
        };
        Poll::Ready(Err(GroupFailure {
            outcomes,
            diagnostics,
            sync,
            cause,
        }))
    }
}
