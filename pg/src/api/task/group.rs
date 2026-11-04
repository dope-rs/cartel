use std::{array, mem::MaybeUninit, ops::ControlFlow, pin::Pin, task::Poll};

use cartel_core::reply;
use dope::{
    core::driver::{route::KeyTag, schedule::clock::timer},
    manifold::client::conn,
};
use dope_fiber::{abi, context, wait};
use o3::{base::cell::region, depot::array::init};
use reply::{
    Slot, order,
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
            GroupFailure,
            group::{Outcome, Sync},
        },
    },
    port::replies::{
        self,
        groups::{self, Owner},
    },
    port::requests,
    reply::{Error, group::Ledger},
    schema,
};

use crate::api::task::{Dispatch, State, lease};

type Output<'r, 's, 'b, 'c, 'd, const ID: u8, const N: usize> =
    GroupFailure<Option<replies::Payload<'b, 'c, 'd>>, Error<'d, ID>, Ledger<'r, 's>, N>;

struct First<const ID: u8>;

impl<'app, 'b, 'c: 'b, 'd: 'c, const ID: u8>
    recv::Extract<'app, 'b, 'c, 'd, replies::Row<'d>, replies::ReplyFailure<'d>> for First<ID>
{
    type Output = Outcome<Option<replies::Payload<'b, 'c, 'd>>, Error<'d, ID>>;
    type Context = ();
    const ACCEPTANCE: recv::Acceptance = recv::Acceptance::Collected;

    fn extract(
        _: &(),
        slot: &mut Slot<'_, 'app, 'b, 'c, 'd, replies::Row<'d>, replies::ReplyFailure<'d>>,
    ) -> Option<Self::Output> {
        Some(match slot.terminal()? {
            Terminal::Complete(mut done) => match done.try_lease() {
                Ok(payload) => Outcome::Completed(payload.map(replies::Payload::from_lease)),
                Err(error) => Outcome::Failed(crate::Error::ResponseRetain(error).into()),
            },
            Terminal::Failed(failure) => match failure.into_error() {
                replies::ReplyFailure::Server(error) => Outcome::Failed(Error::Server(error)),
                replies::ReplyFailure::GroupServer => Outcome::Server,
                replies::ReplyFailure::Failed(crate::Error::Skipped) => Outcome::Skipped,
                replies::ReplyFailure::Failed(error) => Outcome::Interrupted(error.into()),
            },
        })
    }
}

type Admit<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, const ID: u8, const N: usize> = Dispatch<
    'app,
    'g,
    'r,
    's,
    'b,
    'c,
    'd,
    B,
    I,
    ID,
    Ready<'g, 'r, 's, 'b, 'c, 'd, N>,
    conn::Id<'d, KeyTag<ID>>,
    lease::Detached,
    groups::Owner<'g, 'r, 's, 'b, 'c, 'd>,
>;

enum Stop<'d, const ID: u8> {
    Deadline(crate::Error),
    Admission(Error<'d, ID>),
    Sync,
}

struct Completed<'r, 's, 'd, const ID: u8> {
    diagnostics: Ledger<'r, 's>,
    sync: Sync,
    sync_error: Option<Error<'d, ID>>,
    cause: Option<Error<'d, ID>>,
}

fn collect<'app, 'g, 'r, 's, 'b, 'c: 'b, 'd: 'c, const ID: u8, const N: usize>(
    commands: &mut [Response<'app, 'g, 'r, 's, 'b, 'c, 'd, First<ID>>; N],
    token: &mut region::Token<region::Identity<'d>>,
) -> [Outcome<Option<replies::Payload<'b, 'c, 'd>>, Error<'d, ID>>; N] {
    commands
        .each_mut()
        .map(|command| match command.try_take(token) {
            Some(outcome) => outcome,
            None => {
                command.detach();
                Outcome::Unobserved
            }
        })
}

fn poll_deadline<
    'app,
    'g,
    'r,
    's,
    'b,
    'c,
    'd: 'app,
    B: ?Sized,
    I: schema::QuerySet + 'd,
    const ID: u8,
    const N: usize,
>(
    admit: &mut Admit<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID, N>,
    guard: Pin<&mut Result<timer::Alarm<'d>, timer::Exhausted>>,
    cx: Pin<&mut context::Context<'_, 'd>>,
) -> Option<Stop<'d, ID>> {
    let guard = match guard.get_mut() {
        Ok(guard) => Pin::new(guard),
        Err(error) => return Some(Stop::Deadline(crate::Error::Deadline(*error))),
    };
    let phase = admit.phase();
    match admit.deadline.poll_expired(guard, cx, phase) {
        Poll::Ready(time) => Some(Stop::Deadline(crate::Error::RequestTimeout(time))),
        Poll::Pending => None,
    }
}

/// Snapshots before detachment and cancellation can retire another order.
fn complete<
    'app,
    'g,
    'r,
    's,
    'b,
    'c: 'b,
    'd: 'c + 'app,
    B: ?Sized,
    I: schema::QuerySet + 'd,
    const ID: u8,
    const N: usize,
>(
    admit: &mut Admit<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID, N>,
    reply: &mut Response<'app, 'g, 'r, 's, 'b, 'c, 'd, api::ExtractUnit<ID>>,
    guard: Pin<&mut Result<timer::Alarm<'d>, timer::Exhausted>>,
    waiter: Pin<&wait::Waiter<'app, 'd>>,
    stop: Stop<'d, ID>,
    sync: Option<Result<(), Error<'d, ID>>>,
) -> Completed<'r, 's, 'd, ID> {
    waiter.unregister();
    let (mut cause, abort) = match stop {
        Stop::Deadline(error) => (None, Some(error)),
        Stop::Admission(error) => (Some(error), None),
        Stop::Sync => (None, None),
    };
    let (sync, failure) = match sync {
        Some(Ok(())) => (Sync::Complete, None),
        Some(Err(error)) => (Sync::Unobserved, Some(error)),
        None => (Sync::Unobserved, None),
    };
    let ledger = match &admit.state {
        State::Admitted(_, owner) => owner.snapshot(),
        _ => Ledger::empty(),
    };
    reply.detach();
    match abort {
        Some(error) => cause = Some(admit.cancel(error)),
        None => {
            admit.state.take();
            if let Ok(guard) = guard.get_mut() {
                guard.clear();
            }
        }
    }
    Completed {
        diagnostics: ledger,
        sync,
        sync_error: failure,
        cause,
    }
}

struct Replies<'app, 'g, 'r, 's, 'b, 'c: 'b, 'd: 'c, const ID: u8, const N: usize> {
    commands: [Response<'app, 'g, 'r, 's, 'b, 'c, 'd, First<ID>>; N],
    sync: Response<'app, 'g, 'r, 's, 'b, 'c, 'd, api::ExtractUnit<ID>>,
}

impl<'app, 'g, 'r, 's, 'b, 'c: 'b, 'd: 'c, const ID: u8, const N: usize>
    Replies<'app, 'g, 'r, 's, 'b, 'c, 'd, ID, N>
{
    fn new() -> Self {
        const { assert!(N > 0 && N < u32::MAX as usize) };
        Self {
            commands: array::from_fn(|_| Response::new()),
            sync: Response::new(),
        }
    }
}

/// Unpublished diagnostic owners retained by the pending request until admission.
pub(in crate::api) struct Ready<'g, 'r, 's, 'b, 'c, 'd, const N: usize> {
    collector: groups::Draft<'g, 'r, 's, 'b, 'c, 'd>,
    commands: [groups::Owner<'g, 'r, 's, 'b, 'c, 'd>; N],
    sync: groups::Owner<'g, 'r, 's, 'b, 'c, 'd>,
}

impl<'g, 'r, 's, 'b, 'c, 'd, const N: usize> Ready<'g, 'r, 's, 'b, 'c, 'd, N> {
    pub(in crate::api) fn new(
        access: &groups::Access<'g, 'r, 's, 'b, 'c, 'd>,
    ) -> Result<Self, crate::Error> {
        const { assert!(N > 0 && N < u32::MAX as usize) };
        let owner = access.prepare()?;
        let mut storage = MaybeUninit::uninit();
        let mut guard = init::Guard::<_, N>::new(&mut storage);
        let commands = loop {
            match guard.next() {
                ControlFlow::Break(full) => break full.into_array(),
                ControlFlow::Continue(vacant) => {
                    let item = owner.try_clone().map_err(crate::Error::ResponseRetain)?;
                    guard = vacant.insert(item);
                }
            }
        };
        let sync = owner.try_clone().map_err(crate::Error::ResponseRetain)?;
        Ok(Self {
            collector: owner,
            commands,
            sync,
        })
    }
}

impl<'g, 'r, 's, 'b, 'c: 'b, 'd: 'c, const N: usize> Ready<'g, 'r, 's, 'b, 'c, 'd, N> {
    fn try_attach<'app, const ID: u8>(
        self,
        replies: &mut Replies<'app, 'g, 'r, 's, 'b, 'c, 'd, ID, N>,
        token: &mut region::Token<region::Identity<'d>>,
        lane: reply::Lane<
            'app,
            'b,
            'c,
            'd,
            replies::Row<'d>,
            replies::ReplyFailure<'d>,
            replies::Tag<'g, 'r, 's, 'b, 'c, 'd>,
        >,
        tag: replies::Tag<'g, 'r, 's, 'b, 'c, 'd>,
    ) -> Result<(order::Depth, Owner<'g, 'r, 's, 'b, 'c, 'd>), (attach::Error, Self)> {
        let reserved = match lane.try_reserve(self.collector.usage()) {
            Ok(reserved) => reserved,
            Err(error) => return Err((attach::Error::Payload(error), self)),
        };
        Response::try_group_by(
            token,
            lane,
            &mut replies.commands,
            &mut replies.sync,
            (self, reserved),
            |(ready, reserved)| {
                let Self {
                    collector: draft,
                    commands,
                    sync,
                } = ready;
                let owner = draft.admit(reserved);
                let mut index = 0u32;
                let commands = commands.map(|owner| {
                    let position = index;
                    index += 1;
                    replies::Tag::command(tag.batch(), position, index as usize == N, owner)
                });
                (commands, replies::Tag::sync(tag.batch(), sync), owner)
            },
        )
        .map_err(|(error, (ready, _))| (error, ready))
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
    const ID: u8,
    const N: usize,
> {
    admission: Admit<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID, N>,
    reply: Replies<'app, 'g, 'r, 's, 'b, 'c, 'd, ID, N>,
    #[pin]
    deadline: Result<timer::Alarm<'d>, timer::Exhausted>,
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
    const ID: u8,
    const N: usize,
> Group<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID, N>
{
    pub(in crate::api) fn from_outcome(
        client: api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
        target: Option<api::Target<'d, ID>>,
        next: api::Outcome<'d, ID, Ready<'g, 'r, 's, 'b, 'c, 'd, N>>,
    ) -> Self {
        let admit = Dispatch::new(client, target, next);
        let deadline = admit.deadline.guard();
        Self {
            admission: admit,
            reply: Replies::new(),
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
    const ID: u8,
    const N: usize,
> abi::Fiber<'d> for Group<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID, N>
{
    type Output = Output<'r, 's, 'b, 'c, 'd, ID, N>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, mut cx) = call.into_parts();
        let mut this = this.project();
        let mut sync = None;
        let stop = match poll_deadline(this.admission, this.deadline.as_mut(), cx.as_mut()) {
            Some(stop) => stop,
            None => {
                match this.admission.poll_with(
                    cx.as_mut(),
                    this.waiter.as_ref(),
                    |admit, client, conn, request, region| {
                        let api::Pending {
                            frame,
                            boundary,
                            state: ready,
                        } = request;
                        let context = requests::ReplyContext::new(conn, boundary, region);
                        let prepared =
                            match client.port.request().prepare(context, frame.as_ref().len()) {
                                Ok(prepared) => prepared,
                                Err(error) => {
                                    return Err((
                                        error,
                                        api::Pending {
                                            frame,
                                            boundary,
                                            state: ready,
                                        },
                                    ));
                                }
                            };
                        prepared
                            .try_with(
                                (frame, ready),
                                |token, lane, tag, (frame, ready)| match ready
                                    .try_attach(this.reply, token, lane, tag)
                                {
                                    Ok((depth, owner)) => Ok((depth, (frame, owner))),
                                    Err((error, ready)) => Err((error, (frame, ready))),
                                },
                                |(frame, owner)| {
                                    admit.state = State::Admitted(
                                        lease::Lease::admit((), client, conn),
                                        owner,
                                    );
                                    (frame, ())
                                },
                            )
                            .map_err(|(error, (frame, ready))| {
                                (
                                    error,
                                    api::Pending {
                                        frame,
                                        boundary,
                                        state: ready,
                                    },
                                )
                            })
                    },
                ) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => Stop::Admission(error),
                    Poll::Ready(Ok(())) => {
                        let Poll::Ready(observed) =
                            Pin::new(&mut this.reply.sync).poll_reply(cx.as_mut())
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
        let sync = sync.or_else(|| this.reply.sync.try_take(token));
        let outcomes = collect(&mut this.reply.commands, token);
        let done = complete(
            this.admission,
            &mut this.reply.sync,
            this.deadline,
            this.waiter.as_ref(),
            stop,
            sync,
        );
        Poll::Ready(GroupFailure {
            outcomes,
            diagnostics: done.diagnostics,
            sync: done.sync,
            sync_error: done.sync_error,
            cause: done.cause,
        })
    }
}
