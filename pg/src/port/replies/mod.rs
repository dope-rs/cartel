use std::{cmp::Ordering, ptr::NonNull};

use cartel_core::reply::{self, order, retain, usage};
use dope::{
    core::driver::{result::SubmitCause, route::KeyTag, schedule::app},
    manifold::client::{close, conn},
};
use o3::{
    base::cell::region,
    buffer,
    buffer::{Residency as _, bytes::bounded, hold},
};
use usage::Build as _;

use crate::{
    api::{
        results::{group::Position, source},
        traits::sealed::Sealed,
    },
    port::{self, requests::Boundary},
    protocol::{
        self, Reason, frame,
        runtime::{ready, tx::poll},
    },
    reply::{Error, server},
    schema,
};

mod batch;
pub(crate) mod groups;

pub(crate) use batch::Batch;

pub(crate) type Response<'app, 'g, 'r, 's, 'b, 'c, 'd, X> = reply::recv::Response<
    'app,
    'b,
    'c,
    'd,
    Row<'d>,
    X,
    ReplyFailure<'d>,
    Tag<'g, 'r, 's, 'b, 'c, 'd>,
>;

pub(crate) struct Tag<'g, 'r, 's, 'b, 'c, 'd> {
    batch: Batch,
    /// Admission proves a Ready-completed order closes this exact batch.
    kind: Kind<NonNull<groups::State<'r, 's, 'b, 'c, 'd>>, groups::Owner<'g, 'r, 's, 'b, 'c, 'd>>,
}

pub(crate) enum Kind<G, S = G> {
    Command,
    Ready,
    Begin,
    Commit,
    Group(u32, G),
    Last(u32, G),
    Sync(S),
}

impl<G, S> Kind<G, S> {
    fn completion(&self) -> Completion {
        match self {
            Self::Command => Completion::Command,
            Self::Ready => Completion::Ready,
            Self::Begin => Completion::Begin,
            Self::Commit => Completion::Commit,
            Self::Group(_, _) => Completion::Group,
            Self::Last(_, _) => Completion::Last,
            Self::Sync(_) => Completion::Sync,
        }
    }
}

#[derive(Clone, Copy)]
#[repr(u8)]
pub(crate) enum Completion {
    Command,
    Group,
    /// The final command closes command progress, while its Sync stays ordered.
    Last,
    Ready,
    Begin,
    Commit,
    /// A group barrier owns no command and completes only at ReadyForQuery.
    Sync,
}

impl<'g, 'r, 's, 'b, 'c, 'd> Tag<'g, 'r, 's, 'b, 'c, 'd> {
    pub(crate) const fn new(batch: Batch, boundary: Boundary) -> Self {
        Self {
            batch,
            kind: match boundary {
                Boundary::Open => Kind::Command,
                Boundary::Close | Boundary::Group | Boundary::External => Kind::Ready,
                Boundary::Begin => Kind::Begin,
                Boundary::Commit => Kind::Commit,
            },
        }
    }

    pub(crate) const fn batch(&self) -> Batch {
        self.batch
    }

    pub(crate) fn record_error<'input, R: buffer::Cursor>(
        &self,
        skipping: bool,
        plan: crate::reply::Plan<'input, 'd, R>,
    ) -> Result<ErrorRoute<'input, 'd, R>, crate::Error> {
        let end = self.as_ref();
        let (group, command) = match end {
            Kind::Command | Kind::Ready | Kind::Begin | Kind::Commit => {
                return Ok(ErrorRoute::Single(plan));
            }
            Kind::Group(index, owner) | Kind::Last(index, owner) => {
                (owner, Position::Command(index as usize))
            }
            Kind::Sync(owner) => (owner, Position::Sync),
        };
        let scope = plan.scope();
        let class = plan.class();
        let position = match scope {
            crate::reply::Scope::Connection => Position::Connection,
            crate::reply::Scope::Command | crate::reply::Scope::Unknown if skipping => {
                Position::Recovery
            }
            crate::reply::Scope::Unknown => Position::Unknown,
            crate::reply::Scope::Command => command,
        };
        group.record(position, plan)?;
        Ok(ErrorRoute::Group {
            scope,
            class,
            command: matches!(command, Position::Command(_)),
        })
    }
}

pub(crate) fn try_group_by<'app, 'g, 'r, 's, 'b, 'c: 'b, 'd: 'c, X, Y, I, R, const N: usize>(
    token: &mut region::Token<region::Identity<'d>>,
    lane: reply::Lane<'app, 'b, 'c, 'd, Row<'d>, ReplyFailure<'d>, Tag<'g, 'r, 's, 'b, 'c, 'd>>,
    commands: &mut [Response<'app, 'g, 'r, 's, 'b, 'c, 'd, X>; N],
    terminal: &mut Response<'app, 'g, 'r, 's, 'b, 'c, 'd, Y>,
    batch: Batch,
    input: I,
    make: impl FnOnce(I) -> (groups::Owner<'g, 'r, 's, 'b, 'c, 'd>, R),
) -> Result<(order::Depth, R), (reply::recv::attach::Error, I)>
where
    X: reply::recv::Extract<'app, 'b, 'c, 'd, Row<'d>, ReplyFailure<'d>>,
    Y: reply::recv::Extract<'app, 'b, 'c, 'd, Row<'d>, ReplyFailure<'d>>,
{
    Response::try_group_by(token, lane, commands, terminal, input, |input| {
        let (owner, output) = make(input);
        let group = NonNull::from(&*owner);
        // The admitted FIFO publishes its owning Sync last. Only front access
        // can inspect a command; detachment never removes its ordered anchor.
        (
            [(); N],
            move |index, ()| Tag {
                batch,
                kind: if index + 1 == N {
                    Kind::Last(index as u32, group)
                } else {
                    Kind::Group(index as u32, group)
                },
            },
            Tag {
                batch,
                kind: Kind::Sync(owner),
            },
            output,
        )
    })
}

mod raw {
    use super::{Kind, Tag, groups};

    impl<'g, 'r, 's, 'b, 'c, 'd> Tag<'g, 'r, 's, 'b, 'c, 'd> {
        pub(super) fn as_ref(&self) -> Kind<&groups::State<'r, 's, 'b, 'c, 'd>> {
            match &self.kind {
                Kind::Command => Kind::Command,
                Kind::Ready => Kind::Ready,
                Kind::Begin => Kind::Begin,
                Kind::Commit => Kind::Commit,
                // SAFETY: only try_group_by constructs these command tags. Its
                // later Sync retains the stable state through this front borrow.
                Kind::Group(index, group) => Kind::Group(*index, unsafe { group.as_ref() }),
                // SAFETY: the same ordered Sync also outlives the final command.
                Kind::Last(index, group) => Kind::Last(*index, unsafe { group.as_ref() }),
                Kind::Sync(group) => Kind::Sync(group),
            }
        }
    }
}

const _: () = assert!(
    std::mem::size_of::<Tag<'static, 'static, 'static, 'static, 'static, 'static>>()
        == 3 * std::mem::size_of::<usize>()
);

#[derive(Clone, Copy)]
#[doc(hidden)]
#[repr(transparent)]
pub struct Failure(crate::Error);

impl Failure {
    pub(crate) const fn from_error(error: crate::Error) -> Self {
        Self(error)
    }

    pub(crate) const fn from_close(reason: close::Reason) -> Self {
        Self(crate::Error::Connection(reason))
    }

    pub(crate) fn close_reason(self) -> close::Reason {
        match self.0 {
            crate::Error::Connection(reason) => reason,
            crate::Error::Receive(_) => close::Reason::Transport,
            crate::Error::Transmit(conn::Transmit::Rejected(failure)) => {
                match failure.into_error().cause() {
                    SubmitCause::Full => close::Reason::Capacity(close::Capacity::Transport),
                    SubmitCause::IdentityExhausted => {
                        close::Reason::Capacity(close::Capacity::IdentityExhausted)
                    }
                }
            }
            crate::Error::Transmit(conn::Transmit::Io(_) | conn::Transmit::Invalid(_)) => {
                close::Reason::Transport
            }
            crate::Error::Closed => close::Reason::Local(close::LocalOrigin::ApiRequest),
            crate::Error::Timeout(kind) => close::Reason::Timeout(kind),
            crate::Error::Protocol(_)
            | crate::Error::Auth(_)
            | crate::Error::UnsupportedAuthMethod(_)
            | crate::Error::ServerFirst(_)
            | crate::Error::ServerFirstBase64(_)
            | crate::Error::ServerFinal(_)
            | crate::Error::ServerFinalBase64(_)
            | crate::Error::ScramIterations { .. }
            | crate::Error::Scram(_)
            | crate::Error::UnexpectedStartupMessage(_)
            | crate::Error::UnexpectedSaslMessage(_)
            | crate::Error::UnexpectedEagerPrepareMessage(_)
            | crate::Error::UnexpectedReadyMessage(_)
            | crate::Error::ReplyFront(_) => close::Reason::Protocol,
            crate::Error::CopyFinishWithoutStart
            | crate::Error::CopyFinishDuringPipeline { .. }
            | crate::Error::ReplyCapacity(_)
            | crate::Error::GroupDiagnosticRecords { .. }
            | crate::Error::GroupDiagnosticBytes(_) => {
                close::Reason::Capacity(close::Capacity::IngressRetention)
            }
            crate::Error::Skipped => close::Reason::Local(close::LocalOrigin::Superseded),
            _ => close::Reason::Local(close::LocalOrigin::ResourceFailure),
        }
    }

    pub(crate) const fn into_error(self) -> crate::Error {
        self.0
    }
}

#[doc(hidden)]
pub struct Row<'d>(hold::Retained<'d>);

impl Sealed for Row<'_> {}

impl source::Payload for Row<'_> {
    fn wire(&self) -> protocol::Wire<'_> {
        // SAFETY: RowPlan retains the admitted frame's unchanged bytes and u32 extent.
        protocol::Wire::from_raw(unsafe { bounded::raw::Input::new(self.0.as_slice()) })
    }
}

const _: () = assert!(size_of::<Row<'static>>() == size_of::<buffer::resident::Shared<'static>>());

#[doc(hidden)]
pub enum ReplyFailure<'d> {
    Server(server::Diagnostic<'d>),
    GroupServer,
    Failed(crate::Error),
}

struct GroupServer;

pub(crate) enum ErrorRoute<'input, 'd, R: buffer::Cursor> {
    Single(crate::reply::Plan<'input, 'd, R>),
    Group {
        scope: crate::reply::Scope,
        class: crate::reply::Class,
        command: bool,
    },
}

pub(crate) struct RowPlan<'input, 'd, R: buffer::Cursor> {
    payload: hold::scope::Deferred<'input, 'd, R>,
}

pub(crate) enum Pending<'d> {
    Row(Row<'d>),
    Failure(ReplyFailure<'d>),
}

impl<'input, 'd, R: buffer::Cursor> RowPlan<'input, 'd, R> {
    pub(crate) fn row(payload: frame::Payload<'input, 'd, R>) -> Self {
        Self {
            payload: payload.into_inner(),
        }
    }

    pub(crate) fn into_pending(self) -> Result<Pending<'d>, (Self, hold::Error)>
    where
        R: hold::raw::Retain<'d>,
    {
        self.try_build().map(Pending::Row)
    }
}

impl<'d, R: buffer::Cursor + hold::raw::Retain<'d>> usage::Build<Row<'d>> for RowPlan<'_, 'd, R> {
    type Error = hold::Error;

    fn usage(&self) -> usage::Bytes {
        usage::Bytes::new(self.payload.as_slice().len())
    }

    fn try_build(self) -> Result<Row<'d>, (Self, Self::Error)> {
        let payload = self.payload.bind(cartel_core::memory::RETENTION);
        match payload.try_retain() {
            Ok(owner) => Ok(Row(owner)),
            Err(error) => Err((
                Self {
                    payload: payload.defer(),
                },
                error,
            )),
        }
    }
}

#[doc(hidden)]
pub struct Payload<'b, 'c, 'd>(retain::Node<'b, 'c, 'd, Row<'d>>);

impl<'b, 'c, 'd> Payload<'b, 'c, 'd> {
    pub(crate) const fn from_node(node: retain::Node<'b, 'c, 'd, Row<'d>>) -> Self {
        Self(node)
    }
}

impl Sealed for Payload<'_, '_, '_> {}

impl source::Payload for Payload<'_, '_, '_> {
    fn wire(&self) -> protocol::Wire<'_> {
        self.0.get().wire()
    }
}

impl<'d> ReplyFailure<'d> {
    pub fn kind(&self) -> crate::reply::Kind<'_, 'd> {
        match self {
            Self::Server(error) => crate::reply::Kind::Server(error.fields()),
            Self::GroupServer => {
                crate::reply::Kind::Driver(&crate::Error::Protocol(Reason::GroupReply))
            }
            Self::Failed(error) => crate::reply::Kind::Driver(error),
        }
    }

    pub(crate) fn into_error<const ID: u8>(self) -> Error<'d, ID> {
        match self {
            Self::Server(error) => Error::Server(error),
            Self::GroupServer => crate::Error::Protocol(Reason::GroupReply).into(),
            Self::Failed(error) => error.into(),
        }
    }
}

impl usage::Footprint for Row<'_> {
    type Amount = usage::Bytes;

    fn footprint(&self) -> usage::Bytes {
        usage::Bytes::new(self.0.as_slice().len())
    }
}

impl usage::Footprint for ReplyFailure<'_> {
    type Amount = usage::Usage;

    fn footprint(&self) -> usage::Usage {
        match self {
            Self::Server(error) => usage::Usage::new(
                usage::Bytes::new(error.payload.resident()),
                usage::Values::new(1),
            ),
            Self::GroupServer | Self::Failed(_) => usage::Usage::ZERO,
        }
    }
}

impl From<&Failure> for ReplyFailure<'_> {
    fn from(cause: &Failure) -> Self {
        Self::Failed(cause.0)
    }
}

impl From<&GroupServer> for ReplyFailure<'_> {
    fn from(_: &GroupServer) -> Self {
        Self::GroupServer
    }
}

type Front<'turn, 'g, 'r, 's, 'b, 'c, 'd, R = Option<Completion>> = (
    R,
    reply::Push<'turn, 'b, 'c, 'd, Row<'d>, ReplyFailure<'d>, Tag<'g, 'r, 's, 'b, 'c, 'd>>,
);

pub(crate) struct Lane<'life, 'g, 'r, 's, 'b, 'c, 'd: 'life, const ID: u8> {
    entry: ready::Entry<'life, 'd, ID>,
    lane: reply::Lane<'life, 'b, 'c, 'd, Row<'d>, ReplyFailure<'d>, Tag<'g, 'r, 's, 'b, 'c, 'd>>,
    region: &'life mut region::Token<region::Identity<'d>>,
}

impl<'life, 'g, 'r, 's, 'b, 'c, 'd: 'life, const ID: u8> Lane<'life, 'g, 'r, 's, 'b, 'c, 'd, ID> {
    pub(super) fn try_new<I: schema::QuerySet>(
        port: &'life port::Port<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        conn: conn::Id<'d, KeyTag<ID>>,
        region: &'life mut region::Token<region::Identity<'d>>,
    ) -> Option<Self> {
        let entry = port.shared.connection_entry(conn)?;
        let lane = port.lane(conn.lane());
        Some(Self {
            entry,
            lane,
            region,
        })
    }

    pub(crate) fn set_receiving(&mut self, receiving: bool) {
        self.entry
            .set_receiving(receiving, || self.lane.depth(self.region));
    }

    pub(crate) fn depth(&mut self) -> order::Depth {
        self.lane.depth(self.region)
    }

    pub(crate) fn try_flush<E, I: schema::QuerySet>(
        self,
        port: &port::Port<'_, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        send: impl FnOnce(&mut region::Token<region::Identity<'d>>) -> Result<(), E>,
    ) -> Result<(), E> {
        self.entry
            .try_flush(port.request_queue.lane_for(self.lane), self.region, send)
    }

    pub(crate) fn try_push(&mut self, row: Row<'d>) -> Result<(), usage::InsertError<Row<'d>>> {
        self.lane.select(self.region).try_push(row)
    }

    pub(crate) fn try_fail(
        &mut self,
        failure: ReplyFailure<'d>,
    ) -> Result<(), usage::InsertError<ReplyFailure<'d>>> {
        let depth = self.lane.select(self.region).try_fail(failure)?;
        self.reindex(depth);
        Ok(())
    }

    pub(crate) fn complete(mut self) {
        let depth = self.lane.complete(self.region);
        self.reindex(depth);
    }

    pub(crate) fn reindex(&mut self, depth: order::Depth) {
        self.entry.reindex(depth);
    }

    pub(crate) fn settle(
        mut self,
        status: poll::Status,
        depth: order::Depth,
    ) -> Option<poll::Readiness> {
        self.entry.try_settle(status, depth)
    }

    pub(crate) fn fail_one(&mut self, cause: Failure) {
        let depth = self.lane.fail_one(self.region, cause);
        self.reindex(depth);
    }

    pub(crate) fn fail_group(&mut self) {
        let depth = self.lane.fail_one(self.region, GroupServer);
        self.reindex(depth);
    }

    pub(crate) fn fail(self, work: app::Work<'_, 'd>, cause: Failure) -> reply::Progress {
        reply::Retirement::new(self.region).fail(work, self.lane, &cause)
    }

    pub(crate) fn front<'turn>(
        &'turn mut self,
        expected: Batch,
    ) -> Result<Front<'turn, 'g, 'r, 's, 'b, 'c, 'd>, crate::Error>
    where
        'life: 'turn,
    {
        self.inspect(expected, |end, _| end.map(|kind| kind.completion()))
    }

    pub(crate) fn inspect<'turn, R>(
        &'turn mut self,
        expected: Batch,
        inspect: impl for<'tag> FnOnce(
            Option<Kind<&'tag groups::State<'r, 's, 'b, 'c, 'd>>>,
            order::Front,
        ) -> R,
    ) -> Result<Front<'turn, 'g, 'r, 's, 'b, 'c, 'd, R>, crate::Error>
    where
        'life: 'turn,
    {
        let mut selected = self.lane.select(self.region);
        let front = selected.front();
        let Some(tag) = selected.tag() else {
            return Ok((inspect(None, front), selected));
        };
        let end = match tag.batch.relative_to(expected) {
            Ordering::Greater => None,
            Ordering::Less => return Err(crate::Error::Protocol(Reason::ReadyUnknown)),
            Ordering::Equal => Some(tag.as_ref()),
        };
        Ok((inspect(end, front), selected))
    }
}

const _: () = assert!(std::mem::size_of::<Failure>() == std::mem::size_of::<crate::Error>());
