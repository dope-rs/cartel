use std::cmp::Ordering;

use cartel_core::reply::{self, order, retain, usage};
use dope::{
    core::driver::{result::SubmitCause, route::KeyTag, schedule::app},
    manifold::client::{close, conn},
};
use o3::{
    base::cell::region,
    buffer,
    buffer::{Residency as _, hold},
};
use usage::Build as _;

use crate::{
    api::{
        results::{group::Position, source},
        traits::sealed::Sealed,
    },
    port::{self, requests::Boundary},
    protocol::{self, Reason, frame},
    reply::{Error, server},
    schema,
};

mod batch;
pub(crate) mod groups;

pub(crate) use batch::Batch;

pub(crate) struct Tag<'g, 'r, 's, 'b, 'c, 'd> {
    batch: Batch,
    /// Admission proves a Ready-completed order closes this exact batch.
    kind: Kind<'g, 'r, 's, 'b, 'c, 'd>,
}

enum Kind<'g, 'r, 's, 'b, 'c, 'd> {
    Command,
    Ready,
    Commit,
    Group(u32, groups::Owner<'g, 'r, 's, 'b, 'c, 'd>),
    Last(u32, groups::Owner<'g, 'r, 's, 'b, 'c, 'd>),
    Sync(groups::Owner<'g, 'r, 's, 'b, 'c, 'd>),
}

/// A borrowed completion carries the group owner selected in the same turn.
pub(crate) enum End<'a, 'g, 'r, 's, 'b, 'c, 'd> {
    Command,
    Ready,
    Commit,
    Group(u32, &'a groups::Owner<'g, 'r, 's, 'b, 'c, 'd>),
    Last(u32, &'a groups::Owner<'g, 'r, 's, 'b, 'c, 'd>),
    Sync(&'a groups::Owner<'g, 'r, 's, 'b, 'c, 'd>),
}

impl End<'_, '_, '_, '_, '_, '_, '_> {
    fn completion(&self) -> Completion {
        match self {
            Self::Command => Completion::Command,
            Self::Ready => Completion::Ready,
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
                Boundary::Commit => Kind::Commit,
            },
        }
    }

    pub(crate) const fn command(
        batch: Batch,
        index: u32,
        last: bool,
        group: groups::Owner<'g, 'r, 's, 'b, 'c, 'd>,
    ) -> Self {
        Self {
            batch,
            kind: if last {
                Kind::Last(index, group)
            } else {
                Kind::Group(index, group)
            },
        }
    }

    pub(crate) const fn sync(batch: Batch, group: groups::Owner<'g, 'r, 's, 'b, 'c, 'd>) -> Self {
        Self {
            batch,
            kind: Kind::Sync(group),
        }
    }

    pub(crate) const fn batch(&self) -> Batch {
        self.batch
    }

    fn end(&self) -> End<'_, 'g, 'r, 's, 'b, 'c, 'd> {
        match &self.kind {
            Kind::Command => End::Command,
            Kind::Ready => End::Ready,
            Kind::Commit => End::Commit,
            Kind::Group(index, owner) => End::Group(*index, owner),
            Kind::Last(index, owner) => End::Last(*index, owner),
            Kind::Sync(owner) => End::Sync(owner),
        }
    }

    pub(crate) fn record_error<'input, R: buffer::Cursor>(
        &self,
        skipping: bool,
        plan: crate::reply::Plan<'input, 'd, R>,
    ) -> Result<ErrorRoute<'input, 'd, R>, crate::Error> {
        let end = self.end();
        let complete = end.completion();
        let (group, command) = match end {
            End::Command | End::Ready | End::Commit => return Ok(ErrorRoute::Single(plan)),
            End::Group(index, owner) | End::Last(index, owner) => {
                (owner, Position::Command(index as usize))
            }
            End::Sync(owner) => (owner, Position::Sync),
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
            completion: complete,
        })
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
pub struct Row<'d>(frame::Retained<'d>);

impl Sealed for Row<'_> {}

impl source::Payload for Row<'_> {
    fn wire(&self) -> protocol::Wire<'_> {
        self.0.wire()
    }
}

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
        completion: Completion,
    },
}

fn row_usage(bytes: usize) -> usage::Usage {
    usage::Usage::new(usage::Bytes::new(bytes), usage::Values::new(1))
}

pub(crate) struct RowPlan<'input, 'd, R: buffer::Cursor> {
    payload: frame::Payload<'input, 'd, R>,
    policy: hold::Policy,
}

pub(crate) enum Pending<'d> {
    Row(Row<'d>),
    Failure(ReplyFailure<'d>),
}

impl<'input, 'd, R: buffer::Cursor> RowPlan<'input, 'd, R> {
    pub(crate) const fn row(payload: frame::Payload<'input, 'd, R>, policy: hold::Policy) -> Self {
        Self { payload, policy }
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

    fn usage(&self) -> usage::Usage {
        row_usage(self.payload.as_slice().len())
    }

    fn try_build(self) -> Result<Row<'d>, (Self, Self::Error)> {
        let Self { payload, policy } = self;
        match payload.try_retain(policy) {
            Ok(owner) => Ok(Row(owner)),
            Err((payload, error)) => Err((Self { payload, policy }, error)),
        }
    }
}

#[doc(hidden)]
pub struct Payload<'b, 'c, 'd>(retain::Lease<'b, 'c, 'd, Row<'d>>);

impl<'b, 'c, 'd> Payload<'b, 'c, 'd> {
    pub(crate) const fn from_lease(lease: retain::Lease<'b, 'c, 'd, Row<'d>>) -> Self {
        Self(lease)
    }
}

impl Sealed for Payload<'_, '_, '_> {}

impl source::Payload for Payload<'_, '_, '_> {
    fn wire(&self) -> protocol::Wire<'_> {
        self.0.get().0.wire()
    }
}

impl<'d> ReplyFailure<'d> {
    pub(crate) fn into_error<const ID: u8>(self) -> Error<'d, ID> {
        match self {
            Self::Server(error) => Error::Server(error),
            Self::GroupServer => crate::Error::Protocol(Reason::GroupReply).into(),
            Self::Failed(error) => error.into(),
        }
    }
}

impl usage::Footprint for Row<'_> {
    fn footprint(&self) -> usage::Usage {
        row_usage(self.0.len())
    }
}

impl usage::Footprint for ReplyFailure<'_> {
    fn footprint(&self) -> usage::Usage {
        match self {
            Self::Server(error) => row_usage(error.payload.resident()),
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

type Front<'turn, 'g, 'r, 's, 'b, 'c, 'd> = (
    Option<Completion>,
    reply::Push<'turn, 'b, 'c, 'd, Row<'d>, ReplyFailure<'d>, Tag<'g, 'r, 's, 'b, 'c, 'd>>,
);

pub(crate) struct Lane<'life, 'g, 'r, 's, 'b, 'c, 'd: 'life, I: schema::QuerySet, const ID: u8> {
    port: &'life port::Port<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
    conn: conn::Id<'d, KeyTag<ID>>,
    lane: reply::Lane<'life, 'b, 'c, 'd, Row<'d>, ReplyFailure<'d>, Tag<'g, 'r, 's, 'b, 'c, 'd>>,
}

impl<I: schema::QuerySet, const ID: u8> Copy for Lane<'_, '_, '_, '_, '_, '_, '_, I, ID> {}

impl<I: schema::QuerySet, const ID: u8> Clone for Lane<'_, '_, '_, '_, '_, '_, '_, I, ID> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'life, 'g, 'r, 's, 'b, 'c, 'd: 'life, I: schema::QuerySet, const ID: u8>
    Lane<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>
{
    pub(super) const fn new(
        port: &'life port::Port<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        conn: conn::Id<'d, KeyTag<ID>>,
        lane: reply::Lane<
            'life,
            'b,
            'c,
            'd,
            Row<'d>,
            ReplyFailure<'d>,
            Tag<'g, 'r, 's, 'b, 'c, 'd>,
        >,
    ) -> Self {
        Self { port, conn, lane }
    }

    pub(crate) fn attachment(
        self,
        token: &mut region::Token<region::Identity<'d>>,
    ) -> reply::Attachment {
        self.lane.control().attachment(token)
    }

    pub(crate) fn depth(self, token: &mut region::Token<region::Identity<'d>>) -> order::Depth {
        self.lane.depth(token)
    }

    pub(crate) fn try_push(
        self,
        token: &mut region::Token<region::Identity<'d>>,
        row: Row<'d>,
    ) -> Result<(), usage::InsertError<Row<'d>>> {
        self.lane.select(token).try_push(row)
    }

    pub(crate) fn try_fail(
        self,
        token: &mut region::Token<region::Identity<'d>>,
        failure: ReplyFailure<'d>,
    ) -> Result<(), usage::InsertError<ReplyFailure<'d>>> {
        let depth = self.lane.select(token).try_fail(failure)?;
        self.port.shared.reindex(self.conn, depth);
        Ok(())
    }

    pub(crate) fn complete(self, token: &mut region::Token<region::Identity<'d>>) {
        let depth = self.lane.complete(token);
        self.port.shared.reindex(self.conn, depth);
    }

    pub(crate) fn fail_one(self, token: &mut region::Token<region::Identity<'d>>, cause: Failure) {
        let depth = self.lane.fail_one(token, cause);
        self.port.shared.reindex(self.conn, depth);
    }

    pub(crate) fn fail_group(self, token: &mut region::Token<region::Identity<'d>>) {
        let depth = self.lane.fail_one(token, GroupServer);
        self.port.shared.reindex(self.conn, depth);
    }

    pub(crate) fn fail(
        self,
        work: app::Work<'_, 'd>,
        token: &mut region::Token<region::Identity<'d>>,
        cause: Failure,
    ) -> reply::Progress {
        reply::Retirement::new(token).fail(work, self.lane, &cause)
    }

    pub(crate) fn front<'turn>(
        self,
        expected: Batch,
        token: &'turn mut region::Token<region::Identity<'d>>,
    ) -> Result<Front<'turn, 'g, 'r, 's, 'b, 'c, 'd>, crate::Error>
    where
        'life: 'turn,
    {
        let mut selected = self.lane.select(token);
        let Some(tag) = selected.tag() else {
            return Ok((None, selected));
        };
        let complete = match tag.batch.relative_to(expected) {
            Ordering::Greater => None,
            Ordering::Less => {
                return Err(crate::Error::Protocol(Reason::ReadyUnknown));
            }
            Ordering::Equal => Some(tag.end().completion()),
        };
        Ok((complete, selected))
    }

    pub(crate) fn inspect<R>(
        self,
        expected: Batch,
        token: &mut region::Token<region::Identity<'d>>,
        inspect: impl for<'tag> FnOnce(Option<End<'tag, 'g, 'r, 's, 'b, 'c, 'd>>, order::Front) -> R,
    ) -> Result<R, crate::Error> {
        let mut selected = self.lane.select(token);
        let front = selected.front();
        let Some(tag) = selected.tag() else {
            return Ok(inspect(None, front));
        };
        let end = match tag.batch.relative_to(expected) {
            Ordering::Greater => None,
            Ordering::Less => return Err(crate::Error::Protocol(Reason::ReadyUnknown)),
            Ordering::Equal => Some(tag.end()),
        };
        Ok(inspect(end, front))
    }
}

const _: () = assert!(std::mem::size_of::<Failure>() == std::mem::size_of::<crate::Error>());
