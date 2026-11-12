use dope::{
    core::driver::route::KeyTag,
    manifold::client::{app, conn},
};
use o3::base::cell::region;
use o3::base::time::Positive;
use std::num;

use crate::{
    port::replies,
    protocol::{runtime::ready, scram},
    schema::{self, desc},
};

pub struct State<'a, 'd, I, const ID: u8> {
    pub(super) phase: Phase<'a, 'd, I, ID>,
    pub(super) progress: Progress,
    pub(super) batch: replies::Batch,
}

impl<I, const ID: u8> Default for State<'_, '_, I, ID> {
    fn default() -> Self {
        Self {
            phase: Phase::NeedsStartup,
            progress: Progress::Running,
            batch: replies::Batch::default(),
        }
    }
}

#[derive(Clone, Copy, Default)]
#[repr(u8)]
pub(super) enum Progress {
    #[default]
    Running,
    Complete,
    Skipping,
    /// ReadyForQuery can resolve this unclassified diagnostic against the ordered command.
    /// Connection loss cannot establish its scope.
    Unknown,
}

pub(super) enum Prepare<I> {
    LtreeParse {
        next: schema::Step<I>,
    },
    LtreeParameters {
        next: schema::Step<I>,
    },
    LtreeResult {
        next: schema::Step<I>,
    },
    QueryParse {
        cursor: schema::Cursor<I>,
        registry: desc::Registry,
    },
    QueryParameters {
        cursor: schema::Cursor<I>,
        registry: desc::Registry,
    },
    QueryResult {
        cursor: schema::Cursor<I>,
        registry: desc::Registry,
    },
    Complete,
}

impl<I> Copy for Prepare<I> {}

impl<I> Clone for Prepare<I> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<I: schema::QuerySet> Prepare<I> {
    pub(super) fn new(catalog: schema::Catalog<I>) -> Self {
        let next = catalog.first();
        if I::PREPARE.flags() & desc::DYNAMIC_LTREE == 0 {
            Self::resume(next, desc::Registry::default())
        } else {
            Self::LtreeParse { next }
        }
    }

    pub(super) fn register(next: schema::Step<I>, oid: num::NonZero<u32>) -> Self {
        Self::resume(next, desc::Registry::default().with_ltree(oid))
    }

    pub(super) fn resume(next: schema::Step<I>, registry: desc::Registry) -> Self {
        match next {
            schema::Step::Query(cursor) => Self::QueryParse { cursor, registry },
            schema::Step::Complete => Self::Complete,
        }
    }
}

const _: () = assert!(std::mem::size_of::<Progress>() == std::mem::size_of::<bool>());
const _: () = assert!(std::mem::align_of::<Progress>() == std::mem::align_of::<bool>());

impl<'a, 'd, I, const ID: u8> State<'a, 'd, I, ID> {
    pub(super) fn entry(&self) -> Option<ready::Entry<'a, 'd, ID>> {
        match self.phase {
            Phase::Ready(entry)
            | Phase::CopyInActive(entry)
            | Phase::CopyOutActive(entry)
            | Phase::Retiring(entry) => Some(entry),
            Phase::NeedsStartup
            | Phase::StartupSent
            | Phase::Sasl { .. }
            | Phase::AwaitingReady
            | Phase::Preparing { .. }
            | Phase::Failed => None,
        }
    }

    pub(super) fn cancel(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
    ) -> Option<conn::Id<'d, KeyTag<ID>>> {
        match &self.phase {
            Phase::Sasl { scram } => scram.ticket.cancel(token),
            Phase::NeedsStartup
            | Phase::StartupSent
            | Phase::AwaitingReady
            | Phase::Preparing { .. }
            | Phase::Ready(_)
            | Phase::CopyInActive(_)
            | Phase::CopyOutActive(_)
            | Phase::Retiring(_)
            | Phase::Failed => None,
        }
    }

    pub(super) fn inbound(&self, load: Option<usize>, idle: Positive) -> app::Inbound {
        match &self.phase {
            Phase::StartupSent
            | Phase::Sasl { .. }
            | Phase::AwaitingReady
            | Phase::Preparing { .. } => app::Inbound::Awaiting(idle),
            Phase::NeedsStartup
            | Phase::Ready(_)
            | Phase::CopyInActive(_)
            | Phase::CopyOutActive(_)
            | Phase::Retiring(_)
            | Phase::Failed => match load {
                Some(0) => app::Inbound::Quiescent,
                None | Some(1..) => app::Inbound::Awaiting(idle),
            },
        }
    }
}

pub(super) enum Phase<'a, 'd, I, const ID: u8> {
    NeedsStartup,
    StartupSent,
    Sasl { scram: scram::Scram<'a, 'd, ID> },
    AwaitingReady,
    Preparing { remaining: Prepare<I> },
    Ready(ready::Entry<'a, 'd, ID>),
    CopyInActive(ready::Entry<'a, 'd, ID>),
    CopyOutActive(ready::Entry<'a, 'd, ID>),
    Retiring(ready::Entry<'a, 'd, ID>),
    Failed,
}

const _: () = assert!(std::mem::size_of::<Prepare<()>>() <= 16);
