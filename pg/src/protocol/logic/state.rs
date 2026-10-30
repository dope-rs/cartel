use dope::{
    core::driver::route::KeyTag,
    manifold::client::{app, conn},
};
use o3::base::cell::region;
use o3::base::time::Positive;
use std::num;

use crate::{
    port::replies,
    protocol::{runtime::tx::poll, scram},
    schema::{self, desc},
};

pub struct State<'a, 'd, I, const ID: u8> {
    pub(super) phase: Phase<'a, 'd, I, ID>,
    pub(super) skip: Skip,
    pub(super) completed: bool,
    pub(super) batch: replies::Batch,
}

impl<I, const ID: u8> Default for State<'_, '_, I, ID> {
    fn default() -> Self {
        Self {
            phase: Phase::NeedsStartup,
            skip: Skip::None,
            completed: false,
            batch: replies::Batch::default(),
        }
    }
}

#[derive(Clone, Copy, Default)]
#[repr(u8)]
pub(super) enum Skip {
    #[default]
    None,
    Pending,
    /// ReadyForQuery can resolve this unclassified diagnostic against the ordered command.
    /// Connection loss cannot establish its scope.
    Unknown,
}

#[derive(Clone, Copy)]
pub(super) struct Drain {
    pub(super) skip: Skip,
    pub(super) completed: bool,
    pub(super) status: poll::Status,
    pub(super) batch: replies::Batch,
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

const _: () = assert!(std::mem::size_of::<Skip>() == std::mem::size_of::<bool>());
const _: () = assert!(std::mem::align_of::<Skip>() == std::mem::align_of::<bool>());
const _: () = assert!(
    std::mem::size_of::<Drain>()
        == std::mem::size_of::<replies::Batch>() + std::mem::align_of::<replies::Batch>()
);

impl<'d, I, const ID: u8> State<'_, 'd, I, ID> {
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
            | Phase::Ready
            | Phase::CopyInActive
            | Phase::CopyOutActive
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
            | Phase::Ready
            | Phase::CopyInActive
            | Phase::CopyOutActive
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
    Ready,
    CopyInActive,
    CopyOutActive,
    Failed,
}

const _: () = assert!(std::mem::size_of::<Prepare<()>>() <= 16);
