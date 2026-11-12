use crate::{
    port::pool::slot,
    protocol::runtime::{Shared, ready::storage},
};
use cartel_core::reply::order::Depth;
use o3::depot;
use o3::depot::index::bitmap::entry;
use o3::depot::{index::bitmap, queue::links};
use std::num::NonZero;

#[derive(Clone, Copy)]
pub(crate) struct Entry<'a, 'd, const ID: u8> {
    pub(super) core: &'a Shared<'d, ID>,
    pub(super) index: NonZero<u32>,
}

/// The exact entry and bucket retained while its linked transition is committed.
pub(crate) struct Listed<'a, 'd, const ID: u8> {
    pub(super) core: &'a Shared<'d, ID>,
    pub(super) index: NonZero<u32>,
    pub(super) rank: storage::Rank,
}

impl<'a, 'd, const ID: u8> Entry<'a, 'd, ID> {
    pub(crate) fn slot(&self) -> &'a slot::Slot<'d, ID> {
        unsafe {
            self.core
                .index
                .allocation
                .values()
                .get_unchecked(self.index.get() as usize - 1)
        }
    }

    pub(in crate::port::runtime::ready) fn listed(self) -> Option<Listed<'a, 'd, ID>> {
        let rank = self.node().priority.get()?;
        Some(Listed {
            core: self.core,
            index: self.index,
            rank,
        })
    }

    pub(super) fn insert(self, depth: usize) {
        let rank = storage::Rank::new(entry::Row::clamp(&self.core.index.allocation, depth));
        self.insert_rank(rank);
    }

    pub(super) fn insert_rank(self, rank: storage::Rank) {
        let round = storage::Round(&self.core.index.allocation);
        // SAFETY: the completed slot transition admits this exact previously unlisted connection.
        unsafe {
            depot::raw::Hooks::cycle_push(
                &round,
                &self.core.index.ring,
                links::Index::from(self.index),
            )
        };
        self.attach(rank.row(&self.core.index.allocation));
    }

    pub(super) fn reprioritize(&mut self, depth: Depth) {
        let entry = Self {
            core: self.core,
            index: self.index,
        };
        let Some(listed) = entry.listed() else { return };
        listed.reprioritize(depth.get() + self.slot().barriers());
    }

    pub(super) fn node(&self) -> &'a storage::Node {
        unsafe {
            self.core
                .index
                .allocation
                .nodes()
                .get_unchecked(self.index.get() as usize - 1)
        }
    }

    fn attach(&self, bucket: entry::Row<&storage::Block<'d, ID>>) {
        use depot::raw::Hooks as _;

        let rank = storage::Rank::new(bucket);
        let list = unsafe { bucket.source().heads().get_unchecked(bucket.index()) };
        if list.is_empty() {
            bucket.update::<true>(());
            if self
                .core
                .index
                .minimum
                .get()
                .is_none_or(|current| rank < current)
            {
                self.core.index.minimum.set(Some(rank));
            }
        }
        let mut domain = storage::Buckets(&self.core.index.allocation);
        // SAFETY: this completed transition owns the vacant hook and this exact destination.
        unsafe { domain.link_raw(list, links::Index::from(self.index)) };
        self.node().priority.set(Some(rank));
    }

    /// Called only with the rank consumed from this entry's private Listed witness.
    fn detach(self, rank: storage::Rank) -> Self {
        use bitmap::Access as _;
        use depot::raw::Hooks as _;

        let bucket = rank.row(&self.core.index.allocation);
        let list = unsafe { bucket.source().heads().get_unchecked(bucket.index()) };
        let mut domain = storage::Buckets(&self.core.index.allocation);
        // SAFETY: the consumed Listed witness owns this exact entry and bucket.
        unsafe { domain.unlink_raw(list, links::Index::from(self.index)) };
        self.node().priority.set(None);
        if list.is_empty() {
            bucket.update::<false>(());
            if self.core.index.minimum.get() == Some(rank) {
                let next = (&self.core.index.allocation).find((), bucket.index() + 1);
                self.core.index.minimum.set(next.map(storage::Rank::new));
            }
        }
        self
    }
}

impl<'a, 'd, const ID: u8> Listed<'a, 'd, ID> {
    pub(crate) fn slot(&self) -> &'a slot::Slot<'d, ID> {
        Entry {
            core: self.core,
            index: self.index,
        }
        .slot()
    }

    pub(super) fn remove(self) -> (Entry<'a, 'd, ID>, storage::Rank) {
        let Self { core, index, rank } = self;
        let round = storage::Round(&core.index.allocation);
        // SAFETY: this capability owns the original listed entry until removal.
        unsafe {
            depot::raw::Hooks::cycle_unlink(&round, &core.index.ring, links::Index::from(index))
        };
        (Entry { core, index }.detach(rank), rank)
    }

    pub(crate) fn reprioritize(self, depth: usize) {
        let bucket = entry::Row::clamp(&self.core.index.allocation, depth);
        let rank = bucket.index();
        let head = unsafe { bucket.source().heads().get_unchecked(rank) };
        if self.rank == storage::Rank::new(bucket)
            && head.front() == Some(self.index.get() as usize - 1)
        {
            return;
        }
        let Self { core, index, rank } = self;
        Entry { core, index }.detach(rank).attach(bucket);
    }
}

const _: () = {
    assert!(std::mem::size_of::<Entry<'static, 'static, 0>>() == 2 * std::mem::size_of::<usize>());
    assert!(std::mem::size_of::<Listed<'static, 'static, 0>>() == 2 * std::mem::size_of::<usize>());
};
