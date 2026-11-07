use crate::{port::pool::slot, protocol::runtime::ready::storage};
use o3::depot;
use o3::depot::index::bitmap::entry;
use o3::depot::{index::bitmap, queue::links};
use std::num::NonZero;

pub(in crate::protocol::runtime::ready) struct Entry<'a, 'd, const ID: u8> {
    pub(super) core: &'a storage::Index<'d, ID>,
    pub(super) index: NonZero<u32>,
}

/// The exact entry and bucket retained while its linked transition is committed.
pub(crate) struct Listed<'a, 'd, const ID: u8> {
    pub(super) core: &'a storage::Index<'d, ID>,
    pub(super) index: NonZero<u32>,
    pub(super) rank: storage::Rank,
}

impl<'a, 'd, const ID: u8> Entry<'a, 'd, ID> {
    pub(in crate::protocol::runtime::ready) fn slot(&self) -> &'a slot::Slot<'d, ID> {
        unsafe {
            self.core
                .allocation
                .values()
                .get_unchecked(self.index.get() as usize - 1)
        }
    }

    pub(in crate::protocol::runtime::ready) fn listed(self) -> Option<Listed<'a, 'd, ID>> {
        let rank = self.node().priority.get()?;
        Some(Listed {
            core: self.core,
            index: self.index,
            rank,
        })
    }

    pub(super) fn insert(self, depth: usize) {
        let rank = storage::Rank::new(entry::Row::clamp(&self.core.allocation, depth));
        self.insert_rank(rank);
    }

    pub(super) fn insert_rank(self, rank: storage::Rank) {
        let round = storage::Round(&self.core.allocation);
        // SAFETY: the completed slot transition admits this exact previously unlisted connection.
        unsafe {
            depot::raw::Hooks::cycle_push(&round, &self.core.ring, links::Index::from(self.index))
        };
        self.attach(rank.row(&self.core.allocation));
    }

    pub(super) fn reprioritize(self, depth: usize) {
        let Some(listed) = self.listed() else { return };
        listed.reprioritize(depth);
    }

    pub(super) fn node(&self) -> &'a storage::Node {
        unsafe {
            self.core
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
            if self.core.minimum.get().is_none_or(|current| rank < current) {
                self.core.minimum.set(Some(rank));
            }
        }
        let mut domain = storage::Buckets(&self.core.allocation);
        // SAFETY: this completed transition owns the vacant hook and this exact destination.
        unsafe { domain.link_raw(list, links::Index::from(self.index)) };
        self.node().priority.set(Some(rank));
    }

    /// Called only with the rank consumed from this entry's private Listed witness.
    fn detach(self, rank: storage::Rank) -> Self {
        use bitmap::Access as _;
        use depot::raw::Hooks as _;

        let bucket = rank.row(&self.core.allocation);
        let list = unsafe { bucket.source().heads().get_unchecked(bucket.index()) };
        let mut domain = storage::Buckets(&self.core.allocation);
        // SAFETY: the consumed Listed witness owns this exact entry and bucket.
        unsafe { domain.unlink_raw(list, links::Index::from(self.index)) };
        self.node().priority.set(None);
        if list.is_empty() {
            bucket.update::<false>(());
            if self.core.minimum.get() == Some(rank) {
                let next = (&self.core.allocation).find((), bucket.index() + 1);
                self.core.minimum.set(next.map(storage::Rank::new));
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
        let round = storage::Round(&core.allocation);
        // SAFETY: this capability owns the original listed entry until removal.
        unsafe { depot::raw::Hooks::cycle_unlink(&round, &core.ring, links::Index::from(index)) };
        (Entry { core, index }.detach(rank), rank)
    }

    pub(crate) fn reprioritize(self, depth: usize) {
        let bucket = entry::Row::clamp(&self.core.allocation, depth);
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
