use super::{Order, Policy};
use crate::{port::pool::slot, protocol::runtime::Shared};
use cartel_core::reply::order::Depth;
use o3::depot::queue::links;

#[derive(Clone, Copy)]
pub(crate) struct Entry<'a, 'd, const ID: u8, P: Policy> {
    pub(super) core: &'a Shared<'d, ID, P>,
    pub(super) index: links::Index,
}

pub(crate) struct Listed<'a, 'd, const ID: u8, P: Policy> {
    pub(super) core: &'a Shared<'d, ID, P>,
    pub(super) index: links::Index,
    pub(super) rank: <P::Order as Order>::Rank,
}

impl<'a, 'd, const ID: u8, P: Policy> Entry<'a, 'd, ID, P> {
    pub(crate) fn slot(&self) -> &'a slot::Slot<'d, ID> {
        // SAFETY: all entries come from checked lookup or this exact order's live cursor.
        unsafe { self.core.index.values().get_unchecked(self.index.get()) }
    }

    pub(in crate::port::runtime::ready) fn listed(self) -> Option<Listed<'a, 'd, ID, P>> {
        let rank = unsafe { self.core.index.order.rank(self.index) }?;
        Some(Listed {
            core: self.core,
            index: self.index,
            rank,
        })
    }

    pub(super) fn insert(self, depth: impl FnOnce() -> usize) {
        // SAFETY: the successful slot transition admits this previously unlisted member.
        unsafe { self.core.index.order.insert(self.index, depth) };
    }

    pub(super) fn reprioritize(&mut self, depth: Depth) {
        if P::Order::RANKED
            && let Some(listed) = self.listed()
        {
            listed.reprioritize(|| depth.get() + self.slot().barriers());
        }
    }
}

impl<'a, 'd, const ID: u8, P: Policy> Listed<'a, 'd, ID, P> {
    fn entry(&self) -> Entry<'a, 'd, ID, P> {
        Entry {
            core: self.core,
            index: self.index,
        }
    }

    pub(crate) fn slot(&self) -> &'a slot::Slot<'d, ID> {
        self.entry().slot()
    }

    pub(super) fn remove(self) -> Entry<'a, 'd, ID, P> {
        // SAFETY: this consuming witness carries the exact current member and its rank.
        unsafe { self.core.index.order.remove(self.index, self.rank) };
        self.entry()
    }

    pub(crate) fn reprioritize(self, depth: impl FnOnce() -> usize) {
        unsafe {
            self.core
                .index
                .order
                .reprioritize(self.index, self.rank, depth)
        };
    }
}

const _: () = {
    use super::{LeastInflight, RoundRobin};
    assert!(size_of::<Entry<'static, 'static, 0, RoundRobin>>() == 2 * size_of::<usize>());
    assert!(size_of::<Listed<'static, 'static, 0, RoundRobin>>() == 2 * size_of::<usize>());
    assert!(size_of::<Listed<'static, 'static, 0, LeastInflight>>() == 2 * size_of::<usize>());
};
