use std::{cell, marker, mem, num::NonZero};

use cartel_core::reply::order::Depth;
use dope::core::driver::route;
use dope::manifold::client::conn;
use o3::base::num::bounded;
use o3::{alloc::Error, base::marker::ThreadBound, depot, depot::queue::links};

use crate::{
    port::{
        Disposition,
        pool::{access, slot},
        replies,
    },
    protocol::{
        self,
        runtime::{
            ready::{plan, storage},
            tx::poll,
        },
    },
};

pub(in crate::protocol::runtime::ready) struct Index<'d, const ID: u8> {
    pub(super) allocation: storage::Block<'d, ID>,
    pub(super) ring: links::Cycle,
    pub(super) minimum: cell::Cell<Option<storage::Rank>>,
    live: cell::Cell<bounded::U32<0, { route::SLOT_MASK as u32 }>>,
    blockers: cell::Cell<bounded::U32<0, { route::SLOT_MASK as u32 }>>,
    thread: marker::PhantomData<ThreadBound>,
}

impl<'d, const ID: u8> Index<'d, ID> {
    pub(in crate::protocol::runtime::ready) fn try_new(plan: plan::Plan) -> Result<Self, Error> {
        Ok(Self {
            allocation: storage::Block::try_new(plan)?,
            ring: links::Cycle::new(),
            minimum: cell::Cell::new(None),
            live: cell::Cell::new(bounded::U32::exact::<0>()),
            blockers: cell::Cell::new(bounded::U32::exact::<0>()),
            thread: marker::PhantomData,
        })
    }

    pub(in crate::protocol::runtime::ready) fn entry(
        &self,
        index: usize,
    ) -> Option<storage::Entry<'_, 'd, ID>> {
        self.allocation.values().get(index)?;
        let index = unsafe { NonZero::new_unchecked(index as u32 + 1) };
        Some(storage::Entry { core: self, index })
    }

    pub(in crate::protocol::runtime::ready) fn next(
        &self,
    ) -> Option<conn::Id<'d, route::KeyTag<ID>>> {
        let index = self.ring.first()?;
        let slot = self.step(index).1;
        // SAFETY: this same-owner ring contains only initialized Shared bindings.
        Some(access::Access::listed(unsafe {
            depot::raw::Proof::new(slot)
        }))
    }

    /// The private ring contains only this allocation's admitted entries;
    /// advancing its head preserves the same indexed owner.
    fn step(&self, index: links::Index) -> (links::Index, &slot::Slot<'d, ID>) {
        // SAFETY: this Index owns the exact reciprocal ring and its resident domain.
        let next = unsafe {
            depot::raw::Hooks::cycle_step(&storage::Round(&self.allocation), &self.ring, index)
        };
        let entry = storage::Entry {
            core: self,
            index: index.position(),
        };
        (next, entry.slot())
    }

    pub(in crate::protocol::runtime::ready) fn minimum(
        &self,
    ) -> Option<conn::Id<'d, route::KeyTag<ID>>> {
        let row = self.minimum.get()?.row(&self.allocation);
        // SAFETY: the retained rank belongs to this allocation. It is stored as
        // the minimum only while this exact bucket is occupied.
        let head = unsafe { self.allocation.heads().get_unchecked(row.index()) };
        let index = links::List::front_from(unsafe { depot::raw::Proof::new(head) });
        let entry = storage::Entry {
            core: self,
            index: index.position(),
        };
        // SAFETY: this occupied bucket contains only this owner's Shared bindings.
        Some(access::Access::listed(unsafe {
            depot::raw::Proof::new(entry.slot())
        }))
    }

    pub(in crate::protocol::runtime::ready) fn next_where(
        &self,
        eligible: impl Fn(&slot::Slot<'d, ID>) -> bool,
    ) -> Option<conn::Id<'d, route::KeyTag<ID>>> {
        let first = self.ring.first()?;
        let mut index = first;
        loop {
            let (next, slot) = self.step(index);
            if eligible(slot) {
                // SAFETY: step retains this ring member's initialized binding.
                return Some(access::Access::listed(unsafe {
                    depot::raw::Proof::new(slot)
                }));
            }
            if next == first {
                return None;
            }
            index = next;
        }
    }

    pub(in crate::protocol::runtime::ready) fn least_where(
        &self,
        eligible: impl Fn(&slot::Slot<'d, ID>) -> bool,
    ) -> Option<conn::Id<'d, route::KeyTag<ID>>> {
        use o3::depot::index::bitmap::Access as _;

        let mut row = self.minimum.get()?.row(&self.allocation);
        loop {
            // SAFETY: minimum and subsequent bitmap hits retain an admitted row.
            let head = unsafe { self.allocation.heads().get_unchecked(row.index()) };
            // SAFETY: each hit names this owner's occupied bucket until the
            // synchronous traversal completes; no transition runs in the loop.
            let mut index = links::List::front_from(unsafe { depot::raw::Proof::new(head) });
            loop {
                let entry = storage::Entry {
                    core: self,
                    index: index.position(),
                };
                let slot = entry.slot();
                if eligible(slot) {
                    // SAFETY: bucket links retain this owner's initialized binding.
                    return Some(access::Access::listed(unsafe {
                        depot::raw::Proof::new(slot)
                    }));
                }
                let Some(next) = entry.node().bucket.next_index() else {
                    break;
                };
                index = next;
            }
            row = (&self.allocation).find((), row.index() + 1)?;
        }
    }

    pub(in crate::protocol::runtime::ready) fn try_ready(
        &self,
        id: conn::Id<'d, route::KeyTag<ID>>,
    ) -> Result<(), protocol::Reason> {
        let entry = self
            .entry(id.index().into_usize())
            .ok_or(protocol::Reason::PrepareReady)?;
        entry.slot().try_ready(id)?;
        entry.insert(0);
        // SAFETY: only Handshaking -> Ready succeeds above. That uncounted
        // resident makes live strictly smaller than this fixed connection domain.
        self.live
            .set(unsafe { bounded::raw::u32(self.live.get().get() + 1) });
        Ok(())
    }

    pub(in crate::protocol::runtime::ready) fn release(
        &self,
        proof: access::Reservation<'_, 'd, ID>,
        depth: usize,
    ) -> access::Release {
        let Some(entry) = self.entry(proof.id().conn.index().into_usize()) else {
            return access::Release::Stale;
        };
        match access::Access::new(entry.slot()).release(proof) {
            access::Release::Released => {
                if !entry.slot().is_receiving() {
                    entry.insert(depth);
                }
                // SAFETY: the exact Reserved -> Shared transition consumes one
                // counted blocker; stale generations never reach this branch.
                let left = self.blockers.get().get() - 1;
                self.blockers.set(unsafe { bounded::raw::u32(left) });
                access::Release::Released
            }
            access::Release::Stale => access::Release::Stale,
        }
    }

    pub(in crate::protocol::runtime::ready) fn reserve(
        &self,
        id: conn::Id<'d, route::KeyTag<ID>>,
    ) -> access::Reserve<'_, 'd, ID> {
        let Some(entry) = self.entry(id.index().into_usize()) else {
            return access::Reserve::Rejected;
        };
        let Some(listed) = entry.listed() else {
            return access::Reserve::Rejected;
        };
        let state = access::Access::new(listed.slot()).reserve(id);
        match &state {
            access::Reserve::Reserved(_) => {
                listed.remove();
                // SAFETY: Shared -> Reserved adds one previously uncounted
                // blocker in the same fixed connection domain.
                self.blockers
                    .set(unsafe { bounded::raw::u32(self.blockers.get().get() + 1) });
            }
            access::Reserve::Exhausted => listed.remove(),
            access::Reserve::Rejected => {}
        }
        state
    }

    pub(in crate::protocol::runtime::ready) fn poll_ready(
        &self,
        slot: conn::Id<'d, route::KeyTag<ID>>,
        status: poll::Status,
        depth: usize,
    ) -> (poll::Readiness, Option<access::Transaction<'d, ID>>) {
        let Some(entry) = self.entry(slot.index().into_usize()) else {
            return (poll::Readiness::Ready, None);
        };
        let id = match (access::Access::new(entry.slot()).current(slot), status) {
            (access::Current::Reserved(proof), poll::Status::Idle) => proof.id(),
            (access::Current::Reserved(proof), poll::Status::Transaction) => proof.activate(),
            (access::Current::Held(id), _) => id,
            (access::Current::Finalizing(_), poll::Status::Transaction) => {
                return (poll::Readiness::Pending, None);
            }
            (access::Current::Finalizing(proof), poll::Status::Idle) => {
                let id = proof.finish();
                if !entry.slot().is_receiving() {
                    entry.insert(depth);
                }
                id
            }
            (
                access::Current::Stale
                | access::Current::Handshaking
                | access::Current::Shared
                | access::Current::Draining
                | access::Current::Closing,
                _,
            ) => {
                return (poll::Readiness::Ready, None);
            }
        };
        (poll::Readiness::Ready, Some(id))
    }

    pub(in crate::protocol::runtime::ready) fn finalize(
        &self,
        id: access::Transaction<'d, ID>,
    ) -> access::Finalization<'d, ID> {
        let Some(entry) = self.entry(id.conn.index().into_usize()) else {
            return access::Finalization::Lost(id);
        };
        let state = access::Access::new(entry.slot()).finalize(id);
        if let access::Finalization::Pending(_) = state {
            // SAFETY: Held -> Finalizing consumes the original counted blocker.
            let left = self.blockers.get().get() - 1;
            self.blockers.set(unsafe { bounded::raw::u32(left) });
        }
        state
    }

    pub(in crate::protocol::runtime::ready) fn close(
        &self,
        id: conn::Id<'d, route::KeyTag<ID>>,
        cause: replies::Failure,
        fate: Disposition,
    ) -> Option<slot::Close<'d, ID>> {
        let entry = self.entry(id.index().into_usize())?;
        let slot = entry.slot();
        let listed = entry.listed();
        let close = slot.try_close(id, cause, fate)?;
        if let Some(listed) = listed {
            listed.remove();
        }
        match &close.state {
            slot::Closed::Reserved(_) | slot::Closed::Held(_) => {
                // SAFETY: both returned phases own one counted blocker.
                let left = self.blockers.get().get() - 1;
                self.blockers.set(unsafe { bounded::raw::u32(left) });
            }
            slot::Closed::Shared | slot::Closed::Finalizing(_) | slot::Closed::Draining => {}
            slot::Closed::Handshaking | slot::Closed::Closing => return Some(close),
        }
        // SAFETY: every remaining Closed variant consumed one Ready resident;
        // repeated close and stale generations return before this subtraction.
        let left = self.live.get().get() - 1;
        self.live.set(unsafe { bounded::raw::u32(left) });
        Some(close)
    }

    /// Both counters originate in u32 domains and o3 admits usize widths of at
    /// least 64 bits. The optional open barrier adds at most one.
    pub(in crate::protocol::runtime::ready) fn reindex(
        &self,
        id: conn::Id<'d, route::KeyTag<ID>>,
        depth: Depth,
    ) -> Option<conn::Id<'d, route::KeyTag<ID>>> {
        let entry = self.entry(id.index().into_usize())?;
        let load = depth.get() + entry.slot().barriers();
        match access::Access::new(entry.slot()).current(id) {
            access::Current::Shared => {
                entry.reprioritize(load);
                None
            }
            access::Current::Draining => (load == 0).then_some(id),
            access::Current::Stale
            | access::Current::Handshaking
            | access::Current::Reserved(_)
            | access::Current::Held(_)
            | access::Current::Finalizing(_)
            | access::Current::Closing => None,
        }
    }

    pub(in crate::protocol::runtime::ready) fn set_receiving(
        &self,
        id: conn::Id<'d, route::KeyTag<ID>>,
        receiving: bool,
        depth: impl FnOnce() -> Depth,
    ) -> bool {
        let Some(entry) = self.entry(id.index().into_usize()) else {
            return false;
        };
        let slot = entry.slot();
        if !slot.set_receiving(id, receiving) {
            return false;
        }
        if receiving {
            if let Some(listed) = entry.listed() {
                listed.remove();
            }
            return false;
        }
        if matches!(
            access::Access::new(slot).current(id),
            access::Current::Shared
        ) {
            entry.insert(depth().get() + slot.barriers());
        }
        true
    }

    pub(in crate::protocol::runtime::ready) fn count(&self) -> usize {
        self.live.get().into_usize()
    }

    pub(in crate::protocol::runtime::ready) fn blockers(&self) -> Option<NonZero<usize>> {
        NonZero::new(self.blockers.get().into_usize())
    }
}

const _: () = assert!(
    mem::size_of::<Index<'static, 0>>() == 6 * mem::size_of::<usize>() + 6 * mem::size_of::<u32>()
);
