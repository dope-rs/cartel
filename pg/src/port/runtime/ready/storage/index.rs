use std::{
    alloc::Layout,
    cell, marker, mem,
    num::NonZero,
    ptr::{self, NonNull},
    slice,
};

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
            Shared,
            ready::{plan, storage},
        },
    },
};

use o3::alloc::raw::LayoutExt as _;
use storage::{Order, Policy};

pub(in crate::port::runtime) struct Index<'d, const ID: u8, P: Policy> {
    slots: NonNull<slot::Slot<'d, ID>>,
    layout: Layout,
    capacity: route::table::conn::Capacity,
    pub(super) order: P::Order,
    live: cell::Cell<bounded::U32<0, { route::SLOT_MASK as u32 }>>,
    blockers: cell::Cell<bounded::U32<0, { route::SLOT_MASK as u32 }>>,
    thread: marker::PhantomData<ThreadBound>,
}

pub(crate) struct Reservation<'a, 'd, const ID: u8, P: Policy> {
    listed: storage::Listed<'a, 'd, ID, P>,
    phase: access::Reservation<'a, 'd, ID>,
}

pub(crate) struct Remaining<'a, 'd, const ID: u8, P: Policy> {
    core: &'a Shared<'d, ID, P>,
    next: Option<(links::Index, <P::Order as Order>::Rank)>,
    end: <P::Order as Order>::End,
}

impl<'a, 'd, const ID: u8, P: Policy> Iterator for Remaining<'a, 'd, ID, P> {
    type Item = storage::Listed<'a, 'd, ID, P>;

    fn next(&mut self) -> Option<Self::Item> {
        let (index, rank) = self.next?;
        // SAFETY: this cursor is consumed within the same exclusive admission turn.
        self.next = unsafe { self.core.index.order.following(index, rank, &self.end) };
        Some(storage::Listed {
            core: self.core,
            index,
            rank,
        })
    }
}

impl<'a, 'd, const ID: u8, P: Policy> storage::Listed<'a, 'd, ID, P> {
    pub(crate) fn remaining(self) -> Remaining<'a, 'd, ID, P> {
        // SAFETY: this is the originally selected candidate of this admission turn.
        let (next, end) = unsafe { self.core.index.order.remaining(self.index, self.rank) };
        Remaining {
            core: self.core,
            next,
            end,
        }
    }

    pub(crate) fn reserve(self) -> Option<Reservation<'a, 'd, ID, P>> {
        let Some(phase) = access::Reservation::reserve(&self) else {
            self.remove();
            return None;
        };
        Some(Reservation {
            listed: self,
            phase,
        })
    }
}

impl<'a, 'd, const ID: u8, P: Policy> Reservation<'a, 'd, ID, P> {
    pub(crate) fn ready(&self) -> slot::Ready<'a, 'd, ID> {
        slot::Ready::reserved(&self.phase)
    }

    pub(crate) fn commit(self) -> access::Transaction<'d, ID> {
        let id = self.phase.commit();
        let entry = self.listed.remove();
        // SAFETY: this Shared -> Reserved transition adds one previously uncounted blocker.
        entry
            .core
            .index
            .blockers
            .set(unsafe { bounded::raw::u32(entry.core.index.blockers.get().get() + 1) });
        id
    }

    pub(crate) fn into_listed(self) -> storage::Listed<'a, 'd, ID, P> {
        self.listed
    }
}

impl<'d, const ID: u8, P: Policy> Index<'d, ID, P> {
    pub(in crate::port::runtime) fn try_new(plan: plan::Plan<P>) -> Result<Self, Error> {
        let ptr = plan.allocation.try_allocate::<u8>()?;
        let count = plan.capacity.entries().get();
        let slots = ptr.cast::<slot::Slot<'d, ID>>();
        // SAFETY: values start at offset zero, followed by this policy's admitted arrays.
        // Slot and schedule initialization do not exit early or re-enter the owner.
        for index in 0..count {
            unsafe { slots.add(index).write(slot::Slot::new()) };
        }
        let order = unsafe { P::Order::install(ptr, count, plan.offsets) };
        Ok(Self {
            slots,
            layout: plan.allocation,
            capacity: plan.capacity,
            order,
            live: cell::Cell::new(bounded::U32::exact::<0>()),
            blockers: cell::Cell::new(bounded::U32::exact::<0>()),
            thread: marker::PhantomData,
        })
    }

    pub(super) fn values(&self) -> &[slot::Slot<'d, ID>] {
        unsafe { slice::from_raw_parts(self.slots.as_ptr(), self.capacity.entries().get()) }
    }
}

impl<const ID: u8, P: Policy> Drop for Index<'_, ID, P> {
    fn drop(&mut self) {
        if mem::needs_drop::<slot::Slot<'_, ID>>() {
            let values =
                ptr::slice_from_raw_parts_mut(self.slots.as_ptr(), self.capacity.entries().get());
            unsafe { values.drop_in_place() };
        }
        // The sealed order contains only pointer/bitmap/hook metadata with no drop effect.
        unsafe { self.layout.deallocate(self.slots) };
    }
}

impl<'d, const ID: u8, P: Policy> Shared<'d, ID, P> {
    pub(in crate::port::runtime) fn entry(
        &self,
        index: usize,
    ) -> Option<storage::Entry<'_, 'd, ID, P>> {
        self.index.values().get(index)?;
        // SAFETY: the route slot capacity excludes the compact sentinel.
        let index = unsafe { <links::Index as depot::raw::Word>::from_offset(index) };
        Some(storage::Entry { core: self, index })
    }

    pub(crate) fn pick(&self) -> Option<storage::Listed<'_, 'd, ID, P>> {
        let (index, rank) = self.index.order.pick()?;
        Some(storage::Listed {
            core: self,
            index,
            rank,
        })
    }

    pub(crate) fn try_ready(
        &self,
        id: conn::Id<'d, route::KeyTag<ID>>,
    ) -> Result<storage::Entry<'_, 'd, ID, P>, protocol::Reason> {
        let entry = self
            .entry(id.index().into_usize())
            .ok_or(protocol::Reason::PrepareReady)?;
        entry.slot().try_ready(id)?;
        entry.insert(|| 0);
        // SAFETY: only Handshaking -> Ready succeeds above. That uncounted
        // resident makes live strictly smaller than this fixed connection domain.
        self.index
            .live
            .set(unsafe { bounded::raw::u32(self.index.live.get().get() + 1) });
        self.wake_ready();
        Ok(entry)
    }

    pub(crate) fn finalize(&self, id: access::Transaction<'d, ID>) -> access::Finalization<'d, ID> {
        let Some(entry) = self.entry(id.conn.index().into_usize()) else {
            return access::Finalization::Lost(id);
        };
        let state = entry.slot().finalize(id);
        if let access::Finalization::Pending(_) = state {
            // SAFETY: Held -> Finalizing consumes the original counted blocker.
            let left = self.index.blockers.get().get() - 1;
            self.index.blockers.set(unsafe { bounded::raw::u32(left) });
        }
        state
    }

    pub(crate) fn count(&self) -> usize {
        self.index.live.get().into_usize()
    }

    pub(crate) fn blocking(&self) -> Option<NonZero<usize>> {
        NonZero::new(self.index.blockers.get().into_usize())
    }
}

impl<'d, const ID: u8, P: Policy> storage::Entry<'_, 'd, ID, P> {
    pub(in crate::port::runtime) fn close(
        self,
        id: conn::Id<'d, route::KeyTag<ID>>,
        cause: &mut replies::Failure,
        fate: Disposition,
    ) -> Option<()> {
        let core = self.core;
        let slot = self.slot();
        let listed = self.listed();
        let close = slot.try_close(id, cause, fate)?;
        if let Some(listed) = listed {
            listed.remove();
        }
        match &close {
            slot::Closed::Reserved(_) | slot::Closed::Held(_) => {
                // SAFETY: both returned phases own one counted blocker.
                let left = core.index.blockers.get().get() - 1;
                core.index.blockers.set(unsafe { bounded::raw::u32(left) });
            }
            slot::Closed::Shared | slot::Closed::Finalizing(_) | slot::Closed::Draining => {}
            slot::Closed::Handshaking | slot::Closed::Closing => {
                return Some(core.apply_close(close));
            }
        }
        // SAFETY: every remaining Closed variant consumed one Ready resident;
        // repeated close and stale generations return before this subtraction.
        let left = core.index.live.get().get() - 1;
        core.index.live.set(unsafe { bounded::raw::u32(left) });
        Some(core.apply_close(close))
    }
}
