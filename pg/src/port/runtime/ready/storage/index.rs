use std::{cell, marker, mem, num::NonZero};

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

pub(in crate::port::runtime) struct Index<'d, const ID: u8> {
    pub(super) allocation: storage::Block<'d, ID>,
    pub(super) ring: links::Cycle,
    pub(super) minimum: cell::Cell<Option<storage::Rank>>,
    live: cell::Cell<bounded::U32<0, { route::SLOT_MASK as u32 }>>,
    blockers: cell::Cell<bounded::U32<0, { route::SLOT_MASK as u32 }>>,
    thread: marker::PhantomData<ThreadBound>,
}

pub(crate) struct Reservation<'a, 'd, const ID: u8> {
    listed: storage::Listed<'a, 'd, ID>,
    phase: access::Reservation<'a, 'd, ID>,
}

pub(crate) struct Remaining<'a, 'd, const ID: u8> {
    next: Option<storage::Listed<'a, 'd, ID>>,
    end: Option<links::Index>,
}

impl<'a, 'd, const ID: u8> Iterator for Remaining<'a, 'd, ID> {
    type Item = storage::Listed<'a, 'd, ID>;

    fn next(&mut self) -> Option<Self::Item> {
        let listed = self.next.take()?;
        self.next = match self.end {
            Some(end) => {
                let (next, _) = listed.core.step(links::Index::from(listed.index));
                (next != end)
                    .then(|| storage::Entry {
                        core: listed.core,
                        index: next.position(),
                    })
                    .and_then(storage::Entry::listed)
            }
            None => listed.following(),
        };
        Some(listed)
    }
}

impl<'a, 'd, const ID: u8> storage::Listed<'a, 'd, ID> {
    pub(crate) fn remaining(self) -> Remaining<'a, 'd, ID> {
        match self.core.policy.get() {
            protocol::PickPolicy::RoundRobin => {
                let end = links::Index::from(self.index);
                let next = self.core.index.ring.first().filter(|index| *index != end);
                Remaining {
                    next: next.and_then(|index| {
                        storage::Entry {
                            core: self.core,
                            index: index.position(),
                        }
                        .listed()
                    }),
                    end: Some(end),
                }
            }
            protocol::PickPolicy::LeastInflight => Remaining {
                next: self.following(),
                end: None,
            },
        }
    }

    fn following(&self) -> Option<Self> {
        use o3::depot::index::bitmap::Access as _;

        let entry = storage::Entry {
            core: self.core,
            index: self.index,
        };
        if let Some(next) = entry.node().bucket.next_index() {
            return Some(Self {
                core: self.core,
                index: next.position(),
                rank: self.rank,
            });
        }
        let row = self.rank.row(&self.core.index.allocation);
        let next = (&self.core.index.allocation).find((), row.index() + 1)?;
        Some(self.core.first_rank(storage::Rank::new(next)))
    }

    pub(crate) fn reserve(self) -> Option<Reservation<'a, 'd, ID>> {
        let Some(phase) = access::Access::reserve(&self) else {
            self.remove();
            return None;
        };
        Some(Reservation {
            listed: self,
            phase,
        })
    }
}

impl<'a, 'd, const ID: u8> Reservation<'a, 'd, ID> {
    pub(crate) fn ready(&self) -> slot::Ready<'a, 'd, ID> {
        slot::Ready::reserved(&self.phase)
    }

    pub(crate) fn commit(self) -> access::Transaction<'d, ID> {
        let id = self.phase.commit();
        let (entry, _) = self.listed.remove();
        // SAFETY: this Shared -> Reserved transition adds one previously uncounted blocker.
        entry
            .core
            .index
            .blockers
            .set(unsafe { bounded::raw::u32(entry.core.index.blockers.get().get() + 1) });
        id
    }

    pub(crate) fn into_listed(self) -> storage::Listed<'a, 'd, ID> {
        self.listed
    }
}

impl<'d, const ID: u8> Index<'d, ID> {
    pub(in crate::port::runtime) fn try_new(plan: plan::Plan) -> Result<Self, Error> {
        Ok(Self {
            allocation: storage::Block::try_new(plan)?,
            ring: links::Cycle::new(),
            minimum: cell::Cell::new(None),
            live: cell::Cell::new(bounded::U32::exact::<0>()),
            blockers: cell::Cell::new(bounded::U32::exact::<0>()),
            thread: marker::PhantomData,
        })
    }
}

impl<'d, const ID: u8> Shared<'d, ID> {
    pub(in crate::port::runtime) fn entry(
        &self,
        index: usize,
    ) -> Option<storage::Entry<'_, 'd, ID>> {
        self.index.allocation.values().get(index)?;
        let index = unsafe { NonZero::new_unchecked(index as u32 + 1) };
        Some(storage::Entry { core: self, index })
    }

    pub(in crate::port::runtime::ready) fn next(&self) -> Option<storage::Listed<'_, 'd, ID>> {
        let index = self.index.ring.first()?;
        self.step(index).1.listed()
    }

    /// The private ring contains only this allocation's admitted entries;
    /// advancing its head preserves the same indexed owner.
    fn step(&self, index: links::Index) -> (links::Index, storage::Entry<'_, 'd, ID>) {
        // SAFETY: this Index owns the exact reciprocal ring and its resident domain.
        let next = unsafe {
            depot::raw::Hooks::cycle_step(
                &storage::Round(&self.index.allocation),
                &self.index.ring,
                index,
            )
        };
        let entry = storage::Entry {
            core: self,
            index: index.position(),
        };
        (next, entry)
    }

    pub(in crate::port::runtime::ready) fn minimum(&self) -> Option<storage::Listed<'_, 'd, ID>> {
        Some(self.first_rank(self.index.minimum.get()?))
    }

    fn first_rank(&self, rank: storage::Rank) -> storage::Listed<'_, 'd, ID> {
        let row = rank.row(&self.index.allocation);
        // SAFETY: minimum and bitmap hits name this owner's occupied buckets.
        let head = unsafe { self.index.allocation.heads().get_unchecked(row.index()) };
        let index = links::List::front_from(unsafe { depot::raw::Proof::new(head) });
        storage::Listed {
            core: self,
            index: index.position(),
            rank,
        }
    }

    pub(crate) fn try_ready(
        &self,
        id: conn::Id<'d, route::KeyTag<ID>>,
    ) -> Result<storage::Entry<'_, 'd, ID>, protocol::Reason> {
        let entry = self
            .entry(id.index().into_usize())
            .ok_or(protocol::Reason::PrepareReady)?;
        entry.slot().try_ready(id)?;
        entry.insert(0);
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
        let state = access::Access::new(entry.slot()).finalize(id);
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

impl<'d, const ID: u8> storage::Entry<'_, 'd, ID> {
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

const _: () = assert!(
    mem::size_of::<Index<'static, 0>>() == 6 * mem::size_of::<usize>() + 6 * mem::size_of::<u32>()
);
