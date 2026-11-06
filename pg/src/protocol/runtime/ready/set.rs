use crate::port::Disposition;
use dope::core::driver::{self, route, schedule::ready::Target};
use dope::manifold::client::conn;
use dope_fiber::wait::Capacity;
use dope_fiber::{context, wait};
use o3::alloc::Error;
use std::{cell, num, pin};

use crate::port::pool::{access, slot};
use crate::port::replies;
use crate::protocol;
use crate::protocol::runtime::{ready, tx::poll};
use cartel_core::reply::order::Depth;
use ready::{plan, storage};

pub(crate) struct Set<'d, const ID: u8> {
    index: storage::Index<'d, ID>,
    policy: cell::Cell<protocol::PickPolicy>,
    pub(in crate::protocol) waiters: wait::Notifier<'d>,
}

#[derive(Clone, Copy)]
pub(crate) enum Bind {
    Bound,
    Rejected,
}

impl<'d, const ID: u8> Set<'d, ID> {
    pub(crate) fn try_new(
        plan: plan::Plan,
        driver: driver::Reference<'d>,
        cap: Capacity,
    ) -> Result<Self, Error> {
        Ok(Self {
            index: storage::Index::try_new(plan)?,
            policy: cell::Cell::new(protocol::PickPolicy::default()),
            waiters: wait::Notifier::new(driver, cap),
        })
    }

    pub(crate) fn try_register<'target, 'poll>(
        &'target self,
        waiter: pin::Pin<&wait::Waiter<'target, 'd>>,
        ctx: pin::Pin<&context::Context<'poll, 'd>>,
    ) -> Result<(), wait::Error<'d>> {
        self.waiters.try_register(waiter, ctx)
    }

    pub(crate) fn connection(
        &self,
        id: conn::Id<'d, route::KeyTag<ID>>,
    ) -> Option<&slot::Slot<'d, ID>> {
        let slot = self.index.entry(id.index().into_usize())?.slot();
        slot.matches(id).then_some(slot)
    }

    pub(crate) fn current(
        &self,
        id: conn::Id<'d, route::KeyTag<ID>>,
    ) -> access::Current<'_, 'd, ID> {
        let Some(entry) = self.index.entry(id.index().into_usize()) else {
            return access::Current::Stale;
        };
        access::Access::new(entry.slot()).current(id)
    }

    pub(crate) fn bind(&self, id: conn::Id<'d, route::KeyTag<ID>>, target: Target<'d>) -> Bind {
        let Some(entry) = self.index.entry(id.index().into_usize()) else {
            return Bind::Rejected;
        };
        match entry.slot().bind(id, target) {
            slot::Bind::Bound => Bind::Bound,
            slot::Bind::Occupied => Bind::Rejected,
        }
    }

    pub(crate) fn pick<const GROUP: bool>(&self) -> Option<conn::Id<'d, route::KeyTag<ID>>> {
        if GROUP {
            return self.pick_where(slot::Slot::is_synced);
        }
        match self.policy.get() {
            protocol::PickPolicy::RoundRobin => self.index.next(),
            protocol::PickPolicy::LeastInflight => self.index.minimum(),
        }
    }

    pub(crate) fn pick_begin(&self) -> Option<conn::Id<'d, route::KeyTag<ID>>> {
        self.pick_where(slot::Slot::can_begin)
    }

    fn pick_where(
        &self,
        eligible: impl Fn(&slot::Slot<'d, ID>) -> bool,
    ) -> Option<conn::Id<'d, route::KeyTag<ID>>> {
        match self.policy.get() {
            protocol::PickPolicy::RoundRobin => self.index.next_where(eligible),
            protocol::PickPolicy::LeastInflight => self.index.least_where(eligible),
        }
    }

    pub(crate) fn try_ready(
        &self,
        id: conn::Id<'d, route::KeyTag<ID>>,
    ) -> Result<(), protocol::Reason> {
        self.index.try_ready(id)?;
        self.waiters.notify();
        Ok(())
    }

    pub(crate) fn release(
        &self,
        proof: access::Reservation<'_, 'd, ID>,
        depth: usize,
    ) -> access::Release {
        self.index.release(proof, depth)
    }

    pub(crate) fn reserve(
        &self,
        id: conn::Id<'d, route::KeyTag<ID>>,
    ) -> access::Reserve<'_, 'd, ID> {
        self.index.reserve(id)
    }

    pub(in crate::protocol) fn poll_ready(
        &self,
        id: conn::Id<'d, route::KeyTag<ID>>,
        status: poll::Status,
        depth: usize,
    ) -> (poll::Readiness, Option<access::Transaction<'d, ID>>) {
        self.index.poll_ready(id, status, depth)
    }

    pub(crate) fn finalize(&self, id: access::Transaction<'d, ID>) -> access::Finalization<'d, ID> {
        self.index.finalize(id)
    }

    pub(crate) fn try_close(
        &self,
        id: conn::Id<'d, route::KeyTag<ID>>,
        cause: replies::Failure,
        fate: Disposition,
    ) -> Option<slot::Close<'d, ID>> {
        self.index.close(id, cause, fate)
    }

    pub(crate) fn reindex(
        &self,
        id: conn::Id<'d, route::KeyTag<ID>>,
        depth: Depth,
    ) -> Option<conn::Id<'d, route::KeyTag<ID>>> {
        self.index.reindex(id, depth)
    }

    pub(crate) fn set_receiving(
        &self,
        id: conn::Id<'d, route::KeyTag<ID>>,
        receiving: bool,
        depth: impl FnOnce() -> Depth,
    ) {
        if self.index.set_receiving(id, receiving, depth) {
            self.waiters.notify();
        }
    }

    pub(crate) fn count(&self) -> usize {
        self.index.count()
    }

    pub(crate) fn blocking(&self) -> Option<num::NonZero<usize>> {
        self.index.blockers()
    }

    pub(crate) fn set_policy(&self, policy: protocol::PickPolicy) {
        self.policy.set(policy);
    }
}
