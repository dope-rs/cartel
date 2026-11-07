use crate::{api, api::task::lease::Target as _, port::Disposition};
use dope::core::driver::{self, route, schedule::ready::Target};
use dope::manifold::client::conn;
use dope_fiber::wait::Capacity;
use dope_fiber::{context, wait};
use o3::{alloc::Error, base::cell::region};
use std::{cell, num, pin};

use crate::port::pool::{access, slot};
use crate::port::replies;
use crate::port::requests::Boundary;
use crate::protocol;
use crate::protocol::runtime::{ready, tx::poll};
use cartel_core::reply::order::Depth;
use ready::{plan, storage};

enum Source<'a, 'd, const ID: u8> {
    Shared(storage::Listed<'a, 'd, ID>),
    Exclusive(slot::Ready<'a, 'd, ID>),
}

pub(crate) struct ReplyContext<'turn, 'a, 'd, const ID: u8> {
    source: Source<'a, 'd, ID>,
    boundary: Boundary,
    region: &'turn mut region::Token<region::Identity<'d>>,
}

impl<'turn, 'a: 'turn, 'd, const ID: u8> ReplyContext<'turn, 'a, 'd, ID> {
    pub(crate) fn reserved(
        proof: &ready::Reservation<'a, 'd, ID>,
        boundary: Boundary,
        region: &'turn mut region::Token<region::Identity<'d>>,
    ) -> Self {
        Self {
            source: Source::Exclusive(proof.ready()),
            boundary,
            region,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        slot::Ready<'turn, 'd, ID>,
        Option<storage::Listed<'turn, 'd, ID>>,
        Boundary,
        &'turn mut region::Token<region::Identity<'d>>,
    ) {
        let (ready, listed) = match self.source {
            Source::Shared(listed) => (slot::Ready::listed(&listed), Some(listed)),
            Source::Exclusive(ready) => (ready, None),
        };
        (ready, listed, self.boundary, self.region)
    }
}

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

    pub(crate) fn select<'turn, 'a: 'turn>(
        &'a self,
        target: &Option<api::Target<'d, ID>>,
        boundary: Boundary,
        region: &'turn mut region::Token<region::Identity<'d>>,
    ) -> Result<Option<ReplyContext<'turn, 'a, 'd, ID>>, api::Target<'d, ID>> {
        let source = if let Some(target) = *target {
            let token = target.connection();
            let generation = target.transaction().map(|tx| tx.generation);
            let entry = self.index.entry(token.index().into_usize()).ok_or(target)?;
            let slot = entry.slot();
            let ready = slot.select(token, generation).ok_or(target)?;
            if slot.is_receiving() {
                return Ok(None);
            }
            if generation.is_some() {
                Source::Exclusive(ready)
            } else {
                let Some(listed) = entry.listed() else {
                    return Ok(None);
                };
                Source::Shared(listed)
            }
        } else {
            let selected = match boundary {
                Boundary::Group => self.pick::<true>(),
                _ => self.pick::<false>(),
            };
            let Some(listed) = selected else {
                return Ok(None);
            };
            Source::Shared(listed)
        };
        Ok(Some(ReplyContext {
            source,
            boundary,
            region,
        }))
    }

    fn pick<const GROUP: bool>(&self) -> Option<storage::Listed<'_, 'd, ID>> {
        if GROUP {
            return self.pick_where(slot::Slot::is_synced);
        }
        match self.policy.get() {
            protocol::PickPolicy::RoundRobin => self.index.next(),
            protocol::PickPolicy::LeastInflight => self.index.minimum(),
        }
    }

    pub(crate) fn pick_begin(&self) -> Option<storage::Listed<'_, 'd, ID>> {
        self.pick_where(slot::Slot::can_begin)
    }

    fn pick_where(
        &self,
        eligible: impl Fn(&slot::Slot<'d, ID>) -> bool,
    ) -> Option<storage::Listed<'_, 'd, ID>> {
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
