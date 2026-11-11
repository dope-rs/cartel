use crate::{api, api::task::lease::Target as _};
use dope::core::driver::{route, schedule::ready::Target};
use dope::manifold::client::conn;
use dope_fiber::{context, wait};
use o3::base::cell::region;
use std::pin;

use crate::port::pool::{access, slot};
use crate::port::requests::Boundary;
use crate::protocol;
use crate::protocol::runtime::{Shared, ready};
use ready::storage;

pub(crate) enum Source<'a, 'd, const ID: u8> {
    Shared(storage::Listed<'a, 'd, ID>),
    Exclusive(slot::Ready<'a, 'd, ID>),
}

impl<'a, 'd, const ID: u8> Source<'a, 'd, ID> {
    fn ready(&self) -> slot::Ready<'a, 'd, ID> {
        match self {
            Self::Shared(listed) => slot::Ready::listed(listed),
            Self::Exclusive(ready) => ready.reborrow(),
        }
    }

    pub(crate) fn connection(&self) -> conn::Id<'d, route::KeyTag<ID>> {
        self.ready().connection()
    }

    pub(crate) fn context<'turn>(
        self,
        boundary: Boundary,
        region: &'turn mut region::Token<region::Identity<'d>>,
    ) -> ReplyContext<'turn, 'a, 'd, ID> {
        ReplyContext {
            source: self,
            boundary,
            region,
        }
    }

    pub(crate) fn into_listed(self) -> Option<storage::Listed<'a, 'd, ID>> {
        match self {
            Self::Shared(listed) => Some(listed),
            Self::Exclusive(_) => None,
        }
    }
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
        slot::Ready<'a, 'd, ID>,
        Source<'a, 'd, ID>,
        Boundary,
        &'turn mut region::Token<region::Identity<'d>>,
    ) {
        (self.source.ready(), self.source, self.boundary, self.region)
    }
}

#[derive(Clone, Copy)]
pub(crate) enum Bind {
    Bound,
    Rejected,
}

impl<'d, const ID: u8> Shared<'d, ID> {
    pub(crate) fn try_register<'target, 'poll>(
        &'target self,
        waiter: pin::Pin<&wait::Waiter<'target, 'd>>,
        ctx: pin::Pin<&context::Context<'poll, 'd>>,
    ) -> Result<(), wait::Error<'d>> {
        self.ready_waiters.try_register(waiter, ctx)
    }

    pub(crate) fn connection(
        &self,
        id: conn::Id<'d, route::KeyTag<ID>>,
    ) -> Option<&slot::Slot<'d, ID>> {
        self.connection_entry(id).map(|entry| entry.slot())
    }

    pub(crate) fn connection_entry(
        &self,
        id: conn::Id<'d, route::KeyTag<ID>>,
    ) -> Option<storage::Entry<'_, 'd, ID>> {
        let entry = self.entry(id.index().into_usize())?;
        entry.slot().matches(id).then_some(entry)
    }

    pub(crate) fn current(&self, id: conn::Id<'d, route::KeyTag<ID>>) -> access::Current {
        let Some(entry) = self.entry(id.index().into_usize()) else {
            return access::Current::Stale;
        };
        access::Access::new(entry.slot()).current(id)
    }

    pub(crate) fn bind(&self, id: conn::Id<'d, route::KeyTag<ID>>, target: Target<'d>) -> Bind {
        let Some(entry) = self.entry(id.index().into_usize()) else {
            return Bind::Rejected;
        };
        match entry.slot().bind(id, target) {
            slot::Bind::Bound => Bind::Bound,
            slot::Bind::Occupied => Bind::Rejected,
        }
    }

    pub(crate) fn select(
        &self,
        target: &Option<api::Target<'d, ID>>,
    ) -> Result<Option<Source<'_, 'd, ID>>, api::Target<'d, ID>> {
        let source = if let Some(target) = *target {
            let token = target.connection();
            let generation = target.transaction().map(|tx| tx.generation);
            let entry = self.entry(token.index().into_usize()).ok_or(target)?;
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
            let Some(listed) = self.pick() else {
                return Ok(None);
            };
            Source::Shared(listed)
        };
        Ok(Some(source))
    }

    pub(crate) fn pick(&self) -> Option<storage::Listed<'_, 'd, ID>> {
        match self.policy.get() {
            protocol::PickPolicy::RoundRobin => self.next(),
            protocol::PickPolicy::LeastInflight => self.minimum(),
        }
    }

    pub(crate) fn set_policy(&self, policy: protocol::PickPolicy) {
        self.policy.set(policy);
    }
}
