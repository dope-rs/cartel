use dope::{core::driver::route::KeyTag, manifold::client::conn};

use crate::{
    port::pool::{key, slot},
    protocol,
};

mod bound;
mod rejected;

pub(crate) use bound::Bound;
pub(crate) use rejected::Rejected;

#[derive(Clone, Copy)]
pub(crate) struct Backend<'a, 'd, const ID: u8> {
    slot: &'a slot::Slot<'d, ID>,
}

impl<'a, 'd, const ID: u8> Backend<'a, 'd, ID> {
    pub(crate) const fn new(slot: &'a slot::Slot<'d, ID>) -> Self {
        Self { slot }
    }

    pub(crate) fn try_install(
        &self,
        token: conn::Id<'d, KeyTag<ID>>,
        key: key::Key,
    ) -> Result<(), protocol::Reason> {
        let slot::Phase::Handshaking(bound) = self.slot.phase.get() else {
            return Err(protocol::Reason::BackendKeyStale);
        };
        if bound.token != token {
            return Err(protocol::Reason::BackendKeyStale);
        }
        if self.slot.pid.get().is_some() {
            return Err(protocol::Reason::DuplicateBackendKey);
        }
        self.slot.pid.set(Some(key.pid()));
        self.slot.key.set(key);
        Ok(())
    }

    pub(crate) fn bind(self, target: conn::Id<'d, KeyTag<ID>>) -> Option<(Bound<'a, 'd, ID>, i32)> {
        let pid = self.pid(target)?;
        Some((
            Bound {
                slot: self.slot,
                target,
            },
            pid,
        ))
    }

    pub(crate) fn pid(&self, token: conn::Id<'d, KeyTag<ID>>) -> Option<i32> {
        let slot::Phase::Ready { bound, .. } = self.slot.phase.get() else {
            return None;
        };
        if bound.token != token {
            return None;
        }
        self.slot.pid.get()
    }
}

const _: () = {
    assert!(std::mem::size_of::<Backend<'static, 'static, 0>>() == std::mem::size_of::<&()>());
};
