use std::num;

use super::Lifecycle;
use crate::{
    port::{Disposition, pool::slot, replies},
    protocol::runtime::{ready, tx::poll},
};
use cartel_core::reply::order::Depth;
use dope::{core::driver::route::KeyTag, manifold::client::conn};
use o3::depot;

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct Sequence(u64);

impl Sequence {
    pub(crate) const INITIAL: Self = Self(0);

    const fn next(self) -> Option<Generation> {
        match num::NonZero::<u64>::new(self.0.wrapping_add(1)) {
            Some(next) => Some(Generation(next)),
            None => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct Generation(num::NonZero<u64>);

impl Generation {
    const fn sequence(self) -> Sequence {
        Sequence(self.0.get())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Transaction<'d, const ID: u8> {
    pub(crate) conn: conn::Id<'d, KeyTag<ID>>,
    pub(crate) generation: Generation,
}

/// A synchronous admission retains its original connection and next generation.
pub(crate) struct Reservation<'a, 'd, const ID: u8> {
    pub(in crate::port::runtime::ready::storage::pool) slot: &'a slot::Slot<'d, ID>,
    id: Transaction<'d, ID>,
}

impl<'d, const ID: u8> Reservation<'_, 'd, ID> {
    pub(crate) fn commit(self) -> Transaction<'d, ID> {
        self.slot.set_access(State::Reserved(self.id.generation));
        self.id
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::port::runtime::ready::storage::pool) enum State {
    Shared(Sequence),
    Reserved(Generation),
    Held(Generation),
    Finalizing(Generation),
    Draining,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum Finalization<'d, const ID: u8> {
    Pending(Transaction<'d, ID>),
    Reusable(Transaction<'d, ID>),
    Lost(Transaction<'d, ID>),
}

impl<'d, const ID: u8> Finalization<'d, ID> {
    pub(crate) const fn id(self) -> Transaction<'d, ID> {
        match self {
            Self::Pending(id) | Self::Reusable(id) | Self::Lost(id) => id,
        }
    }
}

#[repr(u8)]
pub(crate) enum Current {
    Stale,
    Handshaking,
    Shared,
    Reserved(Generation),
    Held(Generation),
    Finalizing(Generation),
    Draining,
    Closing,
}

pub(crate) struct Access<'a, 'd, const ID: u8> {
    slot: &'a slot::Slot<'d, ID>,
}

impl<'a, 'd, const ID: u8> Access<'a, 'd, ID> {
    pub(crate) const fn new(slot: &'a slot::Slot<'d, ID>) -> Self {
        Self { slot }
    }

    pub(crate) fn is_current<const FINAL: bool>(&self, value: Generation) -> bool {
        match self.slot.phase.get() {
            Some(Lifecycle::Ready {
                access: State::Finalizing(current),
                ..
            }) => FINAL && current == value,
            Some(Lifecycle::Ready {
                access: State::Reserved(current) | State::Held(current),
                ..
            }) => !FINAL && current == value,
            _ => false,
        }
    }

    pub(crate) fn reserve(listed: &ready::Listed<'a, 'd, ID>) -> Option<Reservation<'a, 'd, ID>> {
        let slot = listed.slot();
        let Some((
            bound,
            Lifecycle::Ready {
                access: State::Shared(seq),
                ..
            },
        )) = slot.snapshot()
        else {
            unreachable!("listed");
        };
        let Some(next) = seq.next() else {
            slot.set_access(State::Draining);
            return None;
        };
        Some(Reservation {
            slot,
            id: Transaction {
                conn: bound.token,
                generation: next,
            },
        })
    }

    pub(crate) fn finalize(&self, id: Transaction<'d, ID>) -> Finalization<'d, ID> {
        let Some((bound, Lifecycle::Ready { access, .. })) = self.slot.snapshot() else {
            return Finalization::Lost(id);
        };
        if bound.token != id.conn {
            return Finalization::Lost(id);
        }
        if access != State::Held(id.generation) {
            return Finalization::Reusable(id);
        }
        self.slot.set_access(State::Finalizing(id.generation));
        Finalization::Pending(id)
    }

    pub(crate) fn current(&self, token: conn::Id<'d, KeyTag<ID>>) -> Current {
        let Some((bound, phase)) = self.slot.snapshot() else {
            return Current::Stale;
        };
        if bound.token != token {
            return Current::Stale;
        }
        match phase {
            Lifecycle::Handshaking => Current::Handshaking,
            Lifecycle::Ready { access, .. } => match access {
                State::Shared(_) => Current::Shared,
                State::Reserved(generation) => Current::Reserved(generation),
                State::Held(generation) => Current::Held(generation),
                State::Finalizing(generation) => Current::Finalizing(generation),
                State::Draining => Current::Draining,
            },
            Lifecycle::Closing { .. } => Current::Closing,
        }
    }
}

impl<'d, const ID: u8> ready::Entry<'_, 'd, ID> {
    pub(crate) fn set_receiving(&mut self, receiving: bool, depth: impl FnOnce() -> Depth) {
        let slot = self.slot();
        let Some(Lifecycle::Ready {
            access,
            receiving: current,
        }) = slot.phase.get()
        else {
            return;
        };
        if current == receiving {
            return;
        }
        slot.phase.set(Some(Lifecycle::Ready { access, receiving }));
        let entry = Self {
            core: self.core,
            index: self.index,
        };
        if receiving {
            if let Some(listed) = entry.listed() {
                listed.remove();
            }
            return;
        }
        if matches!(access, State::Shared(_)) {
            entry.insert(depth().get() + slot.barriers());
        }
        self.core.wake_ready();
    }

    pub(crate) fn reindex(&mut self, depth: Depth) {
        match self.slot().phase.get() {
            Some(Lifecycle::Ready {
                access: State::Shared(_),
                ..
            }) => self.reprioritize(depth),
            Some(Lifecycle::Ready {
                access: State::Draining,
                ..
            }) => self.drain(depth),
            _ => {}
        }
    }

    #[cold]
    fn drain(&mut self, depth: Depth) {
        let slot = self.slot();
        if depth.get() != 0 || slot.barriers() != 0 {
            return;
        }
        // SAFETY: both private callers retain this exact Ready/Draining binding.
        let id = slot::Slot::bound(unsafe { depot::raw::Proof::new(slot) }).token;
        let entry = Self {
            core: self.core,
            index: self.index,
        };
        if entry
            .close(
                id,
                &mut replies::Failure::from_error(crate::Error::Closed),
                Disposition::Reconnect,
            )
            .is_some()
        {
            slot.wake();
        }
    }

    pub(crate) fn try_settle(
        &mut self,
        status: poll::Status,
        depth: Depth,
    ) -> Option<poll::Readiness> {
        let slot = self.slot();
        let (access, receiving) = match slot.phase.get() {
            Some(Lifecycle::Ready { access, receiving }) => (access, receiving),
            Some(Lifecycle::Closing { .. }) => return Some(poll::Readiness::Ready),
            None | Some(Lifecycle::Handshaking) => return None,
        };
        slot.try_settle()?;
        let generation = match (access, status) {
            (State::Shared(_), _) => {
                self.reprioritize(depth);
                return Some(poll::Readiness::Ready);
            }
            (State::Draining, _) => {
                self.drain(depth);
                return Some(poll::Readiness::Ready);
            }
            (State::Reserved(generation), poll::Status::Transaction) => {
                slot.set_access(State::Held(generation));
                generation
            }
            (State::Reserved(generation) | State::Held(generation), _) => generation,
            (State::Finalizing(_), poll::Status::Transaction) => {
                return Some(poll::Readiness::Pending);
            }
            (State::Finalizing(generation), poll::Status::Idle) => {
                slot.set_access(State::Shared(generation.sequence()));
                if !receiving {
                    Self {
                        core: self.core,
                        index: self.index,
                    }
                    .insert(depth.get() + slot.barriers());
                }
                generation
            }
        };
        // SAFETY: only the retained Ready transaction branches reach this wake.
        let conn = slot::Slot::bound(unsafe { depot::raw::Proof::new(slot) }).token;
        self.core.wake_tx(Transaction { conn, generation });
        Some(poll::Readiness::Ready)
    }
}

const _: () = {
    assert!(std::mem::size_of::<Sequence>() == std::mem::size_of::<u64>());
    assert!(std::mem::size_of::<Generation>() == std::mem::size_of::<u64>());
    assert!(
        std::mem::size_of::<Reservation<'static, 'static, 0>>()
            == std::mem::size_of::<(&(), Transaction<'static, 0>)>()
    );
    assert!(
        std::mem::size_of::<Option<Reservation<'static, 'static, 0>>>()
            == std::mem::size_of::<Reservation<'static, 'static, 0>>()
    );
    assert!(
        std::mem::size_of::<Option<Transaction<'static, 0>>>()
            == std::mem::size_of::<Transaction<'static, 0>>()
    );
    assert!(
        std::mem::size_of::<Current>()
            == std::mem::size_of::<Generation>() + std::mem::align_of::<Generation>()
    );
    assert!(std::mem::size_of::<Access<'static, 'static, 0>>() == std::mem::size_of::<&()>());
};
