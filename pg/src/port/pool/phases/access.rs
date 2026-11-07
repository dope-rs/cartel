use std::num;

use crate::{port::pool::slot, protocol::runtime::ready};
use dope::{core::driver::route::KeyTag, manifold::client::conn};

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

/// A synchronous reservation retains its original connection and transition target.
/// It is consumed before the caller can yield the worker to another protocol turn.
pub(crate) struct Reservation<'a, 'd, const ID: u8> {
    pub(in crate::port::pool) slot: &'a slot::Slot<'d, ID>,
    id: Transaction<'d, ID>,
}

impl<'d, const ID: u8> Reservation<'_, 'd, ID> {
    pub(crate) const fn id(&self) -> Transaction<'d, ID> {
        self.id
    }

    pub(crate) fn activate(self) -> Transaction<'d, ID> {
        self.slot.phase.set_access(State::Held(self.id.generation));
        self.id
    }

    pub(crate) fn release(self) -> Transaction<'d, ID> {
        self.slot
            .phase
            .set_access(State::Shared(self.id.generation.sequence()));
        self.id
    }
}

/// One finalizing state selected in the current synchronous protocol turn.
pub(crate) struct Finalizing<'a, 'd, const ID: u8> {
    slot: &'a slot::Slot<'d, ID>,
    id: Transaction<'d, ID>,
}

impl<'d, const ID: u8> Finalizing<'_, 'd, ID> {
    pub(crate) const fn id(&self) -> Transaction<'d, ID> {
        self.id
    }

    pub(crate) fn finish(self) -> Transaction<'d, ID> {
        self.slot
            .phase
            .set_access(State::Shared(self.id.generation.sequence()));
        self.id
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::port::pool) enum State {
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
pub(crate) enum Current<'a, 'd, const ID: u8> {
    Stale,
    Handshaking,
    Shared,
    Reserved(Reservation<'a, 'd, ID>),
    Held(Transaction<'d, ID>),
    Finalizing(Finalizing<'a, 'd, ID>),
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
            slot::Phase::Ready {
                access: State::Finalizing(current),
                ..
            } => FINAL && current == value,
            slot::Phase::Ready {
                access: State::Reserved(current) | State::Held(current),
                ..
            } => !FINAL && current == value,
            _ => false,
        }
    }

    pub(crate) fn reserve(listed: &ready::Listed<'a, 'd, ID>) -> Option<Reservation<'a, 'd, ID>> {
        let slot = listed.slot();
        let slot::Phase::Ready {
            bound,
            access: State::Shared(seq),
        } = slot.phase.get()
        else {
            unreachable!("listed");
        };
        let Some(next) = seq.next() else {
            slot.phase.set_access(State::Draining);
            return None;
        };
        slot.phase.set_access(State::Reserved(next));
        Some(Reservation {
            slot,
            id: Transaction {
                conn: bound.token,
                generation: next,
            },
        })
    }

    pub(crate) fn finalize(&self, id: Transaction<'d, ID>) -> Finalization<'d, ID> {
        let slot::Phase::Ready { bound, access } = self.slot.phase.get() else {
            return Finalization::Lost(id);
        };
        if bound.token != id.conn {
            return Finalization::Lost(id);
        }
        if access != State::Held(id.generation) {
            return Finalization::Reusable(id);
        }
        self.slot.phase.set_access(State::Finalizing(id.generation));
        Finalization::Pending(id)
    }

    pub(crate) fn current(&self, token: conn::Id<'d, KeyTag<ID>>) -> Current<'a, 'd, ID> {
        match self.slot.phase.get() {
            slot::Phase::Vacant => Current::Stale,
            slot::Phase::Handshaking(bound) if bound.token == token => Current::Handshaking,
            slot::Phase::Ready { bound, .. } if bound.token != token => Current::Stale,
            slot::Phase::Ready {
                access: State::Shared(_),
                ..
            } => Current::Shared,
            slot::Phase::Ready {
                access: State::Reserved(value),
                ..
            } => Current::Reserved(Reservation {
                slot: self.slot,
                id: Transaction {
                    conn: token,
                    generation: value,
                },
            }),
            slot::Phase::Ready {
                access: State::Held(value),
                ..
            } => Current::Held(Transaction {
                conn: token,
                generation: value,
            }),
            slot::Phase::Ready {
                access: State::Finalizing(value),
                ..
            } => Current::Finalizing(Finalizing {
                slot: self.slot,
                id: Transaction {
                    conn: token,
                    generation: value,
                },
            }),
            slot::Phase::Ready {
                access: State::Draining,
                ..
            } => Current::Draining,
            slot::Phase::Closing { bound, .. } if bound.token == token => Current::Closing,
            slot::Phase::Handshaking(_) | slot::Phase::Closing { .. } => Current::Stale,
        }
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
        std::mem::size_of::<Current<'static, 'static, 0>>()
            == std::mem::size_of::<Finalizing<'static, 'static, 0>>()
                + std::mem::align_of::<Finalizing<'static, 'static, 0>>()
    );
    assert!(std::mem::size_of::<Access<'static, 'static, 0>>() == std::mem::size_of::<&()>());
};
