use std::num;

use dope::{core::driver::route::KeyTag, manifold::client::conn};
use o3::depot::{self, index::Initialized};

use crate::port::pool::{phases, slot};

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
    slot: &'a slot::Slot<'d, ID>,
    bound: slot::Bound<'d, ID>,
    generation: Generation,
}

impl<'d, const ID: u8> Reservation<'_, 'd, ID> {
    pub(crate) const fn id(&self) -> Transaction<'d, ID> {
        Transaction {
            conn: self.bound.token,
            generation: self.generation,
        }
    }

    pub(crate) fn activate(self) -> Transaction<'d, ID> {
        self.slot.phase.set(slot::Phase::Ready {
            bound: self.bound,
            access: State::Held(self.generation),
        });
        self.id()
    }
}

/// One finalizing state selected in the current synchronous protocol turn.
pub(crate) struct Finalizing<'a, 'd, const ID: u8> {
    slot: &'a slot::Slot<'d, ID>,
    bound: slot::Bound<'d, ID>,
    generation: Generation,
}

impl<'d, const ID: u8> Finalizing<'_, 'd, ID> {
    pub(crate) const fn id(&self) -> Transaction<'d, ID> {
        Transaction {
            conn: self.bound.token,
            generation: self.generation,
        }
    }

    pub(crate) fn finish(self) -> Transaction<'d, ID> {
        self.slot.phase.set(slot::Phase::Ready {
            bound: self.bound,
            access: State::Shared(self.generation.sequence()),
        });
        self.id()
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum State {
    Shared(Sequence),
    Reserved(Generation),
    Held(Generation),
    Finalizing(Generation),
    Draining,
}

pub(crate) enum Reserve<'a, 'd, const ID: u8> {
    Reserved(Reservation<'a, 'd, ID>),
    Exhausted,
    Rejected,
}

pub(crate) enum Release {
    Released,
    Stale,
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
    Reserved(Transaction<'d, ID>),
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
        let state = if FINAL {
            State::Finalizing(value)
        } else {
            State::Held(value)
        };
        matches!(self.slot.phase.get(), slot::Phase::Ready { access, .. } if access == state)
    }

    pub(crate) fn listed(
        proof: depot::raw::Proof<&'a slot::Slot<'d, ID>, Initialized<slot::Bound<'d, ID>>>,
    ) -> conn::Id<'d, KeyTag<ID>> {
        phases::State::listed(proof).token
    }

    pub(crate) fn reserve(&self, token: conn::Id<'d, KeyTag<ID>>) -> Reserve<'a, 'd, ID> {
        let slot::Phase::Ready {
            bound,
            access: State::Shared(seq),
        } = self.slot.phase.get()
        else {
            return Reserve::Rejected;
        };
        if bound.token != token {
            return Reserve::Rejected;
        }
        let Some(next) = seq.next() else {
            self.slot.phase.set(slot::Phase::Ready {
                bound,
                access: State::Draining,
            });
            return Reserve::Exhausted;
        };
        self.slot.phase.set(slot::Phase::Ready {
            bound,
            access: State::Reserved(next),
        });
        Reserve::Reserved(Reservation {
            slot: self.slot,
            bound,
            generation: next,
        })
    }

    pub(crate) fn release(&self, proof: Reservation<'_, 'd, ID>) -> Release {
        let id = proof.id();
        let slot::Phase::Ready { bound, access } = self.slot.phase.get() else {
            return Release::Stale;
        };
        if bound.token != id.conn || access != State::Reserved(id.generation) {
            return Release::Stale;
        }
        self.slot.phase.set(slot::Phase::Ready {
            bound,
            access: State::Shared(id.generation.sequence()),
        });
        Release::Released
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
        self.slot.phase.set(slot::Phase::Ready {
            bound,
            access: State::Finalizing(id.generation),
        });
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
            } => Current::Reserved(Transaction {
                conn: token,
                generation: value,
            }),
            slot::Phase::Ready {
                access: State::Held(value),
                ..
            } => Current::Held(Transaction {
                conn: token,
                generation: value,
            }),
            slot::Phase::Ready {
                bound,
                access: State::Finalizing(value),
            } => Current::Finalizing(Finalizing {
                slot: self.slot,
                bound,
                generation: value,
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
            == std::mem::size_of::<(&(), slot::Bound<'static, 0>, Generation)>()
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
