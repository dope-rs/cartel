use std::{cell::Cell, mem::MaybeUninit};

use o3::depot::{self, index::Initialized};

use crate::port::{
    Disposition,
    pool::{access, slot, slot::Phase},
    replies,
};

#[derive(Clone, Copy)]
enum Lifecycle {
    Vacant,
    Handshaking,
    Ready(access::State),
    Closing {
        failure: replies::Failure,
        disposition: Disposition,
    },
}

/// Initializes binding storage before publishing any live lifecycle state.
/// Deactivation preserves that Copy binding until its next epoch replaces it.
pub(super) struct State<'d, const ID: u8> {
    bound: Cell<MaybeUninit<slot::Bound<'d, ID>>>,
    phase: Cell<Lifecycle>,
}

impl<'d, const ID: u8> State<'d, ID> {
    pub(super) fn new() -> Self {
        Self {
            bound: Cell::new(MaybeUninit::uninit()),
            phase: Cell::new(Lifecycle::Vacant),
        }
    }

    pub(super) fn get(&self) -> Phase<'d, ID> {
        let phase = self.phase.get();
        if matches!(phase, Lifecycle::Vacant) {
            return Phase::Vacant;
        }
        // SAFETY: set writes the binding before publishing any live variant.
        let bound = unsafe { self.bound.get().assume_init() };
        match phase {
            Lifecycle::Vacant => Phase::Vacant,
            Lifecycle::Handshaking => Phase::Handshaking(bound),
            Lifecycle::Ready(access) => Phase::Ready { bound, access },
            Lifecycle::Closing {
                failure,
                disposition: fate,
            } => Phase::Closing {
                bound,
                failure,
                disposition: fate,
            },
        }
    }

    pub(super) fn set(&self, phase: Phase<'d, ID>) {
        let (bound, phase) = match phase {
            Phase::Vacant => {
                self.phase.set(Lifecycle::Vacant);
                return;
            }
            Phase::Handshaking(bound) => (bound, Lifecycle::Handshaking),
            Phase::Ready { bound, access } => (bound, Lifecycle::Ready(access)),
            Phase::Closing {
                bound,
                failure,
                disposition: fate,
            } => (
                bound,
                Lifecycle::Closing {
                    failure,
                    disposition: fate,
                },
            ),
        };
        self.bound.set(MaybeUninit::new(bound));
        self.phase.set(phase);
    }
    pub(super) fn listed(
        proof: depot::raw::Proof<&slot::Slot<'d, ID>, Initialized<slot::Bound<'d, ID>>>,
    ) -> slot::Bound<'d, ID> {
        // SAFETY: ready storage publishes a selector member only after bind
        // initialized this exact slot. Its membership proof is retained for
        // this synchronous selection; no generation transition intervenes.
        unsafe { proof.into_inner().phase.bound.get().assume_init() }
    }
}
