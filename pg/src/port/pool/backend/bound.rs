use std::mem;

use dope::{core::driver::route::KeyTag, manifold::client::conn};

use crate::{
    api::tx::control::error::Inactive,
    port,
    port::{
        Storage,
        pool::{backend::Rejected, key, slot},
    },
};

#[derive(Clone, Copy)]
pub(crate) struct Bound<'a, 'd, const ID: u8> {
    pub(super) slot: &'a slot::Slot<'d, ID>,
    pub(super) target: conn::Id<'d, KeyTag<ID>>,
}

impl<'a, 'd, const ID: u8> Bound<'a, 'd, ID> {
    pub(crate) fn target(self) -> conn::Id<'d, KeyTag<ID>> {
        self.target
    }

    /// Uses the key admitted for this exact live epoch, without user callbacks.
    /// Restoring its owner discards only the known zero placeholder; retirement
    /// still drops and zeroizes the live key immediately.
    pub(crate) fn try_current(
        self,
        pool: &Storage,
    ) -> Result<port::Backend<'d, ID>, Rejected<'a, 'd, ID>> {
        let bound = match self.slot.phase.get() {
            slot::Phase::Ready { bound, .. } => bound,
            slot::Phase::Vacant => {
                return Err(Rejected::Inactive {
                    owner: self,
                    observed: Inactive::Vacant,
                });
            }
            slot::Phase::Handshaking(bound) => {
                return Err(Rejected::Inactive {
                    owner: self,
                    observed: Inactive::Handshaking(bound.token),
                });
            }
            slot::Phase::Closing {
                bound,
                failure,
                disposition: fate,
            } => {
                return Err(Rejected::Inactive {
                    owner: self,
                    observed: Inactive::Closing {
                        target: bound.token,
                        cause: failure.into_error(),
                        disposition: fate,
                    },
                });
            }
        };
        if bound.token != self.target {
            return Err(Rejected::Epoch {
                owner: self,
                observed: bound.token,
            });
        }
        let key = self.slot.key.replace(key::Key::empty());
        let intent = port::Backend::try_new(pool, self.target, &key);
        mem::forget(self.slot.key.replace(key));
        match intent {
            Some(intent) => Ok(intent),
            None => Err(Rejected::Capacity {
                owner: self,
                slots: pool.capacity(),
            }),
        }
    }
}

const _: () = {
    assert!(
        std::mem::size_of::<Bound<'static, 'static, 0>>()
            == std::mem::size_of::<(&(), conn::Id<'static, KeyTag<0>>)>()
    );
};
