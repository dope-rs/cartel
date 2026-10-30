use dope::core::driver::route::KeyTag;
use std::num::NonZero;

use dope::manifold::client::{conn, side, side::registry};

use crate::port::{self, pool::backend};

use crate::port::Disposition;

/// The observed connection state which prevented a cancellation request.
pub enum Inactive<'d, const ID: u8> {
    Vacant,
    Handshaking(conn::Id<'d, KeyTag<ID>>),
    Closing {
        target: conn::Id<'d, KeyTag<ID>>,
        cause: crate::Error,
        disposition: Disposition,
    },
}

/// Borrows the exact cancellation failure while its original owners remain held.
pub enum Kind<'a, 'd, const ID: u8> {
    Capacity {
        target: conn::Id<'d, KeyTag<ID>>,
        slots: NonZero<usize>,
    },
    Inactive {
        target: conn::Id<'d, KeyTag<ID>>,
        observed: &'a Inactive<'d, ID>,
    },
    Epoch {
        target: conn::Id<'d, KeyTag<ID>>,
        observed: conn::Id<'d, KeyTag<ID>>,
    },
    Reserve {
        target: conn::Id<'d, KeyTag<ID>>,
        cause: &'a registry::Rejected<'d, port::Backend<'d, ID>, KeyTag<ID>>,
    },
    Delivery {
        target: conn::Id<'d, KeyTag<ID>>,
        cause: &'a side::Error,
    },
}

pub struct Error<'tx, 'd: 'tx, const ID: u8 = 0> {
    pub(super) failure: Failure<'tx, 'd, ID>,
}

pub(super) enum Failure<'tx, 'd: 'tx, const ID: u8> {
    Backend(backend::Rejected<'tx, 'd, ID>),
    Reserve {
        backend: backend::Bound<'tx, 'd, ID>,
        cause: registry::Rejected<'d, port::Backend<'d, ID>, KeyTag<ID>>,
    },
    Delivery {
        backend: backend::Bound<'tx, 'd, ID>,
        cause: side::Error,
    },
}

impl<'tx, 'd: 'tx, const ID: u8> Error<'tx, 'd, ID> {
    pub fn kind(&self) -> Kind<'_, 'd, ID> {
        match &self.failure {
            Failure::Backend(rejected) => match rejected {
                backend::Rejected::Capacity { owner, slots } => Kind::Capacity {
                    target: owner.target(),
                    slots: *slots,
                },
                backend::Rejected::Inactive { owner, observed } => Kind::Inactive {
                    target: owner.target(),
                    observed,
                },
                backend::Rejected::Epoch { owner, observed } => Kind::Epoch {
                    target: owner.target(),
                    observed: *observed,
                },
            },
            Failure::Reserve { backend, cause } => Kind::Reserve {
                target: backend.target(),
                cause,
            },
            Failure::Delivery { backend, cause } => Kind::Delivery {
                target: backend.target(),
                cause,
            },
        }
    }
}
