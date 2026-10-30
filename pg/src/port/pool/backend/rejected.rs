use std::num::NonZero;

use dope::{core::driver::route::KeyTag, manifold::client::conn};

use crate::{api::tx::control::error::Inactive, port::pool::backend::Bound};

pub(crate) enum Rejected<'a, 'd, const ID: u8> {
    Capacity {
        owner: Bound<'a, 'd, ID>,
        slots: NonZero<usize>,
    },
    Inactive {
        owner: Bound<'a, 'd, ID>,
        observed: Inactive<'d, ID>,
    },
    Epoch {
        owner: Bound<'a, 'd, ID>,
        observed: conn::Id<'d, KeyTag<ID>>,
    },
}
