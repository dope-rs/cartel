use std::num::NonZero;

use dope::core::driver::route::table::conn::Capacity;
use o3::buffer::{
    pool,
    pool::{owners, state},
};

use crate::{port::backend::wire::Payload, protocol::messages::Fe};

pub(crate) struct Storage {
    pool: pool::Pool<state::Initialized, owners::Plain<pool::Width<16>>>,
    slots: NonZero<usize>,
}

impl Storage {
    pub(in crate::port) fn try_new(capacity: Capacity) -> Result<Self, pool::CreateError> {
        let slots = capacity.nonzero();
        let layout = pool::Width::new(slots.get()).map_err(pool::CreateError::Layout)?;
        let pool = pool::Pool::try_from(layout).map_err(pool::CreateError::Exhausted)?;
        Ok(Self { pool, slots })
    }

    pub(in crate::port) const fn capacity(&self) -> NonZero<usize> {
        self.slots
    }

    pub(in crate::port) fn acquire(&self, pid: i32, secret: &[u8; 4]) -> Option<Payload> {
        self.pool
            .copy([
                &16u32.to_be_bytes(),
                &Fe::CANCEL_REQUEST.to_be_bytes(),
                &pid.to_be_bytes(),
                secret,
            ])
            .map(Payload)
    }
}
