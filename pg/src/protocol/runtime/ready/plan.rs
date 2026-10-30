use std::{mem, num};

use crate::port::ready;
use crate::protocol::runtime::ready::storage;
use cartel_core::reply::layout;
use dope::core::driver::route::table::conn;

#[derive(Clone, Copy)]
pub(crate) struct Plan {
    capacity: conn::Capacity,
    buckets: num::NonZero<u32>,
    block: storage::Layout,
}

impl Plan {
    /// With K shared credits, M eligible lanes carry at most 2K + 3M load: one reserved order
    /// per lane and only its current Sync may outlive all orders. Clipping at K + 3 keeps the minimum.
    /// The validated reply layout bounds the K + 1 order limit by u32::MAX / 2.
    pub(crate) fn new(
        capacity: conn::Capacity,
        replies: layout::Layout,
    ) -> Result<Self, ready::Error> {
        let buckets = if capacity.entries().get() == 1 {
            num::NonZero::<u32>::MIN
        } else {
            // SAFETY: Layout admission bounds order_limit by u32::MAX / 2.
            unsafe { num::NonZero::new_unchecked(replies.order_limit().get() + 3) }
        };
        let block = storage::Layout::try_new(capacity, buckets)?;
        Ok(Self {
            capacity,
            buckets,
            block,
        })
    }

    pub(super) const fn capacity(self) -> conn::Capacity {
        self.capacity
    }

    pub(super) const fn buckets(self) -> num::NonZero<u32> {
        self.buckets
    }

    pub(crate) const fn bytes(self) -> usize {
        self.block.allocation.size()
    }

    pub(super) const fn block(self) -> storage::Layout {
        self.block
    }
}

const _: () =
    assert!(mem::size_of::<Plan>() == 5 * mem::size_of::<usize>() + 4 * mem::size_of::<u32>());
