use super::{Policy, RoundRobin, storage};
use crate::port::{
    pool::slot,
    ready::{Error, Part},
};
use cartel_core::reply::layout;
use dope::core::driver::route::table::conn;
use std::alloc::Layout;
use storage::Order;

pub(crate) struct Plan<P: Policy = RoundRobin> {
    pub(super) capacity: conn::Capacity,
    pub(super) allocation: Layout,
    pub(super) offsets: <P::Order as Order>::Layout,
}

impl<P: Policy> Copy for Plan<P> {}

impl<P: Policy> Clone for Plan<P> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<P: Policy> Plan<P> {
    pub(crate) fn new(capacity: conn::Capacity, replies: layout::Layout) -> Result<Self, Error> {
        let slots = capacity.entries().get();
        let values = storage::layout::array::<slot::Slot<'_, 0>>(slots, Part::Values)?;
        let (allocation, offsets) = P::Order::layout(values, slots, replies)?;
        Ok(Self {
            capacity,
            allocation: allocation.pad_to_align(),
            offsets,
        })
    }

    pub(crate) const fn bytes(self) -> usize {
        self.allocation.size()
    }
}
