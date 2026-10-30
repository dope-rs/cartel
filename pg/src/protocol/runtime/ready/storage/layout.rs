use std::{alloc, mem, num};

use dope::core::driver::route::table::conn;
use o3::depot::{index::bitmap, queue::links};

use crate::port::{pool::slot, ready};
use crate::protocol::runtime::ready::storage;

#[derive(Clone, Copy)]
pub(in crate::protocol::runtime::ready) struct Layout {
    pub(in crate::protocol::runtime::ready) allocation: alloc::Layout,
    pub(in crate::protocol::runtime::ready) nodes: usize,
    pub(in crate::protocol::runtime::ready) heads: usize,
    pub(in crate::protocol::runtime::ready) header: usize,
    pub(in crate::protocol::runtime::ready) shape: bitmap::Shape<num::NonZero<u32>>,
}

impl Layout {
    pub(in crate::protocol::runtime::ready) fn try_new(
        capacity: conn::Capacity,
        buckets: num::NonZero<u32>,
    ) -> Result<Self, ready::Error> {
        let shape = bitmap::Shape::<num::NonZero<u32>>::new(buckets);
        let values =
            Self::array::<slot::Slot<'_, 0>>(capacity.entries().get(), ready::Part::Values)?;
        let nodes = Self::array::<storage::Node>(capacity.entries().get(), ready::Part::Nodes)?;
        let heads = Self::array::<links::List>(buckets.get() as usize, ready::Part::Heads)?;
        let words = Self::array::<bitmap::Word>(shape.words(), ready::Part::Words)?;
        let (layout, nodes) = Self::join(values, nodes, ready::Part::Nodes)?;
        let (layout, heads) = Self::join(layout, heads, ready::Part::Heads)?;
        let (layout, header) = Self::join(
            layout,
            alloc::Layout::new::<storage::Header>(),
            ready::Part::Header,
        )?;
        let (layout, _) = Self::join(layout, words, ready::Part::Words)?;
        Ok(Self {
            allocation: layout.pad_to_align(),
            nodes,
            heads,
            header,
            shape,
        })
    }

    fn array<T>(len: usize, part: ready::Part) -> Result<alloc::Layout, ready::Error> {
        alloc::Layout::array::<T>(len).map_err(|source| ready::Error::Array { part, len, source })
    }

    fn join(
        left: alloc::Layout,
        right: alloc::Layout,
        part: ready::Part,
    ) -> Result<(alloc::Layout, usize), ready::Error> {
        left.extend(right)
            .map_err(|source| ready::Error::Join { part, source })
    }
}

const _: () = {
    assert!(mem::align_of::<storage::Header>() == mem::align_of::<bitmap::Word>());
    assert!(mem::size_of::<storage::Header>().is_multiple_of(mem::align_of::<bitmap::Word>()));
};
