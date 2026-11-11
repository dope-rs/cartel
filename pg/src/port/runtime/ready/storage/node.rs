use crate::protocol::runtime::ready::storage;
use o3::depot::queue::links;
use std::cell;

#[repr(C)]
pub(in crate::port::runtime::ready::storage) struct Node {
    pub(in crate::port::runtime::ready::storage) round: links::Hook,
    pub(in crate::port::runtime::ready::storage) bucket: links::Hook,
    pub(in crate::port::runtime::ready::storage) priority: cell::Cell<Option<storage::Rank>>,
}

impl Node {
    pub(in crate::port::runtime::ready::storage) fn new(index: links::Index) -> Self {
        Self {
            round: links::Hook::new(index),
            bucket: links::Hook::new(index),
            priority: cell::Cell::new(None),
        }
    }
}

const _: () = {
    assert!(std::mem::size_of::<Node>() == 5 * std::mem::size_of::<u32>());
    assert!(std::mem::align_of::<Node>() == std::mem::align_of::<u32>());
};
