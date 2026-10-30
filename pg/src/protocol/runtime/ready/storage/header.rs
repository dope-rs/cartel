use std::num;

use o3::depot::index::bitmap;

#[repr(C)]
pub(in crate::protocol::runtime::ready::storage) struct Header {
    pub(super) shape: bitmap::Shape<num::NonZero<u32>>,
    align: [usize; 0],
}

impl Header {
    pub(super) const fn new(shape: bitmap::Shape<num::NonZero<u32>>) -> Self {
        Self { shape, align: [] }
    }
}
