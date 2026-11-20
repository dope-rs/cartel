use crate::port::ready::{Error, Part};
use std::alloc::Layout;

pub(in crate::port::runtime::ready) fn array<T>(len: usize, part: Part) -> Result<Layout, Error> {
    Layout::array::<T>(len).map_err(|source| Error::Array { part, len, source })
}

pub(super) fn append<T>(left: Layout, len: usize, part: Part) -> Result<(Layout, usize), Error> {
    let right = array::<T>(len, part)?;
    left.extend(right)
        .map_err(|source| Error::Join { part, source })
}
