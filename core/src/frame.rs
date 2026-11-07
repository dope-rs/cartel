use dope::net::link::egress::data;
use o3::buffer::{Residency, pool};
use std::mem;
use zeroize::Zeroize as _;

pub enum Sensitivity {
    Plain,
    Secret,
}

impl Sensitivity {
    pub fn clear(&self, bytes: &mut [u8]) {
        if matches!(self, Self::Secret) {
            bytes.zeroize();
        }
    }
}

/// A completed request whose initialized bytes retain their clearing policy.
pub struct Buffer {
    data: pool::Span,
    kind: Sensitivity,
}

impl Buffer {
    pub const fn new(data: pool::Span, kind: Sensitivity) -> Self {
        Self { data, kind }
    }
}

impl AsRef<[u8]> for Buffer {
    fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        self.kind.clear(self.data.as_mut_slice());
    }
}

impl Residency for Buffer {
    fn resident(&self) -> usize {
        self.data.capacity().get() as usize
    }
}

// SAFETY: shared borrows fix the entire pool slice and its capacity; secret
// clearing requires the final exclusive owner after all slice borrows end.
unsafe impl<'d> data::raw::Contiguous<'d> for Buffer {}

const _: () = {
    assert!(mem::size_of::<Buffer>() == mem::size_of::<pool::Span>() + mem::size_of::<usize>());
    assert!(mem::align_of::<Buffer>() == mem::align_of::<pool::Span>());
};
