use std::{mem, num};

#[derive(Clone, Copy, Default)]
#[doc(hidden)]
pub struct Registry {
    ltree: Option<num::NonZero<u32>>,
}

impl Registry {
    pub(super) const fn ltree(self) -> Option<num::NonZero<u32>> {
        self.ltree
    }

    pub(crate) fn with_ltree(self, ltree: num::NonZero<u32>) -> Self {
        Self { ltree: Some(ltree) }
    }
}

const _: () = assert!(mem::size_of::<Registry>() == mem::size_of::<u32>());
const _: () = assert!(mem::align_of::<Registry>() == mem::align_of::<u32>());
