use crate::runtime;
use std::mem;

#[repr(transparent)]
pub(in crate::runtime) struct Release(runtime::raw::Memory);

impl Release {
    pub(in crate::runtime) const fn new(memory: runtime::raw::Memory) -> Self {
        Self(memory)
    }

    pub(crate) fn run(self) {}
}

const _: () = assert!(mem::size_of::<Release>() == mem::size_of::<runtime::raw::Memory>());
