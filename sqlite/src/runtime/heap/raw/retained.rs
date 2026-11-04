use crate::runtime::raw::Memory;
use std::ptr;

pub(crate) struct Retained(ptr::NonNull<super::Arena>);

impl Retained {
    pub(super) const fn new(arena: ptr::NonNull<super::Arena>) -> Self {
        Self(arena)
    }

    pub(in crate::runtime) fn arm(self, memory: Memory) {
        // SAFETY: Retained carries the exact live arena pointer until this handoff.
        let arena = unsafe { self.0.as_ref() };
        let memory = arena.arm(memory);
        // The arena borrow ends before its backing is reclaimed.
        drop(memory);
    }
}

const _: () = assert!(size_of::<Retained>() == size_of::<usize>());
