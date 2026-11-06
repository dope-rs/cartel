use std::ptr;

pub(crate) struct Retained(ptr::NonNull<super::Arena>);

impl Retained {
    pub(super) const fn new(arena: ptr::NonNull<super::Arena>) -> Self {
        Self(arena)
    }
}

impl Drop for Retained {
    fn drop(&mut self) {
        let memory = super::Owner(self.0).with_state(|_, state| state.release());
        // All arena and allocator-state borrows end before reclaiming the backing.
        drop(memory);
    }
}

const _: () = assert!(size_of::<Retained>() == size_of::<usize>());
