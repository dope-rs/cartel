use crate::runtime::release;
use std::ptr;

pub(crate) struct Retained(ptr::NonNull<super::Arena>);

impl Retained {
    pub(super) const fn new(arena: ptr::NonNull<super::Arena>) -> Self {
        Self(arena)
    }

    pub(in crate::runtime) fn arm(self, release: release::Release) {
        // SAFETY: Retained carries the exact live arena pointer until this handoff.
        let arena = unsafe { self.0.as_ref() };
        let release = arena.arm(release);
        if let Some(release) = release {
            release.run();
        }
    }
}

const _: () = assert!(size_of::<Retained>() == size_of::<usize>());
