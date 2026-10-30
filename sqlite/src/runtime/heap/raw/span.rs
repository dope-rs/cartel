//! Raw backing authority for one externally retained SQLite heap region.

use crate::runtime::heap::layout;
use std::ptr;

#[repr(transparent)]
pub(super) struct ArenaBase(ptr::NonNull<u8>);

impl ArenaBase {
    /// Binds a non-null address whose alignment is proven by its backing owner.
    /// # Safety
    /// The pointer must retain arena alignment for its full allocation lifetime.
    pub(super) const unsafe fn new(pointer: ptr::NonNull<u8>) -> Self {
        Self(pointer)
    }

    pub(super) const fn pointer(&self) -> ptr::NonNull<u8> {
        self.0
    }
}

pub(crate) struct Span {
    arena: ptr::NonNull<u8>,
    base: ArenaBase,
    layout: layout::Layout,
}

impl Span {
    /// Binds a validated layout to its externally retained bytes.
    /// # Safety
    /// The region stays uniquely writable until the resulting local heap dies.
    pub(crate) unsafe fn new(base: ptr::NonNull<u8>, layout: layout::Layout) -> Self {
        let heap = unsafe {
            ArenaBase::new(ptr::NonNull::new_unchecked(
                base.as_ptr().add(super::Arena::BYTES),
            ))
        };
        Self {
            arena: base,
            base: heap,
            layout,
        }
    }

    pub(super) const fn into_parts(self) -> (ptr::NonNull<u8>, ArenaBase, layout::Layout) {
        (self.arena, self.base, self.layout)
    }
}
