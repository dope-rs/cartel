//! Raw backing authority for one externally retained SQLite heap region.

use crate::runtime::heap::layout;
use std::ptr;

pub(crate) struct Span {
    storage: ptr::NonNull<u8>,
    layout: layout::Layout,
}

impl Span {
    /// Binds a validated layout to its externally retained bytes.
    /// # Safety
    /// Storage is a 64-byte-aligned, uniquely writable layout.backing() region
    /// in the Memory handed to Local::from_span, retained until its last owner releases it.
    pub(crate) unsafe fn new(storage: ptr::NonNull<u8>, layout: layout::Layout) -> Self {
        Self { storage, layout }
    }

    pub(super) const fn into_parts(self) -> (ptr::NonNull<u8>, layout::Layout) {
        (self.storage, self.layout)
    }
}
