use crate::runtime::heap;
use std::{cell, mem, ptr};

thread_local! {
    static CURRENT: cell::Cell<Option<heap::raw::Owner>> = const { cell::Cell::new(None) };
}

/// Owns this worker's exact destination for new native allocations.
/// Live allocations retain their original arena independently of this scope.
pub(crate) struct Scope {
    _heap: heap::raw::Local,
}

impl Scope {
    #[inline]
    pub(crate) fn enter(heap: heap::raw::Local) -> Result<Self, crate::Error> {
        if CURRENT.get().is_some() {
            return Err(crate::Error::LocalAllocatorOccupied);
        }
        CURRENT.set(Some(heap.owner()));
        Ok(Self { _heap: heap })
    }

    pub(crate) fn finish(self) {
        CURRENT.set(None);
    }

    /// Rejects a new Core allocation when this worker has no admitted arena.
    pub(super) fn allocate(order: heap::raw::Order) -> *mut u8 {
        let Some(owner) = CURRENT.get() else {
            return ptr::null_mut();
        };
        owner.allocate(order)
    }
}

const _: () = {
    let word = mem::size_of::<usize>();
    assert!(mem::size_of::<Option<heap::raw::Owner>>() == word);
    assert!(mem::size_of::<Scope>() == word);
};
