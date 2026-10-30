use crate::runtime::heap;
use std::{cell, mem, ptr};

thread_local! {
    static CURRENT: cell::Cell<Option<heap::raw::Owner>> = const { cell::Cell::new(None) };
}

pub(crate) struct Conflict;

/// Owns this worker's exact destination for new native allocations.
/// Live allocations retain their original arena independently of this scope.
pub(crate) struct Scope {
    _heap: heap::raw::Local,
}

impl Scope {
    pub(crate) fn enter(heap: heap::raw::Local) -> Result<Self, (heap::raw::Local, Conflict)> {
        if CURRENT.get().is_some() {
            return Err((heap, Conflict));
        }
        CURRENT.set(Some(heap.owner()));
        Ok(Self { _heap: heap })
    }

    /// Rejects a new Core allocation when this worker has no admitted arena.
    pub(super) fn allocate(order: heap::raw::Order) -> *mut u8 {
        let Some(owner) = CURRENT.get() else {
            return ptr::null_mut();
        };
        owner.allocate(order)
    }
}

impl Drop for Scope {
    fn drop(&mut self) {
        CURRENT.set(None);
    }
}

const _: () = {
    let word = mem::size_of::<usize>();
    assert!(mem::size_of::<Option<heap::raw::Owner>>() == word);
    assert!(mem::size_of::<Scope>() == word);
};
