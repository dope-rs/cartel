use crate::api::work;
use std::mem;
use work::{Depth, depth::Count};

/// Consumes one live release against the unchanged count of its original source.
pub(crate) struct Release<'source> {
    source: &'source Depth,
    current: Count,
}

impl<'source> Release<'source> {
    /// # Safety
    /// The caller owns a counted savepoint or cleanup debt from this exact source.
    /// Its depth stays unchanged until this receipt is consumed or abandoned.
    pub(crate) unsafe fn new(source: &'source Depth) -> Self {
        // SAFETY: the retained source owner proves its current count is nonzero.
        let current = unsafe { *source.0.get().as_slice().get_unchecked(0) };
        Self { source, current }
    }

    pub(crate) const fn depth(&self) -> Count {
        self.current
    }

    pub(crate) fn close(self) {
        self.source.0.set(self.current.prior().nonzero());
    }
}

const _: () = assert!(mem::size_of::<Release<'_>>() == 2 * mem::size_of::<usize>());
