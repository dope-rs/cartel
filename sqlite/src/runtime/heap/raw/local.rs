use std::ptr;

use crate::runtime::raw::Memory;
use o3::base::marker::ThreadBound;

pub(crate) struct Local {
    arena: ptr::NonNull<super::Arena>,
    _thread: ThreadBound,
}

impl Local {
    pub(crate) fn from_span(span: super::Span, memory: Memory) -> (Self, super::Retained) {
        let (storage, layout) = span.into_parts();
        let pointer = storage.cast::<super::Arena>();
        // SAFETY: Span carries this backing's unpublished aligned arena region.
        unsafe { super::Arena::initialize(pointer, layout, memory) };
        (
            Self {
                arena: pointer,
                _thread: ThreadBound::NEW,
            },
            super::Retained::new(pointer),
        )
    }

    pub(in crate::runtime::heap) fn owner(&self) -> super::Owner {
        super::Owner(self.arena)
    }
}
