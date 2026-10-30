use std::ptr;

use o3::base::marker::ThreadBound;

pub(crate) struct Local {
    arena: ptr::NonNull<super::Arena>,
    _thread: ThreadBound,
}

impl Local {
    pub(crate) fn from_span(span: super::Span) -> (Self, super::Retained) {
        let (storage, base, layout) = span.into_parts();
        let arena = unsafe { super::Arena::new(base, layout) };
        let pointer = storage.cast::<super::Arena>();
        unsafe { pointer.as_ptr().write(arena) };
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
