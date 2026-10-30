use o3::buffer::{self, hold, storage::Shared, storage::range};
use std::{cell::Cell, ops};

/// Rejects its first selection without changing the source or issuing a range proof.
pub struct Cursor {
    source: Shared,
    reject: Cell<bool>,
}

impl Cursor {
    pub fn new(source: Shared) -> Self {
        Self {
            source,
            reject: Cell::new(true),
        }
    }
}

impl AsRef<[u8]> for Cursor {
    fn as_ref(&self) -> &[u8] {
        self.source.as_slice()
    }
}

impl buffer::Source for Cursor {
    type View<'input> = <Shared as buffer::Source>::View<'input>;

    fn remaining(&self) -> usize {
        self.source.len()
    }
}

impl buffer::Cursor for Cursor {
    fn try_get<'input>(
        &'input self,
        range: ops::Range<usize>,
    ) -> Result<Self::View<'input>, range::Error> {
        if self.reject.replace(false) {
            return Err(range::Error::new(range, self.source.len()));
        }
        buffer::Cursor::try_get(&self.source, range)
    }
}

// SAFETY: Immutable Shared views preserve exact selected bytes; default retention copies them.
unsafe impl<'d> hold::raw::Retain<'d> for Cursor {}

// SAFETY: The adapter exposes precisely Shared's range and delegates its admitted consumption.
unsafe impl buffer::raw::Consume for Cursor {
    fn consume<const MAX: u32>(prefix: buffer::raw::Consumed<'_, '_, Self, MAX>) {
        // SAFETY: The inner Shared has the same unchanged range as this adapter.
        let prefix = unsafe { prefix.map(|source| &mut source.source) };
        <Shared as buffer::raw::Consume>::consume(prefix);
    }
}
