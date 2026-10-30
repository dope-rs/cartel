use crate::{row, runtime};
use std::{marker, mem, ops, result, slice};

pub(crate) struct Arena<'owner> {
    storage: &'owner mut runtime::raw::Span,
}

impl<'owner> Arena<'owner> {
    pub(crate) const fn from_span(storage: &'owner mut runtime::raw::Span) -> Self {
        Self { storage }
    }

    pub(crate) fn retain<T: Copy>(
        &mut self,
        rows: usize,
    ) -> result::Result<row::raw::Retainer<'_, T>, crate::Error> {
        row::raw::Retainer::new(self.storage, rows)
    }
}

const _: () = assert!(mem::size_of::<Arena<'static>>() == mem::size_of::<usize>());

/// A fixed-arena collection whose row views borrow the issuing local runtime.
pub struct Rows<'arena, R: row::raw::Decode + 'arena> {
    rows: &'arena [R::Borrowed<'arena>],
    row: marker::PhantomData<fn() -> R>,
}

impl<'arena, R: row::raw::Decode + 'arena> Rows<'arena, R> {
    pub(crate) const fn new(rows: &'arena [R::Borrowed<'arena>]) -> Self {
        Self {
            rows,
            row: marker::PhantomData,
        }
    }

    pub fn as_slice(&self) -> &'arena [R::Borrowed<'arena>] {
        self.rows
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn iter(&self) -> slice::Iter<'arena, R::Borrowed<'arena>> {
        self.rows.iter()
    }
}

impl<'arena, R: row::raw::Decode + 'arena> ops::Deref for Rows<'arena, R> {
    type Target = [R::Borrowed<'arena>];

    fn deref(&self) -> &Self::Target {
        self.rows
    }
}
