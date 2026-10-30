use crate::{
    api::{stmt, work},
    catalogs, runtime,
};
use std::{marker, mem, ptr, slice};

pub(super) const ALIGNMENT: usize = 64;

#[repr(C, align(64))]
struct Unit([mem::MaybeUninit<u8>; ALIGNMENT]);

pub(crate) struct Storage<I: catalogs::Catalog> {
    units: ptr::NonNull<Unit>,
    items: usize,
    marker: marker::PhantomData<I>,
}

impl<I: catalogs::Catalog> Storage<I> {
    pub(crate) fn new(span: runtime::raw::Span) -> Result<Self, crate::runtime::Error> {
        let items = I::STATEMENTS
            .len()
            .checked_add(stmt::Control::ALL.len())
            .ok_or(crate::runtime::Error::Layout)?;
        let bytes = stmt::bytes(items).ok_or(crate::runtime::Error::Layout)?;
        let (base, capacity) = span.into_parts();
        if capacity < bytes || !base.as_ptr().addr().is_multiple_of(ALIGNMENT) {
            return Err(crate::runtime::Error::Layout);
        }
        let storage = Self {
            units: base.cast(),
            items,
            marker: marker::PhantomData,
        };
        let pointer = storage.units.as_ptr().cast::<stmt::Slot<'_>>();
        for index in 0..items {
            unsafe { pointer.add(index).write(stmt::Slot::EMPTY) };
        }
        Ok(storage)
    }

    pub(super) fn slots<'connection>(
        &'connection mut self,
    ) -> &'connection mut [stmt::Slot<'connection>] {
        let pointer = self.units.as_ptr().cast::<stmt::Slot<'connection>>();
        unsafe { slice::from_raw_parts_mut(pointer, self.items) }
    }

    /// Reclaims statements left behind by a forgotten exclusive catalog owner.
    pub(crate) fn retire_all<'a>(&'a mut self, work: &'a work::Work) {
        for slot in self.slots() {
            if let Some(prepared) = slot.statement.as_mut() {
                prepared.work = work;
            }
            slot.clear();
        }
    }
}

const _: () = {
    assert!(mem::align_of::<stmt::Slot<'_>>() <= ALIGNMENT);
    assert!(mem::size_of::<Storage<catalogs::Empty>>() == 2 * mem::size_of::<usize>());
};
