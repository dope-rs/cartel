//! Retained diagnostics from one explicitly synchronized command group.

use std::{mem::MaybeUninit, slice};

use o3::{
    buffer::storage,
    depot::arena::pool::{self, recycle},
};

use crate::api::results::group::{self, Diagnostic, Position};

#[derive(Clone, Copy, Debug)]
pub(crate) struct Record {
    pub(crate) position: Position,
    pub(crate) start: usize,
    pub(crate) end: usize,
}

/// One exclusively borrowed backing with its published diagnostic prefix.
#[derive(Debug)]
pub(crate) struct Records<'s> {
    slots: &'s mut [MaybeUninit<Record>],
    len: usize,
    unknown: Option<usize>,
}

/// The admitted next record and its sole publication authority.
pub(crate) struct Vacant<'a> {
    slot: &'a mut MaybeUninit<Record>,
    len: &'a mut usize,
    unknown: &'a mut Option<usize>,
}

impl<'s> Records<'s> {
    pub(crate) fn new(slots: &'s mut [MaybeUninit<Record>]) -> Self {
        Self {
            slots,
            len: 0,
            unknown: None,
        }
    }

    pub(crate) fn clear(&mut self) {
        self.len = 0;
        self.unknown = None;
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn as_slice(&self) -> &[Record] {
        // SAFETY: only an admitted vacancy can extend this same backing's
        // published prefix, and it writes the complete record before extending.
        unsafe { slice::from_raw_parts(self.slots.as_ptr().cast(), self.len) }
    }

    pub(crate) fn vacancy(&mut self) -> Option<Vacant<'_>> {
        Some(Vacant {
            slot: self.slots.get_mut(self.len)?,
            len: &mut self.len,
            unknown: &mut self.unknown,
        })
    }

    pub(crate) fn resolve(&mut self, position: Position) -> Resolution {
        let Some(index) = self.unknown.take() else {
            return Resolution::Unchanged;
        };
        // SAFETY: commit alone records this index, after writing this exact
        // backing's next record. No operation removes records before clear,
        // which clears the unknown position along with the published length.
        unsafe { self.slots.get_unchecked_mut(index).assume_init_mut() }.position = position;
        Resolution::Resolved
    }
}

impl Vacant<'_> {
    pub(crate) fn commit(self, record: Record) {
        self.slot.write(record);
        if record.position == Position::Unknown {
            *self.unknown = Some(*self.len);
        }
        *self.len += 1;
    }
}

const _: () = assert!(size_of::<Records<'static>>() == 5 * size_of::<usize>());

pub(crate) enum Resolution {
    Resolved,
    Unchanged,
}

const _: () = assert!(size_of::<Record>() == 4 * size_of::<usize>());

/// An immutable snapshot of the diagnostics observed before group completion.
///
/// Its storage remains independent of the query and transaction borrow. A
/// timed-out group's later protocol drain cannot change this snapshot.
pub struct Ledger<'r, 's> {
    pub(crate) inner: Option<Retained<'r, 's>>,
}

pub(crate) struct Retained<'r, 's> {
    pub(crate) records: pool::Unique<'r, Records<'s>, recycle::Header<Records<'s>>>,
    pub(crate) bytes: storage::Shared,
}

impl Ledger<'_, '_> {
    pub(crate) const fn empty() -> Self {
        Self { inner: None }
    }
}

pub struct Iter<'a> {
    pub(crate) records: slice::Iter<'a, Record>,
    pub(crate) bytes: &'a [u8],
}

impl group::Diagnostics for Ledger<'_, '_> {
    type Iter<'a>
        = Iter<'a>
    where
        Self: 'a;

    fn iter(&self) -> Self::Iter<'_> {
        match &self.inner {
            None => Iter {
                records: [].iter(),
                bytes: &[],
            },
            Some(retained) => {
                let records = retained.records.get().get_ref();
                Iter {
                    records: records.as_slice().iter(),
                    bytes: retained.bytes.as_slice(),
                }
            }
        }
    }

    fn len(&self) -> usize {
        self.inner
            .as_ref()
            .map_or(0, |retained| retained.records.get().get_ref().len())
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = Diagnostic<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let record = *self.records.next()?;
        Some(Diagnostic {
            position: record.position,
            fields: super::raw::group_fields(self.bytes, record),
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.records.size_hint()
    }
}

impl ExactSizeIterator for Iter<'_> {}
