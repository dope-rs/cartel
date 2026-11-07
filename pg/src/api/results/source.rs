use cartel_core::reply::{self, usage};
use o3::alloc::array;

use crate::{api::traits::sealed::Sealed, port::replies, protocol};

/// Immutable bytes retained by their original storage and accounting owner.
/// Only this crate's owners implement the trait; repeated borrows expose the
/// same bytes and extent for the owner's complete lifetime.
pub trait Payload: Sealed {
    fn wire(&self) -> protocol::Wire<'_>;

    fn as_slice(&self) -> &[u8] {
        self.wire().as_slice()
    }
}

/// One immutable batch and the retirement owner that keeps its entries alive.
/// The sealed implementations retain the same ordered rows through every borrow.
pub trait Batch: Sealed {
    type Item: Payload;
    type Iter<'rows>: Iterator<Item = &'rows Self::Item>
    where
        Self: 'rows;

    fn iter(&self) -> Self::Iter<'_>;
    fn len(&self) -> reply::Length;

    fn is_empty(&self) -> bool {
        self.len() == reply::Length::ZERO
    }
    fn payload_len(&self) -> usage::Bytes;
    fn try_map<T>(
        self,
        make: impl for<'slot> FnMut(
            &Self::Item,
            array::Slot<'slot, T>,
        ) -> Result<array::Written<'slot>, crate::Error>,
    ) -> Result<Vec<T>, crate::Error>;
}

impl<'d> Batch for reply::Batch<'_, '_, '_, 'd, replies::Row<'d>> {
    type Item = replies::Row<'d>;
    type Iter<'rows>
        = reply::BatchIter<'rows, Self::Item>
    where
        Self: 'rows;

    fn iter(&self) -> Self::Iter<'_> {
        self.iter()
    }

    fn len(&self) -> reply::Length {
        self.len()
    }

    fn payload_len(&self) -> usage::Bytes {
        self.usage().bytes()
    }

    fn try_map<T>(
        self,
        make: impl for<'slot> FnMut(
            &Self::Item,
            array::Slot<'slot, T>,
        ) -> Result<array::Written<'slot>, crate::Error>,
    ) -> Result<Vec<T>, crate::Error> {
        self.try_map(make).map_err(crate::Error::Allocation)?
    }
}
