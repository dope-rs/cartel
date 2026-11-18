use cartel_core::reply::{self, usage};

use crate::{api::traits::sealed::Sealed, port::replies, protocol, schema};
use protocol::value::decode::RowReader;
use schema::Row as _;

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
    fn usage(&self) -> usage::Usage;
    fn into_owned<Q: schema::TypedQuery>(self) -> Result<Vec<Q::Row>, crate::Error>;
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

    #[inline]
    fn usage(&self) -> usage::Usage {
        self.usage()
    }

    fn into_owned<Q: schema::TypedQuery>(self) -> Result<Vec<Q::Row>, crate::Error> {
        self.try_map(|row, slot| {
            let mut reader = RowReader::new(row.wire(), Q::N_RESULT_COLS)?;
            let done = slot.insert(Q::Row::decode_owned(&mut reader)?);
            reader.finish()?;
            Ok(done)
        })
        .map_err(crate::Error::Allocation)?
    }
}
