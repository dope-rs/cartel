use cartel_core::reply::{self, usage};
use o3::buffer::bytes::bounded;

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
    type Iter<'rows>: Iterator<Item = protocol::Wire<'rows>>
    where
        Self: 'rows;

    fn iter(&self) -> Self::Iter<'_>;
    fn usage(&self) -> usage::Usage;
    fn try_drain<E>(
        &mut self,
        visit: impl FnMut(protocol::Wire<'_>) -> Result<(), E>,
    ) -> Result<(), E>;
}

struct Frames<'a>(&'a [u8]);

impl<'a> Iterator for Frames<'a> {
    type Item = protocol::Wire<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let (header, tail) = self.0.split_first_chunk::<5>()?;
        let length = protocol::frame::declared(header).wrapping_sub(4);
        // SAFETY: Complete::collected admitted this entire frame in the same immutable
        // run. Its body is within tail and inherits the run's u32 extent.
        let row = unsafe {
            let (body, next) = tail.split_at_unchecked(length as usize);
            self.0 = next;
            bounded::raw::Input::new(body)
        };
        Some(protocol::Wire::from_raw(row))
    }
}

pub struct Rows<'a, 'd> {
    groups: reply::BatchIter<'a, replies::Row<'d>>,
    frames: Frames<'a>,
}

impl<'a, 'd> Iterator for Rows<'a, 'd> {
    type Item = protocol::Wire<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.find_map(Some)
    }

    fn find_map<B, F: FnMut(Self::Item) -> Option<B>>(&mut self, mut find: F) -> Option<B> {
        loop {
            if let Some(found) = self.frames.find_map(&mut find) {
                return Some(found);
            }
            self.frames = Frames(self.groups.next()?.as_slice());
        }
    }
}

impl<'d> Batch for reply::Batch<'_, '_, '_, 'd, replies::Row<'d>> {
    type Iter<'rows>
        = Rows<'rows, 'd>
    where
        Self: 'rows;

    fn iter(&self) -> Self::Iter<'_> {
        Rows {
            groups: self.iter(),
            frames: Frames(&[]),
        }
    }

    #[inline]
    fn usage(&self) -> usage::Usage {
        self.usage()
    }

    fn try_drain<E>(
        &mut self,
        mut visit: impl FnMut(protocol::Wire<'_>) -> Result<(), E>,
    ) -> Result<(), E> {
        self.try_drain(|group| Frames(group.as_slice()).try_for_each(&mut visit))
    }
}
