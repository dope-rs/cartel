//! Borrowed request payloads written directly into the Redis wire frame.

pub mod encode;

use std::num;

use dope::net::link::egress::load;
use o3::buffer::write::consume;

/// Exact request representation or pool limit that rejected one frame.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum Error {
    Length { actual: usize, maximum: usize },
    Add { left: usize, right: usize },
    Mul { left: usize, right: usize },
    Capacity(consume::Error),
    Write(consume::Error),
    Entries { required: num::NonZero<usize> },
    BulkLength { declared: usize, actual: usize },
    Items { declared: usize, actual: usize },
    Incomplete { remaining: num::NonZero<usize> },
    Egress(load::Error),
}

/// A RESP bulk-string payload with an exact logical length.
///
/// `visit` must yield exactly `len` bytes in wire order.
pub trait Bulk {
    /// Returns the payload length without traversing its chunks.
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Visits the payload without joining its borrowed chunks.
    fn visit(&self, visit: impl FnMut(&[u8]));
}

impl<T> Bulk for T
where
    T: AsRef<[u8]> + ?Sized,
{
    fn len(&self) -> usize {
        self.as_ref().len()
    }

    fn visit(&self, mut visit: impl FnMut(&[u8])) {
        visit(self.as_ref());
    }
}

const _: () = assert!(std::mem::size_of::<Error>() == 3 * std::mem::size_of::<usize>());
