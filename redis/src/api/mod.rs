//! Storage-preserving operations issued by one Redis runtime session.

use crate::value::source;
use dope_fiber::abi::Fiber;

mod client;
pub mod decode;
mod sealed;
mod storage;

pub use client::{Acquired, Client, Commands};
pub(crate) use sealed::Sealed;
pub use storage::Storage;

/// An exclusive connection lease; each operation keeps its mutable borrow.
pub trait Connection<'d, const ID: u8 = 0>: Sealed {
    type Error: source::Error<Bytes = Self::Bytes>;
    type Bytes: source::Bytes;

    fn ping(&mut self) -> impl Fiber<'d, Output = Result<(), Self::Error>> + '_;
    fn client_id(&mut self) -> impl Fiber<'d, Output = Result<i64, Self::Error>> + '_;
    fn client_name(
        &mut self,
    ) -> impl Fiber<'d, Output = Result<Option<Self::Bytes>, Self::Error>> + '_;
}
