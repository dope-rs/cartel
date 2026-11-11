use crate::api::traits::sealed::Sealed;
use crate::reply::Error;
use dope_fiber::abi::Fiber;
use std::ops::Deref;

/// COPY operations retaining their original client or transaction borrow.
pub trait Transfer<'d, const ID: u8 = 0>: Sealed + Sized {
    type Chunk: AsRef<[u8]> + Deref<Target = [u8]>;
    type Sink: Sink<'d, ID>;
    type Stream: Stream<'d, ID, Chunk = Self::Chunk>;

    /// Encodes both inputs before returning the future.
    fn copy_in(
        self,
        sql: &str,
        data: &[u8],
    ) -> impl Fiber<'d, Output = Result<(), Error<'d, ID>>> + use<'d, Self, ID>;

    fn copy_stream(self, sql: &str) -> Self::Sink;
    fn copy_out(self, sql: &str) -> Self::Stream;
}

/// One COPY IN operation; each write borrows its sink and input together.
pub trait Sink<'d, const ID: u8 = 0>: Sealed + Sized {
    fn write<'write>(
        &'write mut self,
        chunk: &'write [u8],
    ) -> impl Fiber<'d, Output = Result<(), Error<'d, ID>>> + 'write;

    fn cancel(self);
    fn finish(self) -> impl Fiber<'d, Output = Result<(), Error<'d, ID>>>;
}

/// A COPY OUT stream whose completed chunks retain their own storage owner.
pub trait Stream<'d, const ID: u8 = 0>: Sealed {
    type Chunk: AsRef<[u8]> + Deref<Target = [u8]>;

    fn next_chunk(
        &mut self,
    ) -> impl Fiber<'d, Output = Result<Option<Self::Chunk>, Error<'d, ID>>> + '_;
}
