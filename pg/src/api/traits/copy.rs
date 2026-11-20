use crate::api::traits::sealed::Sealed;
use crate::reply::Error;
use dope_fiber::abi::Fiber;
use std::ops::Deref;

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
