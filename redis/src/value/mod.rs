use std::mem;

use crate::{client, protocol::parser};
use cartel_core::reply::retain;
use parser::frame;
use reply::Fault;

mod bytes;
pub mod list;
pub mod reply;
mod scalar;
pub mod source;
mod wire;

pub use bytes::Bytes;

/// One validated RESP reply retaining exactly one frame and parser tape.
pub struct Reply<'s, 'arena, 'credits, 'region> {
    stored: frame::Stored<'s, 'region>,
    keep: retain::Held<'arena, 'credits, 'region>,
}

impl<'s, 'arena, 'credits, 'region> Reply<'s, 'arena, 'credits, 'region> {
    pub(crate) fn new(
        scanned: frame::Stored<'s, 'region>,
        keep: retain::Held<'arena, 'credits, 'region>,
    ) -> Self {
        Self {
            stored: scanned,
            keep,
        }
    }

    fn into_parts(
        self,
    ) -> (
        frame::Stored<'s, 'region>,
        retain::Held<'arena, 'credits, 'region>,
    ) {
        (self.stored, self.keep)
    }
}

impl source::Sealed for Reply<'_, '_, '_, '_> {}

impl source::Reply for Reply<'_, '_, '_, '_> {
    fn reader(&self) -> impl source::Reader<'_> {
        self.stored.reader()
    }
}

/// A copyable borrowed event in the pre-order RESP tape.
#[derive(Clone, Copy, Debug)]
pub enum Event<'a> {
    Nil,
    Ok,
    Status(&'a [u8]),
    Integer(i64),
    Bulk(&'a [u8]),
    Array { len: usize },
    Error(&'a [u8]),
}

/// Converts one retained RESP reply into a concrete result type.
/// The reply keeps frame, tape, and credits after dispatch bookkeeping, including server errors.
pub trait Decode<'s, 'arena, 'credits, 'region, const ID: u8 = 0> {
    type Output;

    fn decode(
        reply: Reply<'s, 'arena, 'credits, 'region>,
    ) -> Result<
        Self::Output,
        reply::Failure<crate::Error<'region, ID>, Bytes<'arena, 'credits, 'region>>,
    >;
}

impl<'s, 'arena, 'credits, 'region, const ID: u8> Decode<'s, 'arena, 'credits, 'region, ID>
    for Reply<'s, 'arena, 'credits, 'region>
{
    type Output = Self;

    fn decode(
        reply: Reply<'s, 'arena, 'credits, 'region>,
    ) -> Result<Self, reply::Failure<crate::Error<'region, ID>, Bytes<'arena, 'credits, 'region>>>
    {
        Ok(reply)
    }
}

const _: () = assert!(
    mem::size_of::<Reply<'static, 'static, 'static, 'static>>() == 8 * mem::size_of::<usize>()
);

impl<'s, 'arena, 'credits, 'region, const ID: u8> Decode<'s, 'arena, 'credits, 'region, ID> for () {
    type Output = Self;

    fn decode(
        reply: Reply<'s, 'arena, 'credits, 'region>,
    ) -> Result<Self, reply::Failure<crate::Error<'region, ID>, Bytes<'arena, 'credits, 'region>>>
    {
        wire::Wire::discard(reply).map_err(reply::Failure::Server)
    }
}

impl<'s, 'arena, 'credits, 'region, const ID: u8> Decode<'s, 'arena, 'credits, 'region, ID>
    for i64
{
    type Output = Self;

    fn decode(
        reply: Reply<'s, 'arena, 'credits, 'region>,
    ) -> Result<Self, reply::Failure<crate::Error<'region, ID>, Bytes<'arena, 'credits, 'region>>>
    {
        wire::Wire::decode(reply, |wire, batch| wire::Wire::signed(wire, batch))
    }
}

impl<'s, 'arena, 'credits, 'region, const ID: u8> Decode<'s, 'arena, 'credits, 'region, ID>
    for u64
{
    type Output = Self;

    fn decode(
        reply: Reply<'s, 'arena, 'credits, 'region>,
    ) -> Result<Self, reply::Failure<crate::Error<'region, ID>, Bytes<'arena, 'credits, 'region>>>
    {
        wire::Wire::decode(reply, |wire, batch| wire::Wire::unsigned(wire, batch))
    }
}

impl<'s, 'arena, 'credits, 'region, const ID: u8> Decode<'s, 'arena, 'credits, 'region, ID>
    for f64
{
    type Output = Self;

    fn decode(
        reply: Reply<'s, 'arena, 'credits, 'region>,
    ) -> Result<Self, reply::Failure<crate::Error<'region, ID>, Bytes<'arena, 'credits, 'region>>>
    {
        wire::Wire::decode(reply, |wire, batch| wire::Wire::float(wire, batch))
    }
}

impl<'s, 'arena, 'credits, 'region, const ID: u8> Decode<'s, 'arena, 'credits, 'region, ID>
    for client::GeoCoord
{
    type Output = Self;

    fn decode(
        reply: Reply<'s, 'arena, 'credits, 'region>,
    ) -> Result<Self, reply::Failure<crate::Error<'region, ID>, Bytes<'arena, 'credits, 'region>>>
    {
        wire::Wire::decode(reply, |wire, batch| wire::Wire::coordinate(wire, batch))
    }
}

impl<'s, 'arena, 'credits, 'region, const ID: u8> Decode<'s, 'arena, 'credits, 'region, ID>
    for bool
{
    type Output = Self;

    fn decode(
        reply: Reply<'s, 'arena, 'credits, 'region>,
    ) -> Result<Self, reply::Failure<crate::Error<'region, ID>, Bytes<'arena, 'credits, 'region>>>
    {
        wire::Wire::decode(reply, |wire, batch| wire::Wire::boolean(wire, batch))
    }
}

impl<'s, 'arena, 'credits, 'region, const ID: u8> Decode<'s, 'arena, 'credits, 'region, ID>
    for Bytes<'arena, 'credits, 'region>
{
    type Output = Self;

    fn decode(
        reply: Reply<'s, 'arena, 'credits, 'region>,
    ) -> Result<Self, reply::Failure<crate::Error<'region, ID>, Bytes<'arena, 'credits, 'region>>>
    {
        Bytes::try_decode::<false, ID>(reply)
    }
}

impl<'s, 'arena, 'credits, 'region, const ID: u8> Decode<'s, 'arena, 'credits, 'region, ID>
    for Option<Bytes<'arena, 'credits, 'region>>
{
    type Output = Self;

    fn decode(
        reply: Reply<'s, 'arena, 'credits, 'region>,
    ) -> Result<Self, reply::Failure<crate::Error<'region, ID>, Bytes<'arena, 'credits, 'region>>>
    {
        wire::Wire::with(reply, |wire| {
            let read = match wire.try_next(wire.nodes.root()) {
                Ok(read) => read,
                Err(error) => return Err(wire.failure(error)),
            };
            let span = match read.value {
                parser::Value::Nil => None,
                parser::Value::Bulk(span) => Some(span),
                actual => {
                    return Err(
                        wire.failure(Fault::expected(reply::Expectation::BulkOrNil, actual))
                    );
                }
            };
            let wire = wire.finish(read.cursor)?;
            Ok(span.map(|span| wire.project(span)))
        })
    }
}

impl<'s, 'arena, 'credits, 'region, const ID: u8> Decode<'s, 'arena, 'credits, 'region, ID>
    for (
        Bytes<'arena, 'credits, 'region>,
        Bytes<'arena, 'credits, 'region>,
    )
{
    type Output = Self;

    fn decode(
        reply: Reply<'s, 'arena, 'credits, 'region>,
    ) -> Result<Self, reply::Failure<crate::Error<'region, ID>, Bytes<'arena, 'credits, 'region>>>
    {
        wire::Wire::with(reply, |wire| {
            let array = match wire.nodes.array(wire.nodes.root()) {
                Ok(array) => array,
                Err(error) => {
                    return Err(wire.failure(Fault::mismatch(reply::Expectation::Array, error)));
                }
            };
            let batch = array
                .exact::<2>()
                .map_err(|error| reply::Decode::ArrayLength {
                    expected: 2,
                    actual: error.actual,
                })?;
            let read = wire.nodes.pair(batch);
            let [first, second] = read.value;
            let bulk = |value| match value {
                Ok(parser::Value::Bulk(span)) => Ok(span),
                Ok(actual) => Err(Fault::expected(reply::Expectation::Bulk, actual)),
                Err(span) => Err(Fault::Server(span)),
            };
            let first = match bulk(first) {
                Ok(span) => span,
                Err(error) => return Err(wire.failure(error)),
            };
            let second = match bulk(second) {
                Ok(span) => span,
                Err(error) => return Err(wire.failure(error)),
            };
            let wire = wire.finish(read.cursor)?;
            let frame = wire
                .frame
                .try_clone()
                .map_err(crate::Error::ResponseRetain)?;
            let (credit, _) = wire.keep.into_parts();
            let cloned = credit
                .as_ref()
                .map(retain::Bytes::try_clone)
                .transpose()
                .map_err(crate::Error::ResponseRetain)?;
            Ok((
                Bytes::new(wire.frame.project(first), credit),
                Bytes::new(frame.project(second), cloned),
            ))
        })
    }
}

impl<'s, 'arena, 'credits, 'region, const ID: u8> Decode<'s, 'arena, 'credits, 'region, ID>
    for Option<f64>
{
    type Output = Self;

    fn decode(
        reply: Reply<'s, 'arena, 'credits, 'region>,
    ) -> Result<Self, reply::Failure<crate::Error<'region, ID>, Bytes<'arena, 'credits, 'region>>>
    {
        wire::Wire::decode(reply, |wire, batch| wire::Wire::opt_float(wire, batch))
    }
}

impl<'s, 'arena, 'credits, 'region, const ID: u8> Decode<'s, 'arena, 'credits, 'region, ID>
    for Option<u64>
{
    type Output = Self;

    fn decode(
        reply: Reply<'s, 'arena, 'credits, 'region>,
    ) -> Result<Self, reply::Failure<crate::Error<'region, ID>, Bytes<'arena, 'credits, 'region>>>
    {
        wire::Wire::decode(reply, |wire, batch| wire::Wire::opt_unsigned(wire, batch))
    }
}
