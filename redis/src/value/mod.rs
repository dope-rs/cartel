use std::mem;

use crate::{Error, api::Storage, client, protocol::parser};
use cartel_core::reply::retain;
use list::{bulks, coords, pair};
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

    pub(crate) fn try_pair<const ID: u8>(
        self,
    ) -> Result<
        (
            Bytes<'arena, 'credits, 'region>,
            Bytes<'arena, 'credits, 'region>,
        ),
        reply::Failure<Error<'region, ID>, Bytes<'arena, 'credits, 'region>>,
    > {
        wire::Wire::with(self, |wire| {
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
            let [first, second] = wire.nodes.pair(batch);
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

const _: () = assert!(
    mem::size_of::<Reply<'static, 'static, 'static, 'static>>() == 8 * mem::size_of::<usize>()
);

impl<'app, 's, 'b, 'c, 'd: 'app, B: ?Sized, const ID: u8> Storage
    for client::Redis<'app, 's, 'b, 'c, 'd, B, ID>
{
    type DriverError = Error<'d, ID>;
    type Bytes = Bytes<'b, 'c, 'd>;
    type Error = reply::Failure<Error<'d, ID>, Self::Bytes>;
    type Bulks = bulks::Array<'s, 'b, 'c, 'd>;
    type Optional = bulks::Optional<'s, 'b, 'c, 'd>;
    type Pairs = pair::BulkPairs<'s, 'b, 'c, 'd>;
    type Scan = list::Scan<'s, 'b, 'c, 'd>;
    type Reply = Reply<'s, 'b, 'c, 'd>;
    type Text = pair::ScoredText<'s, 'b, 'c, 'd>;
    type Scored = pair::ScoredBulks<'s, 'b, 'c, 'd>;
    type Coordinates = coords::Coordinates<'s, 'b, 'c, 'd>;

    fn unit(reply: Self::Reply) -> Result<(), Self::Error> {
        wire::Wire::discard(reply).map_err(reply::Failure::Server)
    }

    fn signed(reply: Self::Reply) -> Result<i64, Self::Error> {
        wire::Wire::decode(reply, |wire| wire::Wire::signed(wire))
    }

    fn unsigned(reply: Self::Reply) -> Result<u64, Self::Error> {
        wire::Wire::decode(reply, |wire| wire::Wire::unsigned(wire))
    }

    fn boolean(reply: Self::Reply) -> Result<bool, Self::Error> {
        wire::Wire::decode(reply, |wire| wire::Wire::boolean(wire))
    }

    fn float(reply: Self::Reply) -> Result<f64, Self::Error> {
        wire::Wire::decode(reply, |wire| wire::Wire::float(wire))
    }

    fn optional_u64(reply: Self::Reply) -> Result<Option<u64>, Self::Error> {
        wire::Wire::decode(reply, |wire| wire::Wire::opt_unsigned(wire))
    }

    fn optional_f64(reply: Self::Reply) -> Result<Option<f64>, Self::Error> {
        wire::Wire::decode(reply, |wire| wire::Wire::opt_float(wire))
    }

    fn coordinate(reply: Self::Reply) -> Result<client::GeoCoord, Self::Error> {
        wire::Wire::decode(reply, |wire| wire::Wire::coordinate(wire))
    }

    fn maybe_bytes(reply: Self::Reply) -> Result<Option<Self::Bytes>, Self::Error> {
        wire::Wire::with(reply, |wire| {
            let value = match wire.try_next() {
                Ok(value) => value,
                Err(error) => return Err(wire.failure(error)),
            };
            let span = match value {
                parser::Value::Nil => None,
                parser::Value::Bulk(span) => Some(span),
                actual => {
                    return Err(
                        wire.failure(Fault::expected(reply::Expectation::BulkOrNil, actual))
                    );
                }
            };
            Ok(span.map(|span| wire.project(span)))
        })
    }

    fn bulks(reply: Self::Reply) -> Result<Self::Bulks, Self::Error> {
        bulks::Array::try_decode::<ID>(reply)
    }

    fn optional(reply: Self::Reply) -> Result<Self::Optional, Self::Error> {
        bulks::Optional::try_decode::<ID>(reply)
    }

    fn pairs(reply: Self::Reply) -> Result<Self::Pairs, Self::Error> {
        pair::BulkPairs::try_decode::<ID>(reply)
    }

    fn scan(reply: Self::Reply) -> Result<Self::Scan, Self::Error> {
        list::Scan::try_decode::<ID>(reply)
    }

    fn scored(reply: Self::Reply) -> Result<Self::Scored, Self::Error> {
        pair::ScoredBulks::try_decode::<ID>(reply)
    }

    fn coordinates(reply: Self::Reply) -> Result<Self::Coordinates, Self::Error> {
        coords::Coordinates::try_decode::<ID>(reply)
    }

    fn bytes<const TYPE_NAME: bool>(reply: Self::Reply) -> Result<Self::Bytes, Self::Error> {
        Bytes::try_decode::<TYPE_NAME, ID>(reply)
    }
}
