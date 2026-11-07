use crate::value::source::Coordinates as _;
use crate::{
    client::GeoCoord,
    protocol::parser::storage,
    value::{self, reply, scalar, source},
};
use cartel_core::reply::retain;

pub use crate::protocol::parser::storage::CoordinateIter as Iter;

/// Optional coordinates retaining a validated parser tape and its value credits.
pub struct Coordinates<'s, 'arena, 'credits, 'region> {
    coordinates: storage::Region<'s, storage::Coordinate>,
    _values: Option<retain::Values<'arena, 'credits, 'region>>,
}

impl source::Sealed for Coordinates<'_, '_, '_, '_> {}

impl<'s, 'arena, 'credits, 'region> source::Coordinates
    for Coordinates<'s, 'arena, 'credits, 'region>
{
    type Iter<'a>
        = Iter<'a, 's, 'arena, 'credits, 'region>
    where
        Self: 'a;

    fn len(&self) -> usize {
        self.coordinates.len()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn iter(&self) -> Self::Iter<'_> {
        Iter::new(&self.coordinates)
    }
}

impl<'s, 'arena, 'credits, 'region, const ID: u8> value::Decode<'s, 'arena, 'credits, 'region, ID>
    for Coordinates<'s, 'arena, 'credits, 'region>
{
    type Output = Self;

    fn decode(
        reply: value::Reply<'s, 'arena, 'credits, 'region>,
    ) -> Result<
        Self,
        reply::Failure<crate::Error<'region, ID>, value::Bytes<'arena, 'credits, 'region>>,
    > {
        let (stored, keep) = reply.into_parts();
        stored.with(|frame, nodes| {
            let nodes = match nodes.coordinates(|value| scalar::float(value, &frame)) {
                Ok(nodes) => nodes,
                Err(error) => {
                    let fault = match error {
                        storage::CoordError::Array(error) => {
                            reply::Fault::mismatch(reply::Expectation::Array, error)
                        }
                        storage::CoordError::Member(error) => {
                            reply::Fault::mismatch(reply::Expectation::ArrayOrNil, error)
                        }
                        storage::CoordError::Length { actual } => reply::Fault::ArrayLength {
                            expected: 2,
                            actual,
                        },
                        storage::CoordError::Number(error) => error,
                    };
                    return Err(fault.into_reply(frame, keep));
                }
            };
            drop(frame);
            Ok(Self {
                coordinates: nodes,
                _values: keep.into_parts().1,
            })
        })
    }
}

impl<'a, 's, 'arena, 'credits, 'region> IntoIterator
    for &'a Coordinates<'s, 'arena, 'credits, 'region>
{
    type Item = Option<GeoCoord>;
    type IntoIter = Iter<'a, 's, 'arena, 'credits, 'region>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
