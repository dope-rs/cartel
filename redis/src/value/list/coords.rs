use crate::value::list::Lease;
use crate::value::source::Coordinates as _;
use crate::{
    client::GeoCoord,
    protocol::parser::{frame, storage},
    value::{self, reply, scalar, source},
};
use std::{iter, marker};

/// Optional coordinates retaining one validated frame, parser tape, and credits.
pub struct Coordinates<'s, 'arena, 'credits, 'region>(
    Lease<'s, 'arena, 'credits, 'region, storage::Coordinate>,
);

impl source::Sealed for Coordinates<'_, '_, '_, '_> {}

impl<'s, 'arena, 'credits, 'region> source::Coordinates
    for Coordinates<'s, 'arena, 'credits, 'region>
{
    type Iter<'a>
        = Iter<'a, 's, 'arena, 'credits, 'region>
    where
        Self: 'a;

    fn len(&self) -> usize {
        self.0.len()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn iter(&self) -> Self::Iter<'_> {
        Iter(self.0.stored.iter(), marker::PhantomData)
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
            Ok(Self(Lease {
                stored: frame.store(nodes),
                keep,
            }))
        })
    }
}

pub struct Iter<'a, 's, 'arena, 'credits, 'region>(
    frame::Iter<'a, 's, 'region, storage::Coordinate>,
    marker::PhantomData<(&'arena (), &'credits ())>,
);

impl Iterator for Iter<'_, '_, '_, '_, '_> {
    type Item = Option<GeoCoord>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.0.next()?.map(|[lng, lat]| GeoCoord::new(lng, lat)))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.0.remaining() as usize;
        (len, Some(len))
    }
}

impl ExactSizeIterator for Iter<'_, '_, '_, '_, '_> {}
impl iter::FusedIterator for Iter<'_, '_, '_, '_, '_> {}

impl<'a, 's, 'arena, 'credits, 'region> IntoIterator
    for &'a Coordinates<'s, 'arena, 'credits, 'region>
{
    type Item = Option<GeoCoord>;
    type IntoIter = Iter<'a, 's, 'arena, 'credits, 'region>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
