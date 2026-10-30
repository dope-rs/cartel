use crate::protocol::parser::{self, frame, storage};
use crate::value::{self, reply, scalar};
use cartel_core::reply::retain;
use reply::Fault;
use std::mem;

pub mod bulks;
pub mod coords;
pub mod pair;
mod scan;

pub use scan::Scan;

struct Lease<'s, 'arena, 'credits, 'region, K> {
    stored: frame::Stored<'s, 'region, storage::Region<'s, K>>,
    keep: retain::Held<'arena, 'credits, 'region>,
}

impl<'s, 'arena, 'credits, 'region> Lease<'s, 'arena, 'credits, 'region, storage::Raw> {
    fn top<const ID: u8>(
        reply: value::Reply<'s, 'arena, 'credits, 'region>,
    ) -> Result<
        Self,
        reply::Failure<crate::Error<'region, ID>, value::Bytes<'arena, 'credits, 'region>>,
    > {
        let (stored, keep) = reply.into_parts();
        stored.with(|frame, nodes| {
            let array = match nodes.array(nodes.root()) {
                Ok(array) => array,
                Err(error) => {
                    return Err(
                        Fault::mismatch(reply::Expectation::Array, error).into_reply(frame, keep)
                    );
                }
            };
            Ok(Self {
                stored: frame.store(nodes.region(array)),
                keep,
            })
        })
    }

    fn scan<const ID: u8>(
        reply: value::Reply<'s, 'arena, 'credits, 'region>,
    ) -> Result<
        (u64, Self),
        reply::Failure<crate::Error<'region, ID>, value::Bytes<'arena, 'credits, 'region>>,
    > {
        let (stored, keep) = reply.into_parts();
        stored.with(|frame, nodes| {
            let outer = match nodes.array(nodes.root()) {
                Ok(array) => array,
                Err(error) => {
                    return Err(
                        Fault::mismatch(reply::Expectation::Array, error).into_reply(frame, keep)
                    );
                }
            };
            let batch = outer
                .exact::<2>()
                .map_err(|error| reply::Decode::ArrayLength {
                    expected: 2,
                    actual: error.actual,
                })?;
            let (first, second) = nodes.scan(batch);
            let offset = (|| {
                let value = first.map_err(Fault::Server)?;
                let parser::Value::Bulk(span) = value else {
                    return Err(Fault::expected(reply::Expectation::ScanCursor, value));
                };
                match scalar::Scalar::new(frame.slice(span)).unsigned() {
                    Ok(value) => Ok(value),
                    Err(scalar::Error::Negative(value)) => Err(Fault::NegativeCursor(value)),
                    Err(scalar::Error::Invalid(reason)) => Err(Fault::Integer { span, reason }),
                }
            })();
            let offset = match offset {
                Ok(offset) => offset,
                Err(error) => return Err(error.into_reply(frame, keep)),
            };
            let keys = match second {
                Ok(array) => array,
                Err(error) => {
                    return Err(
                        Fault::mismatch(reply::Expectation::Array, error).into_reply(frame, keep)
                    );
                }
            };
            Ok((
                offset,
                Self {
                    stored: frame.store(nodes.region(keys)),
                    keep,
                },
            ))
        })
    }

    fn bulks<const W: usize, const ID: u8>(
        self,
    ) -> Result<
        Lease<'s, 'arena, 'credits, 'region, storage::Bulk<W>>,
        reply::Failure<crate::Error<'region, ID>, value::Bytes<'arena, 'credits, 'region>>,
    > {
        let Self { stored, keep } = self;
        stored.with(|frame, nodes| {
            let nodes = match nodes.bulks::<W>() {
                Ok(nodes) => nodes,
                Err(storage::Group::Length { width, actual }) => {
                    return Err(reply::Decode::ArrayMultiple { width, actual }.into());
                }
                Err(storage::Group::Mismatch(error)) => {
                    return Err(
                        Fault::mismatch(reply::Expectation::Bulk, error).into_reply(frame, keep)
                    );
                }
            };
            Ok(Lease {
                stored: frame.store(nodes),
                keep,
            })
        })
    }

    fn optional<const ID: u8>(
        self,
    ) -> Result<
        Lease<'s, 'arena, 'credits, 'region, storage::Maybe>,
        reply::Failure<crate::Error<'region, ID>, value::Bytes<'arena, 'credits, 'region>>,
    > {
        let Self { stored, keep } = self;
        stored.with(|frame, nodes| {
            let nodes = match nodes.optional() {
                Ok(nodes) => nodes,
                Err(error) => {
                    return Err(Fault::mismatch(reply::Expectation::BulkOrNil, error)
                        .into_reply(frame, keep));
                }
            };
            Ok(Lease {
                stored: frame.store(nodes),
                keep,
            })
        })
    }

    fn scored<const ID: u8>(
        self,
    ) -> Result<
        Lease<'s, 'arena, 'credits, 'region, storage::Scored>,
        reply::Failure<crate::Error<'region, ID>, value::Bytes<'arena, 'credits, 'region>>,
    > {
        let Self { stored, keep } = self;
        stored.with(|frame, nodes| {
            let nodes = match nodes.scored(|value| scalar::float(value, &frame)) {
                Ok(nodes) => nodes,
                Err(storage::Score::Length { width, actual }) => {
                    return Err(reply::Decode::ArrayMultiple { width, actual }.into());
                }
                Err(storage::Score::Server(span)) => {
                    return Err(Fault::Server(span).into_reply(frame, keep));
                }
                Err(storage::Score::Member(actual)) => {
                    return Err(
                        Fault::expected(reply::Expectation::Bulk, actual).into_reply(frame, keep)
                    );
                }
                Err(storage::Score::Number(error)) => return Err(error.into_reply(frame, keep)),
            };
            Ok(Lease {
                stored: frame.store(nodes),
                keep,
            })
        })
    }
}

impl<K> Lease<'_, '_, '_, '_, K> {
    fn len(&self) -> usize {
        self.stored.len()
    }
}

const _: [(); 10 * mem::size_of::<usize>()] =
    [(); mem::size_of::<Lease<'_, '_, '_, '_, storage::Bulk<1>>>()];
