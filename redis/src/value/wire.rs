use std::num;

use cartel_core::reply::retain;

use crate::client;
use crate::protocol::{self, parser, parser::frame, parser::storage};
use crate::value::{self, reply, scalar};
use reply::Fault;

pub(super) struct Wire<'id, 's, 'arena, 'credits, 'region> {
    pub(super) frame: frame::Owner<'id, 'region>,
    pub(super) nodes: frame::Nodes<'id, 's, storage::Tape<'s>>,
    pub(super) keep: retain::Held<'arena, 'credits, 'region>,
}

impl<'s, 'arena, 'credits, 'region> Wire<'_, 's, 'arena, 'credits, 'region> {
    pub(super) fn with<T>(
        reply: value::Reply<'s, 'arena, 'credits, 'region>,
        run: impl for<'id> FnOnce(Wire<'id, 's, 'arena, 'credits, 'region>) -> T,
    ) -> T {
        let (stored, keep) = reply.into_parts();
        stored.with(|frame, nodes| run(Wire { frame, nodes, keep }))
    }

    pub(super) fn decode<T, const ID: u8>(
        reply: value::Reply<'s, 'arena, 'credits, 'region>,
        decode: impl for<'id> FnOnce(
            &Wire<'id, 's, 'arena, 'credits, 'region>,
            frame::Batch<'id, 1>,
        ) -> Result<frame::Read<'id, T>, Fault<'id>>,
    ) -> Result<T, reply::Failure<crate::Error<'region, ID>, value::Bytes<'arena, 'credits, 'region>>>
    {
        Self::with(reply, |wire| {
            let read = match decode(&wire, wire.nodes.root()) {
                Ok(read) => read,
                Err(error) => return Err(wire.failure(error)),
            };
            wire.finish(read.cursor)?;
            Ok(read.value)
        })
    }

    /// Completion already proves that this tape contains exactly one RESP root.
    pub(super) fn discard(
        reply: value::Reply<'s, 'arena, 'credits, 'region>,
    ) -> Result<(), value::Bytes<'arena, 'credits, 'region>> {
        Self::with(reply, |wire| {
            let mut cursor = wire.nodes.cursor();
            while let Some(batch) = cursor.batch::<1>() {
                cursor = match wire.nodes.next(batch) {
                    Ok(read) => read.cursor,
                    Err(span) => return Err(wire.project(span)),
                };
            }
            Ok(())
        })
    }
}

impl<'id, 's, 'arena, 'credits, 'region> Wire<'id, 's, 'arena, 'credits, 'region> {
    pub(super) fn failure<const ID: u8>(
        self,
        error: Fault<'id>,
    ) -> reply::Failure<crate::Error<'region, ID>, value::Bytes<'arena, 'credits, 'region>> {
        error.into_reply(self.frame, self.keep)
    }

    pub(super) fn project(
        self,
        span: frame::Selection<'id>,
    ) -> value::Bytes<'arena, 'credits, 'region> {
        let (bytes, _) = self.keep.into_parts();
        value::Bytes::new(self.frame.project(span), bytes)
    }

    pub(super) fn try_next(
        &self,
        cursor: frame::Batch<'id, 1>,
    ) -> Result<frame::Read<'id, parser::Value<frame::Selection<'id>>>, Fault<'id>> {
        self.nodes.next(cursor).map_err(Fault::Server)
    }

    pub(super) fn boolean(
        wire: &Self,
        cursor: frame::Batch<'id, 1>,
    ) -> Result<frame::Read<'id, bool>, Fault<'id>> {
        let read = wire.try_next(cursor)?;
        let value = match read.value {
            parser::Value::Integer(value) => value != 0,
            parser::Value::Ok => true,
            parser::Value::Nil => false,
            parser::Value::Status(span) if wire.frame.slice(span) == b"OK" => true,
            actual => return Err(Fault::expected(reply::Expectation::Boolean, actual)),
        };
        Ok(frame::Read {
            value,
            cursor: read.cursor,
        })
    }

    pub(super) fn opt_float(
        wire: &Self,
        cursor: frame::Batch<'id, 1>,
    ) -> Result<frame::Read<'id, Option<f64>>, Fault<'id>> {
        let read = wire.try_next(cursor)?;
        let value = match read.value {
            parser::Value::Nil => None,
            value @ (parser::Value::Bulk(_) | parser::Value::Status(_)) => {
                Some(scalar::float(value, &wire.frame)?)
            }
            parser::Value::Integer(value) => Some(value as f64),
            actual => return Err(Fault::expected(reply::Expectation::FloatOrNil, actual)),
        };
        Ok(frame::Read {
            value,
            cursor: read.cursor,
        })
    }

    pub(super) fn opt_unsigned(
        wire: &Self,
        cursor: frame::Batch<'id, 1>,
    ) -> Result<frame::Read<'id, Option<u64>>, Fault<'id>> {
        let read = wire.try_next(cursor)?;
        let value = match read.value {
            parser::Value::Nil => None,
            value @ (parser::Value::Integer(_)
            | parser::Value::Bulk(_)
            | parser::Value::Status(_)) => Some(wire.to_u64(value)?),
            actual => return Err(Fault::expected(reply::Expectation::IntegerOrNil, actual)),
        };
        Ok(frame::Read {
            value,
            cursor: read.cursor,
        })
    }

    pub(super) fn finish<const ID: u8>(
        self,
        cursor: frame::Cursor<'id>,
    ) -> Result<Self, crate::Error<'region, ID>> {
        if let Some(left) = num::NonZero::new(cursor.remaining()) {
            return Err(crate::Error::Protocol(protocol::Error::Values {
                remaining: left,
            }));
        }
        Ok(self)
    }

    pub(super) fn signed(
        wire: &Self,
        cursor: frame::Batch<'id, 1>,
    ) -> Result<frame::Read<'id, i64>, Fault<'id>> {
        let read = wire.try_next(cursor)?;
        let value = match read.value {
            parser::Value::Integer(value) => value,
            parser::Value::Bulk(span) | parser::Value::Status(span) => {
                match scalar::Scalar::new(wire.frame.slice(span)).signed() {
                    Ok(value) => value,
                    Err(reason) => return Err(Fault::Integer { span, reason }),
                }
            }
            actual => return Err(Fault::expected(reply::Expectation::Integer, actual)),
        };
        Ok(frame::Read {
            value,
            cursor: read.cursor,
        })
    }

    pub(super) fn unsigned(
        wire: &Self,
        cursor: frame::Batch<'id, 1>,
    ) -> Result<frame::Read<'id, u64>, Fault<'id>> {
        let read = wire.try_next(cursor)?;
        Ok(frame::Read {
            value: wire.to_u64(read.value)?,
            cursor: read.cursor,
        })
    }

    pub(super) fn to_u64(
        &self,
        value: parser::Value<frame::Selection<'id>>,
    ) -> Result<u64, Fault<'id>> {
        match value {
            parser::Value::Integer(value) if value < 0 => Err(Fault::NegativeInteger(value)),
            parser::Value::Integer(value) => Ok(value as u64),
            parser::Value::Bulk(span) | parser::Value::Status(span) => {
                match scalar::Scalar::new(self.frame.slice(span)).unsigned() {
                    Ok(value) => Ok(value),
                    Err(scalar::Error::Negative(value)) => Err(Fault::NegativeInteger(value)),
                    Err(scalar::Error::Invalid(reason)) => Err(Fault::Integer { span, reason }),
                }
            }
            actual => Err(Fault::expected(reply::Expectation::Integer, actual)),
        }
    }

    pub(super) fn float(
        wire: &Self,
        cursor: frame::Batch<'id, 1>,
    ) -> Result<frame::Read<'id, f64>, Fault<'id>> {
        let read = wire.try_next(cursor)?;
        let value = scalar::float(read.value, &wire.frame)?;
        Ok(frame::Read {
            value,
            cursor: read.cursor,
        })
    }

    pub(super) fn coordinate(
        wire: &Self,
        batch: frame::Batch<'id, 1>,
    ) -> Result<frame::Read<'id, client::GeoCoord>, Fault<'id>> {
        let array = wire
            .nodes
            .array(batch)
            .map_err(|error| Fault::mismatch(reply::Expectation::Array, error))?;
        let batch = array.exact::<2>().map_err(|error| Fault::ArrayLength {
            expected: 2,
            actual: error.actual,
        })?;
        let read = wire.nodes.pair(batch);
        let [first, second] = read.value;
        let first = scalar::float(first.map_err(Fault::Server)?, &wire.frame)?;
        let second = scalar::float(second.map_err(Fault::Server)?, &wire.frame)?;
        Ok(frame::Read {
            value: client::GeoCoord::new(first, second),
            cursor: read.cursor,
        })
    }
}
