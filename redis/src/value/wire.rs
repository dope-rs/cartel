use cartel_core::reply::retain;

use crate::client;
use crate::protocol::{parser, parser::frame, parser::storage};
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
        decode: impl for<'id> FnOnce(&Wire<'id, 's, 'arena, 'credits, 'region>) -> Result<T, Fault<'id>>,
    ) -> Result<T, reply::Failure<crate::Error<'region, ID>, value::Bytes<'arena, 'credits, 'region>>>
    {
        Self::with(reply, |wire| {
            decode(&wire).map_err(|error| wire.failure(error))
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

    pub(super) fn try_next(&self) -> Result<parser::Value<frame::Selection<'id>>, Fault<'id>> {
        self.nodes
            .next(self.nodes.root())
            .map(|read| read.value)
            .map_err(Fault::Server)
    }

    pub(super) fn boolean(wire: &Self) -> Result<bool, Fault<'id>> {
        match wire.try_next()? {
            parser::Value::Integer(value) => Ok(value != 0),
            parser::Value::Ok => Ok(true),
            parser::Value::Nil => Ok(false),
            actual => Err(Fault::expected(reply::Expectation::Boolean, actual)),
        }
    }

    pub(super) fn opt_float(wire: &Self) -> Result<Option<f64>, Fault<'id>> {
        match wire.try_next()? {
            parser::Value::Nil => Ok(None),
            value @ (parser::Value::Bulk(_) | parser::Value::Status(_)) => {
                scalar::float(value, &wire.frame).map(Some)
            }
            parser::Value::Integer(value) => Ok(Some(value as f64)),
            actual => Err(Fault::expected(reply::Expectation::FloatOrNil, actual)),
        }
    }

    pub(super) fn opt_unsigned(wire: &Self) -> Result<Option<u64>, Fault<'id>> {
        match wire.try_next()? {
            parser::Value::Nil => Ok(None),
            value @ (parser::Value::Integer(_)
            | parser::Value::Bulk(_)
            | parser::Value::Status(_)) => wire.to_u64(value).map(Some),
            actual => Err(Fault::expected(reply::Expectation::IntegerOrNil, actual)),
        }
    }

    pub(super) fn signed(wire: &Self) -> Result<i64, Fault<'id>> {
        match wire.try_next()? {
            parser::Value::Integer(value) => Ok(value),
            parser::Value::Bulk(span) | parser::Value::Status(span) => {
                scalar::signed(wire.frame.slice(span))
                    .map_err(|reason| Fault::Integer { span, reason })
            }
            actual => Err(Fault::expected(reply::Expectation::Integer, actual)),
        }
    }

    pub(super) fn unsigned(wire: &Self) -> Result<u64, Fault<'id>> {
        wire.to_u64(wire.try_next()?)
    }

    pub(super) fn to_u64(
        &self,
        value: parser::Value<frame::Selection<'id>>,
    ) -> Result<u64, Fault<'id>> {
        match value {
            parser::Value::Integer(value) if value < 0 => Err(Fault::NegativeInteger(value)),
            parser::Value::Integer(value) => Ok(value as u64),
            parser::Value::Bulk(span) | parser::Value::Status(span) => {
                match scalar::unsigned(self.frame.slice(span)) {
                    Ok(value) => Ok(value),
                    Err(scalar::Error::Negative(value)) => Err(Fault::NegativeInteger(value)),
                    Err(scalar::Error::Invalid(reason)) => Err(Fault::Integer { span, reason }),
                }
            }
            actual => Err(Fault::expected(reply::Expectation::Integer, actual)),
        }
    }

    pub(super) fn float(wire: &Self) -> Result<f64, Fault<'id>> {
        scalar::float(wire.try_next()?, &wire.frame)
    }

    pub(super) fn coordinate(wire: &Self) -> Result<client::GeoCoord, Fault<'id>> {
        let array = wire
            .nodes
            .array(wire.nodes.root())
            .map_err(|error| Fault::mismatch(reply::Expectation::Array, error))?;
        let batch = array.exact::<2>().map_err(|error| Fault::ArrayLength {
            expected: 2,
            actual: error.actual,
        })?;
        let [first, second] = wire.nodes.pair(batch);
        let first = scalar::float(first.map_err(Fault::Server)?, &wire.frame)?;
        let second = scalar::float(second.map_err(Fault::Server)?, &wire.frame)?;
        Ok(client::GeoCoord::new(first, second))
    }
}
