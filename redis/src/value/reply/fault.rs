use cartel_core::reply::retain;

use crate::protocol::parser::{self, frame, storage};
use crate::value::{self, reply};

pub(in crate::value) enum Fault<'id> {
    NegativeInteger(i64),
    NegativeCursor(i64),
    ArrayLength {
        expected: usize,
        actual: usize,
    },
    Expected {
        expected: reply::Expectation,
        actual: parser::Value<frame::Selection<'id>>,
    },
    Server(frame::Selection<'id>),
    Float {
        span: frame::Selection<'id>,
        reason: reply::Float,
    },
    Integer {
        span: frame::Selection<'id>,
        reason: reply::Invalid,
    },
}

impl<'id> Fault<'id> {
    pub(in crate::value) const fn expected(
        expected: reply::Expectation,
        actual: parser::Value<frame::Selection<'id>>,
    ) -> Self {
        Self::Expected { expected, actual }
    }

    pub(in crate::value) const fn mismatch(
        expected: reply::Expectation,
        mismatch: storage::Mismatch<frame::Selection<'id>>,
    ) -> Self {
        match mismatch {
            storage::Mismatch::Server(span) => Self::Server(span),
            storage::Mismatch::Value(actual) => Self::Expected { expected, actual },
        }
    }

    pub(in crate::value) fn into_reply<'arena, 'credits, 'region, const ID: u8>(
        self,
        frame: frame::Owner<'id, 'region>,
        keep: retain::Held<'arena, 'credits, 'region>,
    ) -> reply::Failure<crate::Error<'region, ID>, value::Bytes<'arena, 'credits, 'region>> {
        let (bytes, _) = keep.into_parts();
        match self {
            Self::NegativeInteger(value) => reply::Decode::NegativeInteger(value).into(),
            Self::NegativeCursor(value) => reply::Decode::NegativeCursor(value).into(),
            Self::ArrayLength { expected, actual } => {
                reply::Decode::ArrayLength { expected, actual }.into()
            }
            Self::Expected { expected, actual } => reply::Decode::Expected {
                expected,
                actual: reply::Actual::retained(actual, frame, bytes),
            }
            .into(),
            Self::Server(span) => {
                reply::Failure::Server(value::Bytes::new(frame.project(span), bytes))
            }
            Self::Float { span, reason } => reply::Failure::Number(reply::Number::Float {
                value: value::Bytes::new(frame.project(span), bytes),
                cause: reason,
            }),
            Self::Integer { span, reason } => reply::Failure::Number(reply::Number::Integer {
                value: value::Bytes::new(frame.project(span), bytes),
                cause: reason,
            }),
        }
    }
}
