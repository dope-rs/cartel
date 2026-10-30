use cartel_core::reply::retain;

use crate::protocol::parser::{self, frame};
use crate::value::source::Control;
use crate::value::{self, source};
use std::{fmt, num::ParseFloatError};

mod fault;
pub(in crate::value) use fault::Fault;

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum Expectation {
    Boolean,
    Bulk,
    BulkOrNil,
    Integer,
    IntegerOrNil,
    Float,
    FloatOrNil,
    Array,
    ArrayOrNil,
    ScanCursor,
    TypeName,
}

#[derive(Debug)]
pub enum Actual<B> {
    Nil,
    Ok,
    Status(B),
    Integer(i64),
    Bulk(B),
    Array { len: usize },
}

impl<'arena, 'credits, 'region> Actual<value::Bytes<'arena, 'credits, 'region>> {
    pub(crate) fn retained<'id>(
        node: parser::Value<frame::Selection<'id>>,
        frame: frame::Owner<'id, 'region>,
        credit: Option<retain::Bytes<'arena, 'credits, 'region>>,
    ) -> Self {
        match node {
            parser::Value::Nil => Self::Nil,
            parser::Value::Ok => Self::Ok,
            parser::Value::Status(span) => {
                Self::Status(value::Bytes::new(frame.project(span), credit))
            }
            parser::Value::Integer(value) => Self::Integer(value),
            parser::Value::Bulk(span) => Self::Bulk(value::Bytes::new(frame.project(span), credit)),
            parser::Value::Array { len } => Self::Array { len: len as usize },
        }
    }
}

#[derive(Debug)]
pub enum Decode<B> {
    Expected {
        expected: Expectation,
        actual: Actual<B>,
    },
    NegativeInteger(i64),
    NegativeCursor(i64),
    ArrayLength {
        expected: usize,
        actual: usize,
    },
    ArrayMultiple {
        width: usize,
        actual: usize,
    },
}

#[derive(Clone, Copy, Debug)]
pub enum Invalid {
    Empty,
    Missing { offset: usize },
    Byte { offset: usize, actual: u8 },
    Overflow { offset: usize },
}

/// A byte-level rejection or the original float conversion error and its diagnostic.
#[derive(Clone, Debug)]
pub enum Float {
    Invalid(Invalid),
    Parse {
        error: ParseFloatError,
        cause: Option<Invalid>,
    },
}

#[derive(Debug)]
pub enum Number<B> {
    Float { value: B, cause: Float },
    Integer { value: B, cause: Invalid },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Kind {
    Control,
    Server,
    Number,
    Decode,
}

/// Command failures retain the exact route of their control cause.
#[derive(Debug)]
pub enum Failure<C, B> {
    Control(C),
    Server(B),
    Number(Number<B>),
    Decode(Decode<B>),
}

impl<B: AsRef<[u8]>> Actual<B> {
    pub fn as_ref(&self) -> Actual<&[u8]> {
        match self {
            Self::Nil => Actual::Nil,
            Self::Ok => Actual::Ok,
            Self::Status(value) => Actual::Status(value.as_ref()),
            Self::Integer(value) => Actual::Integer(*value),
            Self::Bulk(value) => Actual::Bulk(value.as_ref()),
            Self::Array { len } => Actual::Array { len: *len },
        }
    }
}

impl<B: AsRef<[u8]>> Decode<B> {
    pub fn as_ref(&self) -> Decode<&[u8]> {
        match self {
            Self::Expected { expected, actual } => Decode::Expected {
                expected: *expected,
                actual: actual.as_ref(),
            },
            Self::NegativeInteger(value) => Decode::NegativeInteger(*value),
            Self::NegativeCursor(value) => Decode::NegativeCursor(*value),
            Self::ArrayLength { expected, actual } => Decode::ArrayLength {
                expected: *expected,
                actual: *actual,
            },
            Self::ArrayMultiple { width, actual } => Decode::ArrayMultiple {
                width: *width,
                actual: *actual,
            },
        }
    }
}

impl<B: AsRef<[u8]>> Number<B> {
    pub fn as_ref(&self) -> Number<&[u8]> {
        match self {
            Self::Float { value, cause } => Number::Float {
                value: value.as_ref(),
                cause: cause.clone(),
            },
            Self::Integer { value, cause } => Number::Integer {
                value: value.as_ref(),
                cause: *cause,
            },
        }
    }
}

impl<'region, const ID: u8, B> From<crate::Error<'region, ID>>
    for Failure<crate::Error<'region, ID>, B>
{
    fn from(error: crate::Error<'region, ID>) -> Self {
        Self::Control(error)
    }
}

impl<C, B> From<Decode<B>> for Failure<C, B> {
    fn from(error: Decode<B>) -> Self {
        Self::Decode(error)
    }
}

impl<'region, const ID: u8> source::Sealed
    for Failure<crate::Error<'region, ID>, value::Bytes<'_, '_, 'region>>
{
}

impl<'arena, 'credits, 'region, const ID: u8> source::Error
    for Failure<crate::Error<'region, ID>, value::Bytes<'arena, 'credits, 'region>>
{
    type Bytes = value::Bytes<'arena, 'credits, 'region>;
    type DriverError = crate::Error<'region, ID>;

    fn kind(&self) -> Kind {
        match self {
            Self::Control(_) => Kind::Control,
            Self::Server(_) => Kind::Server,
            Self::Number(_) => Kind::Number,
            Self::Decode(_) => Kind::Decode,
        }
    }

    fn diagnostic(&self) -> impl fmt::Debug {
        match self {
            Self::Control(cause) => Failure::Control(Control::diagnostic(cause)),
            Self::Server(value) => Failure::Server(value.as_ref()),
            Self::Number(cause) => Failure::Number(cause.as_ref()),
            Self::Decode(cause) => Failure::Decode(cause.as_ref()),
        }
    }
}
