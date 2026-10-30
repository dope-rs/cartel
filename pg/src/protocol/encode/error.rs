use crate::{
    protocol::encode::{Position, Scope},
    schema::result,
};
use o3::buffer::write::consume;
use std::num::NonZero;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    FrameLength {
        actual: usize,
    },
    LengthOverflow {
        scope: Scope,
        current: usize,
        additional: usize,
    },
    ValueLength {
        actual: usize,
    },
    ParseTypes {
        actual: usize,
    },
    ParameterFormats {
        actual: usize,
    },
    ResultFormats {
        actual: usize,
    },
    ParameterCount {
        declared: i16,
        types: usize,
    },
    ParameterValueCount {
        expected: usize,
        actual: usize,
    },
    ParameterOid {
        index: usize,
        expected: u32,
        actual: u32,
    },
    ParameterEncoding {
        index: usize,
        expected: u16,
        actual: u16,
    },
    ResultCount {
        declared: u16,
    },
    ResultShape {
        kind: result::Kind,
        columns: u16,
    },
    ParameterFormatCount {
        actual: usize,
        expected: usize,
    },
    ResultFormatCount {
        actual: usize,
        expected: usize,
    },
    ParameterFormat {
        index: usize,
        actual: u16,
    },
    ResultFormat {
        index: usize,
        actual: u16,
    },
    ResultValueCount {
        expected: usize,
        actual: usize,
    },
    ResultEncoding {
        index: usize,
        expected: u16,
        actual: u16,
    },
    StatementNameNul {
        position: usize,
    },
    StatementNameEmpty,
    StatementNameReserved,
    StatementSqlNul {
        position: usize,
    },
    GroupCount {
        actual: usize,
    },
    StatementCount {
        current: u32,
        additional: usize,
    },
    DuplicateStatement {
        first: Position,
        second: Position,
    },
    PreparationSize {
        expected: usize,
        actual: usize,
    },
    ArrayElements {
        actual: usize,
    },
    Capacity {
        required: NonZero<usize>,
        capacity: usize,
    },
}

impl Error {
    pub(super) fn capacity(error: consume::Error) -> Self {
        let (required, capacity) = error.into_parts();
        Self::Capacity { required, capacity }
    }
}

const _: () = assert!(std::mem::size_of::<Error>() <= 3 * std::mem::size_of::<usize>());
