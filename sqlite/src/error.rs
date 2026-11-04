use crate::{open::journal, row, sys};
use cartel_core::deadline;
use dope_core::driver::schedule::clock::timer;
use o3::alloc;
use std::{mem, str};

#[derive(Debug)]
pub enum Error {
    Sqlite {
        action: sys::Action,
        code: sys::Code,
    },
    QueryReturnedNoRows,
    ParameterItems {
        required: usize,
        available: usize,
    },
    ParameterBytes {
        required: usize,
        available: usize,
    },
    ParameterU64 {
        value: u64,
    },
    ParameterUsize {
        value: usize,
    },
    ScanRows {
        capacity: usize,
    },
    RowBytes {
        required: usize,
        available: usize,
    },
    RowLayout {
        required: usize,
        available: usize,
    },
    ColumnCount {
        expected: usize,
        actual: usize,
    },
    ColumnType {
        column: i32,
        expected: row::Class,
        actual: row::Kind,
    },
    ColumnI32 {
        column: i32,
        value: i64,
    },
    ColumnUtf8 {
        column: i32,
        error: str::Utf8Error,
    },
    CollectRows {
        required: usize,
        available: usize,
    },
    CollectRowBytes {
        required: usize,
        available: usize,
    },
    CollectValueBytes {
        required: usize,
        available: usize,
    },
    PrepareSqlBytes {
        actual: usize,
    },
    PragmaBytes {
        start: usize,
        added: usize,
    },
    StatementSqlLimit {
        requested: i32,
        available: i32,
    },
    StatementBytes {
        required: usize,
        available: usize,
    },
    StatementMissing {
        bytes: usize,
    },
    StatementTail {
        offset: usize,
    },
    CatalogStatementSql {
        statement: usize,
    },
    WorkLimit {
        action: sys::Action,
        code: sys::Code,
    },
    /// The progress callback exhausted its quota while SQLite returned success.
    WorkExhausted,
    Busy {
        action: sys::Action,
        code: sys::Code,
    },
    TransactionBusy {
        action: sys::Action,
        code: sys::Code,
    },
    TransactionAborted {
        action: sys::Action,
        code: sys::Code,
    },
    Savepoints {
        required: usize,
        available: usize,
    },
    Locked {
        action: sys::Action,
        code: sys::Code,
    },
    RequestTimeout {
        elapsed: deadline::Elapsed,
        action: sys::Action,
        code: sys::Code,
    },
    ReentrantQuery,
    ConnectionPoisoned,
    Journal {
        expected: journal::Mode,
        actual: journal::Mode,
    },
    MemoryJournal,
    Mapped {
        expected: i64,
        actual: i64,
    },
    TemporaryStore {
        actual: i64,
    },
    MainCache {
        expected: i64,
        actual: i64,
    },
    TemporaryCache {
        expected: i64,
        actual: i64,
    },
    LocalAllocatorOccupied,
    LocalConnectionBorrowed,
    LocalConnectionClosed,
    TimerCapacity(timer::Exhausted),
    Allocation(alloc::Error),
    NegativeLimit {
        value: i64,
    },
    NegativeOffset {
        value: i64,
    },
    UnexpectedRows {
        expected: usize,
        minimum: usize,
    },
    ParameterCount {
        expected: usize,
        actual: usize,
    },
    PathNul(usize),
    PathBytes {
        actual: usize,
        capacity: usize,
    },
    StatementNul {
        offset: usize,
    },
    ForeignKeys {
        actual: i32,
    },
    Defensive {
        actual: i32,
    },
    TrustedSchema {
        actual: i32,
    },
    JournalBytes {
        value: u64,
    },
    CheckpointPages {
        value: u32,
    },
    MappedBytes {
        value: u64,
    },
    /// SQLite returned an unrecognized journal mode name.
    JournalMode,
}

impl From<timer::Exhausted> for Error {
    fn from(error: timer::Exhausted) -> Self {
        Self::TimerCapacity(error)
    }
}

const _: () = {
    assert!(!mem::needs_drop::<Error>());
    assert!(mem::size_of::<Error>() <= 3 * mem::size_of::<usize>());
};
