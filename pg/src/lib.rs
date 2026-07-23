mod attach;
mod client;
mod decode;
pub mod dsl;
mod encode;
pub mod port;
mod protocol;
mod query;
mod raw;
mod scram;
pub mod sql;
mod tx;
mod value;
mod wire;

pub use attach::attach;
pub use cartel_gen::PgTable;
pub use client::{
    Client, CopyInGuard, CopyOutStream, Dispatched, ExtractUnit, NextNotification, PgOps,
    RunStream, Runner,
};
pub use dope_extra::runtime::AppRuntime;
pub use dope_fiber::{Batch, Fiber, Lazy};

pub use dsl::{
    AggBuilder, AggHandle, ConflictTarget, Cte, DeleteBuilder, EachClosure, EachCols,
    FilterBuilder, InsertBuilder, JoinBuilder, JoinBuilder2, JoinBuilder3, JoinBuilder4,
    Joined2Filter, Joined3Filter, Joined4Filter, SelectBuilder, SourceRow, Stream, TsQuery,
    TsVector, UpdateBuilder, UpdateEachBuilder, WindowExpr, WindowSpec, abs, age, array_length,
    avg, cardinality, ceil, char_length, coalesce, count, current_date, current_time,
    current_timestamp, date_part, date_trunc, dense_rank, exists, floor, lag, lead, length, lower,
    max, min, not_exists, now, phraseto_tsquery, plainto_tsquery, position, power, rank,
    regexp_match, regexp_replace, replace, round, row_number, sqrt, substring, sum, to_tsquery,
    to_tsvector, trim, ts_rank, upper, websearch_to_tsquery,
};
pub use port::{Port, PortFactory};
pub use protocol::{PickPolicy, Session};
pub use query::{HasGroup, QueryGroup, QueryMeta, QuerySet, Row, TypedQuery};
pub use raw::PgRawExt;
pub use tx::{
    AccessMode, CancelToken, IsolationLevel, ListenGuard, PgPool, SavepointGuard, Tx, TxBuilder,
    TxGuard,
};
pub use value::{BindWriter, RowReader};
pub use wire::Sink;

#[derive(Debug, Clone)]
pub struct Notification {
    pub pid: u32,
    pub channel: String,
    pub payload: String,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Default, Debug)]
pub struct Timestamp(pub i64);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Default, Debug)]
pub struct Date(pub i32);

#[derive(Clone, PartialEq, Eq, Hash, Default, Debug)]
pub struct Ltree(pub String);

impl Ltree {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Ltree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone)]
pub struct Text(o3::buffer::SharedStr);

impl Text {
    pub fn from_static(s: &'static str) -> Self {
        Self(o3::buffer::SharedStr::from_static(s))
    }

    pub(crate) fn from_shared(bytes: o3::buffer::Shared) -> Result<Self, std::str::Utf8Error> {
        o3::buffer::SharedStr::from_utf8(bytes).map(Self)
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl std::ops::Deref for Text {
    type Target = str;
    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<str> for Text {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl PartialEq for Text {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl PartialEq<str> for Text {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for Text {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl Eq for Text {}

#[derive(Clone)]
pub struct Jsonb(o3::buffer::SharedStr);

impl Jsonb {
    pub fn from_static_json(s: &'static str) -> Self {
        Self(o3::buffer::SharedStr::from_static(s))
    }

    pub fn from_string(s: String) -> Self {
        Self(o3::buffer::SharedStr::from(s))
    }

    pub(crate) fn from_shared(bytes: o3::buffer::Shared) -> Result<Self, std::str::Utf8Error> {
        o3::buffer::SharedStr::from_utf8(bytes).map(Self)
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl std::fmt::Debug for Jsonb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Jsonb({:?})", self.as_str())
    }
}

impl AsRef<str> for Jsonb {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::ops::Deref for Jsonb {
    type Target = str;
    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl PartialOrd for Text {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Text {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl std::fmt::Debug for Text {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self.as_str(), f)
    }
}

impl std::fmt::Display for Text {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum RangeBound<T> {
    Inclusive(T),
    Exclusive(T),
    Unbounded,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Range<T> {
    pub lower: RangeBound<T>,
    pub upper: RangeBound<T>,
    pub empty: bool,
}

impl<T> Range<T> {
    pub const fn empty() -> Self {
        Self {
            lower: RangeBound::Unbounded,
            upper: RangeBound::Unbounded,
            empty: true,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Uuid(pub [u8; 16]);

impl Uuid {
    pub const NIL: Self = Self([0u8; 16]);

    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub const fn into_bytes(self) -> [u8; 16] {
        self.0
    }

    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl std::fmt::Display for Uuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut buf = [0u8; 36];
        let mut bi = 0;
        for (i, b) in self.0.iter().enumerate() {
            if matches!(i, 4 | 6 | 8 | 10) {
                buf[bi] = b'-';
                bi += 1;
            }
            buf[bi] = HEX[(b >> 4) as usize];
            bi += 1;
            buf[bi] = HEX[(b & 0xf) as usize];
            bi += 1;
        }
        let text = std::str::from_utf8(&buf).map_err(|_| std::fmt::Error)?;
        f.write_str(text)
    }
}

impl std::fmt::Debug for Uuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Uuid({self})")
    }
}

#[doc(hidden)]
pub mod __internal {
    use std::marker::PhantomData;

    pub struct FilterBuilder<T>(PhantomData<T>);

    impl<T> FilterBuilder<T> {
        #[doc(hidden)]
        pub fn __new() -> Self {
            Self(PhantomData)
        }

        pub fn one(self) -> T {
            unreachable!("cartel_pg: terminator only valid inside #[query] body")
        }

        pub fn first(self) -> Option<T> {
            unreachable!("cartel_pg: terminator only valid inside #[query] body")
        }

        pub fn all(self) -> Vec<T> {
            unreachable!("cartel_pg: terminator only valid inside #[query] body")
        }
    }

    pub const fn concat_len(parts: &[&str]) -> usize {
        let mut total = 0;
        let mut i = 0;
        while i < parts.len() {
            total += parts[i].len();
            i += 1;
        }
        total
    }

    pub const fn concat<const N: usize>(parts: &[&str]) -> [u8; N] {
        let mut buf = [0u8; N];
        let mut bi = 0;
        let mut pi = 0;
        while pi < parts.len() {
            let bytes = parts[pi].as_bytes();
            let mut i = 0;
            while i < bytes.len() {
                buf[bi] = bytes[i];
                bi += 1;
                i += 1;
            }
            pi += 1;
        }
        buf
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    user: String,
    password: String,
    database: String,
    application_name: String,
    options: String,
    statement_timeout_ms: u32,
}

impl Config {
    pub const DEFAULT_STATEMENT_TIMEOUT_MS: u32 = 30_000;

    pub fn new(
        user: impl Into<String>,
        password: impl Into<String>,
        database: impl Into<String>,
    ) -> Self {
        Self {
            user: user.into(),
            password: password.into(),
            database: database.into(),
            application_name: "cartel-pg".into(),
            options: String::new(),
            statement_timeout_ms: Self::DEFAULT_STATEMENT_TIMEOUT_MS,
        }
    }

    pub fn search_path(mut self, schema: &str) -> Self {
        self.options = format!("-c search_path={schema},public");
        self
    }

    pub fn statement_timeout(mut self, dur: std::time::Duration) -> Self {
        self.statement_timeout_ms = dur.as_millis().min(u32::MAX as u128) as u32;
        self
    }

    pub(crate) fn user(&self) -> &str {
        &self.user
    }

    pub(crate) fn password(&self) -> &str {
        &self.password
    }

    pub(crate) fn database(&self) -> &str {
        &self.database
    }

    pub(crate) fn application_name(&self) -> &str {
        &self.application_name
    }

    pub(crate) fn options(&self) -> &str {
        &self.options
    }

    pub(crate) fn statement_timeout_ms(&self) -> u32 {
        self.statement_timeout_ms
    }
}

#[derive(Debug, Clone, Default)]
pub struct DbError {
    pub severity: String,
    pub code: String,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
    pub position: Option<u32>,
    pub schema: Option<String>,
    pub table: Option<String>,
    pub column: Option<String>,
    pub constraint: Option<String>,
}

impl DbError {
    pub fn transient(&self) -> bool {
        matches!(self.code.get(..2), Some("08" | "53" | "57" | "58"))
    }
}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.code.is_empty() {
            write!(f, "{}: {}", self.code, self.message)?;
        } else {
            f.write_str(&self.message)?;
        }
        if let Some(d) = &self.detail {
            write!(f, " (detail: {d})")?;
        }
        if let Some(h) = &self.hint {
            write!(f, " (hint: {h})")?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Closed,
    Auth(String),
    Db(Box<DbError>),
    Protocol(&'static str),
    ProtocolOwned(String),
    NotFound,
    UnexpectedNull,
    NoReadyConn,
    WaiterCapacity,
    RequestCapacity,
    RequestTooLarge,
    ResponseCapacity,
    Backpressure {
        inflight: usize,
        queued: usize,
        cap: usize,
    },
    Other(String),
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl Error {
    pub fn db(&self) -> Option<&DbError> {
        match self {
            Self::Db(e) => Some(e),
            _ => None,
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "io error: {e}"),
            Self::Closed => f.write_str("connection closed"),
            Self::Auth(s) => write!(f, "auth: {s}"),
            Self::Db(e) => write!(f, "server: {e}"),
            Self::Protocol(s) => write!(f, "protocol: {s}"),
            Self::ProtocolOwned(s) => write!(f, "protocol: {s}"),
            Self::NotFound => f.write_str("query returned no rows"),
            Self::UnexpectedNull => f.write_str("unexpected NULL in non-nullable column"),
            Self::NoReadyConn => f.write_str("no ready connection (saturated or connecting)"),
            Self::WaiterCapacity => f.write_str("waiter capacity exhausted"),
            Self::RequestCapacity => f.write_str("request capacity exhausted"),
            Self::RequestTooLarge => f.write_str("request exceeds configured byte capacity"),
            Self::ResponseCapacity => f.write_str("response capacity exceeded"),
            Self::Backpressure {
                inflight,
                queued,
                cap,
            } => write!(
                f,
                "backpressure: pipeline full ({}/{}, queued={})",
                inflight, cap, queued
            ),
            Self::Other(s) => f.write_str(s),
        }
    }
}
