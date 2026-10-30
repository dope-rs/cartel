use o3::{
    base::time::Positive,
    buffer::{hold::limits::Bound, pool},
    depot::queue::pool::fair,
};
mod builder;
mod error;
pub mod init;
mod limits;
use std::time;

pub use builder::Builder;
use cartel_core::{memory::Ingress, reply::layout};
use dope::net::link::egress;
use dope_fiber::wait;
pub use error::Error;
use limits::Limits;
use parser::{capacity, limit};

use crate::protocol::parser;
pub const DEFAULT_BACKOFF: time::Duration = time::Duration::from_millis(500);
pub const DEFAULT_BACKOFF_MAXIMUM: time::Duration = time::Duration::from_secs(512);
pub const MAX_FRAME_CAPACITY: usize = 16 * 1024 * 1024;
const DEFAULT_WINDOW: Positive = Positive::from_secs::<30>();
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct Config {
    caps: Limits,
    pub(crate) window: Positive,
    memory: MemoryBound,
    pool: pool::Layout,
    bulk: Option<pool::Layout>,
    queue: fair::Plan,
    arena: layout::Layout,
    egress: egress::Config,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct Bulk {
    entries: usize,
    frame_bytes: usize,
}

/// Physical upper bound for driver-retained payload buffers and parser tape; fixed routing/wait metadata, allocator metadata, and transferred values are excluded.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct MemoryBound {
    /// Total fixed request-frame allocation.
    pub request_bytes: usize,
    /// Logical completed-response allowance within the resident budget.
    pub response_bytes: usize,
    /// Physical ceiling for policy-compliant admitted responses.
    pub response_resident_bytes: Bound,
    pub ingress: Ingress,
    /// One materialized, not-yet-admitted frame per connection.
    pub pending_bytes: usize,
    /// Physical allocation budget for responses, partial ingress, and pending frames.
    pub resident_bytes: Bound,
    pub parse_tape_bytes: usize,
    pub total_bytes: Bound,
}

impl Bulk {
    pub const fn new(items: usize, bytes: usize) -> Self {
        Self {
            entries: items,
            frame_bytes: bytes,
        }
    }

    fn layout(self, base: usize) -> Result<pool::Layout, Error> {
        if self.entries == 0 {
            return Err(Error::Zero(Field::BulkEntry));
        }
        if self.frame_bytes == 0 {
            return Err(Error::Zero(Field::BulkFrame));
        }
        if self.frame_bytes <= base {
            return Err(Error::NotGreater {
                field: Field::BulkFrame,
                actual: self.frame_bytes,
                base,
            });
        }
        if self.frame_bytes > MAX_FRAME_CAPACITY {
            return Err(Error::Above {
                field: Field::BulkFrame,
                actual: self.frame_bytes,
                maximum: MAX_FRAME_CAPACITY,
            });
        }
        pool::Layout::new(self.entries, self.frame_bytes).map_err(Error::BulkPool)
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct Capacities {
    /// Physical connections and independent response lanes.
    pub connections: usize,
    /// Fibers allowed to wait in each independent dispatch or lease class.
    pub admission_waiters: usize,
    /// Requests admitted across all connections.
    pub inflight: usize,
    /// Bytes available to encode one request frame.
    pub request_frame_bytes: usize,
    /// Logical RESP frame bytes retained across all admitted responses.
    pub response_bytes: usize,
    /// RESP values retained across all admitted responses.
    pub response_values: usize,
    /// Largest accepted RESP frame in bytes.
    pub response_frame_bytes: usize,
    /// Largest accepted RESP frame in parsed values.
    pub response_frame_values: usize,
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum Field {
    Connection,
    Waiter,
    Inflight,
    RequestFrame,
    BulkEntry,
    BulkFrame,
    EgressEntries,
    EgressResident,
    Response,
    ResponseValue,
    ResponseFrame,
    ResponseFrameValue,
    ParserValue,
    ParserLane,
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum Calc {
    ParserLaneValues,
    ParserValues,
    TotalRequestBytes,
    EgressEntries,
    ResponseOwners,
    ParseTapeBytes,
    ResidentAndRequest,
    TotalBytes,
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum Op {
    Add,
    Mul,
}

impl Config {
    pub fn new(cap: Capacities) -> Result<Self, Error> {
        Builder {
            caps: cap,
            bulk: None,
            parser_lanes: None,
            window: DEFAULT_WINDOW,
        }
        .build()
    }
}

impl Config {
    pub const fn connections(self) -> usize {
        self.caps.parser.connections()
    }

    pub const fn waiters(self) -> wait::Capacity {
        self.caps.waiters
    }

    pub const fn inflight(self) -> usize {
        self.pool.slots() as usize
    }

    pub const fn request_pool(self) -> pool::Layout {
        self.pool
    }

    pub(crate) const fn bulk_pool(self) -> Option<pool::Layout> {
        self.bulk
    }

    pub(crate) const fn egress(self) -> egress::Config {
        self.egress
    }

    pub const fn queue(self) -> fair::Plan {
        self.queue
    }

    pub(crate) const fn parser(self) -> capacity::Capacity {
        self.caps.parser
    }

    pub const fn max_frame(self) -> limit::Bound<limit::Frame> {
        self.caps.frame
    }

    pub const fn frame_values(self) -> limit::Bound<limit::Value> {
        self.caps.values
    }

    pub const fn memory_bound(self) -> MemoryBound {
        self.memory
    }

    pub(crate) const fn arena(self) -> layout::Layout {
        self.arena
    }
}
