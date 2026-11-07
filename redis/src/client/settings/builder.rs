use crate::protocol::parser::storage;
use cartel_core::{
    memory::{self, Layout},
    reply::layout,
};
use dope::net::link::egress;
use dope_fiber::{reply::usage, wait};
use o3::{base::time::Positive, buffer::pool, depot::queue::pool::fair};
use parser::{capacity, limit};

use crate::{
    client::settings::{
        self, Bulk, Calc, Capacities, Config, Error, Field, MemoryBound, Op, limits::Limits,
    },
    protocol::parser,
};

pub struct Builder {
    pub(super) caps: Capacities,
    pub(super) bulk: Option<Bulk>,
    pub(super) parser_lanes: Option<usize>,
    pub(super) window: Positive,
}

impl Builder {
    pub const fn new(cap: Capacities) -> Self {
        Self {
            caps: cap,
            bulk: None,
            parser_lanes: None,
            window: settings::DEFAULT_WINDOW,
        }
    }

    pub const fn window(mut self, span: Positive) -> Self {
        self.window = span;
        self
    }

    pub const fn bulk(mut self, bulk: Bulk) -> Self {
        self.bulk = Some(bulk);
        self
    }

    pub const fn parser_lanes(mut self, lanes: usize) -> Self {
        self.parser_lanes = Some(lanes);
        self
    }

    pub fn build(self) -> Result<Config, Error> {
        let window = self.window;
        let policy = memory::RETENTION;
        let bulk = self.bulk;
        let lanes = self.parser_lanes;
        let Capacities {
            connections: conns,
            admission_waiters: waits,
            inflight: flight,
            request_frame_bytes: req,
            response_bytes: bytes,
            response_values: vals,
            response_frame_bytes: frame,
            response_frame_values: nodes,
        } = self.caps;
        if conns == 0 {
            return Err(Error::Zero(Field::Connection));
        }
        let lanes = lanes.unwrap_or(conns.min(4));
        if lanes == 0 {
            return Err(Error::Zero(Field::ParserLane));
        }
        if lanes > conns {
            return Err(Error::Above {
                field: Field::ParserLane,
                actual: lanes,
                maximum: conns,
            });
        }
        if waits == 0 {
            return Err(Error::Zero(Field::Waiter));
        }
        let max = usize::MAX / 4 + 1;
        if waits > max {
            return Err(Error::Above {
                field: Field::Waiter,
                actual: waits,
                maximum: max,
            });
        }
        if flight == 0 {
            return Err(Error::Zero(Field::Inflight));
        }
        if req == 0 {
            return Err(Error::Zero(Field::RequestFrame));
        }
        if bytes == 0 {
            return Err(Error::Zero(Field::Response));
        }
        if vals == 0 {
            return Err(Error::Zero(Field::ResponseValue));
        }
        if flight < conns {
            return Err(Error::Below {
                field: Field::Inflight,
                actual: flight,
                minimum: conns,
            });
        }
        if vals < flight {
            return Err(Error::Below {
                field: Field::ResponseValue,
                actual: vals,
                minimum: flight,
            });
        }
        if frame > bytes {
            return Err(Error::Above {
                field: Field::ResponseFrame,
                actual: frame,
                maximum: bytes,
            });
        }
        if nodes > vals {
            return Err(Error::Above {
                field: Field::ResponseFrameValue,
                actual: nodes,
                maximum: vals,
            });
        }
        if frame > settings::MAX_FRAME_CAPACITY {
            return Err(Error::Above {
                field: Field::ResponseFrame,
                actual: frame,
                maximum: settings::MAX_FRAME_CAPACITY,
            });
        }
        let frame = match limit::Bound::<limit::Frame>::try_new(frame) {
            Ok(frame) => frame,
            Err(limit::Invalid::Zero) => return Err(Error::Zero(Field::ResponseFrame)),
            Err(limit::Invalid::Above { actual, maximum }) => {
                return Err(Error::Above {
                    field: Field::ResponseFrame,
                    actual,
                    maximum,
                });
            }
        };
        let limit = frame.get();
        let max = u32::MAX as usize;
        for (field, actual, limit) in [
            (Field::Connection, conns, max),
            (Field::Inflight, flight, max / 2),
            (Field::RequestFrame, req, max),
            (Field::ResponseValue, vals, max),
        ] {
            if actual > limit {
                return Err(Error::Above {
                    field,
                    actual,
                    maximum: limit,
                });
            }
        }
        let nodes = match limit::Bound::<limit::Value>::try_new(nodes) {
            Ok(nodes) => nodes,
            Err(limit::Invalid::Zero) => return Err(Error::Zero(Field::ResponseFrameValue)),
            Err(limit::Invalid::Above { actual, maximum }) => {
                return Err(Error::Above {
                    field: Field::ResponseFrameValue,
                    actual,
                    maximum,
                });
            }
        };
        let count = nodes.get();
        let parser = match capacity::Capacity::try_with(conns, lanes, count, vals) {
            Ok(capacity) => capacity,
            Err(capacity::Error::Lanes {
                connections: left,
                values: right,
            }) => {
                return Err(Error::Overflow {
                    calc: Calc::ParserLaneValues,
                    op: Op::Mul,
                    left,
                    right,
                });
            }
            Err(capacity::Error::Total {
                lanes: left,
                retained: right,
            }) => {
                return Err(Error::Overflow {
                    calc: Calc::ParserValues,
                    op: Op::Add,
                    left,
                    right,
                });
            }
            Err(capacity::Error::Above { actual, maximum }) => {
                return Err(Error::Above {
                    field: Field::ParserValue,
                    actual,
                    maximum,
                });
            }
        };
        let parse = parser.get();
        let direct = pool::Layout::new(flight, req).map_err(Error::RequestPool)?;
        let large = bulk.map(|bulk| bulk.layout(req)).transpose()?;
        let queue = fair::Plan::new(flight, conns).map_err(Error::RequestQueue)?;
        let arena = layout::Layout::new(
            layout::Capacity::new(
                layout::Lanes::new(conns),
                layout::Entries::new(flight),
                layout::Data::new(flight),
                usage::Bytes::new(bytes),
                usage::Values::new(vals),
            ),
            layout::Limit::new(
                layout::Data::new(1),
                usage::Bytes::new(limit),
                usage::Values::new(count),
            ),
        )
        .map_err(Error::ResponseLayout)?;
        let fixed = direct.bytes();
        let extra = large.map_or(0, pool::Layout::bytes);
        let reqs = fixed.checked_add(extra).ok_or(Error::Overflow {
            calc: Calc::TotalRequestBytes,
            op: Op::Add,
            left: fixed,
            right: extra,
        })?;
        let entries = flight
            .checked_add(bulk.map_or(0, |bulk| bulk.entries))
            .ok_or(Error::Overflow {
                calc: Calc::EgressEntries,
                op: Op::Add,
                left: flight,
                right: bulk.map_or(0, |bulk| bulk.entries),
            })?;
        let entries = u32::try_from(entries).map_err(|_| Error::Above {
            field: Field::EgressEntries,
            actual: entries,
            maximum: u32::MAX as usize,
        })?;
        let resident = u32::try_from(reqs).map_err(|_| Error::Above {
            field: Field::EgressResident,
            actual: reqs,
            maximum: u32::MAX as usize,
        })?;
        let egress = egress::Config::shared(egress::Credit::new(
            egress::Entries::new(entries),
            egress::Resident::new(resident),
        ));
        let retain =
            Layout::try_framed(policy, bytes, conns, limit, 0).map_err(Error::Retention)?;
        let admit = retain.admitted();
        let owners = flight.checked_add(conns).ok_or(Error::Overflow {
            calc: Calc::ResponseOwners,
            op: Op::Add,
            left: flight,
            right: conns,
        })?;
        use o3::depot::capacity::Count;
        let owners = Count::try_from(owners).map_err(Error::Owners)?;
        let input = retain.ingress(owners);
        let held = retain.pending();
        let used = input.try_resident(conns, limit).map_err(Error::Retention)?;
        let tape = parse
            .checked_mul(storage::Storage::NODE_SIZE)
            .ok_or(Error::Overflow {
                calc: Calc::ParseTapeBytes,
                op: Op::Mul,
                left: parse,
                right: storage::Storage::NODE_SIZE,
            })?;
        let total = used.try_add(reqs).map_err(|error| Error::Limit {
            calc: Calc::ResidentAndRequest,
            error,
        })?;
        let total = total.try_add(tape).map_err(|error| Error::Limit {
            calc: Calc::TotalBytes,
            error,
        })?;
        Ok(Config {
            caps: Limits {
                waiters: wait::Capacity::new(waits),
                parser,
                frame,
                values: nodes,
            },
            window,
            memory: MemoryBound {
                request_bytes: reqs,
                response_bytes: bytes,
                response_resident_bytes: admit,
                ingress: input,
                pending_bytes: held,
                resident_bytes: used,
                parse_tape_bytes: tape,
                total_bytes: total,
            },
            pool: direct,
            bulk: large,
            queue,
            arena,
            egress,
        })
    }
}
