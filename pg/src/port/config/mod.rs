use std::num::NonZero;

mod error;
use cartel_core::{
    memory::{self, Ingress, Layout},
    reply,
};
use dope::{
    core::{
        driver::route::table::{self, conn},
        io::transfer,
    },
    manifold::client::side,
};
pub use error::Error;
use o3::{
    base::{num::bounded, time::Positive},
    buffer::{hold::limits, pool},
    depot::{capacity, queue::pool::fair},
};
use reply::{layout, usage};

use crate::{port::replies::groups, protocol::runtime::ready::plan};

const FRAME_HEADER_BYTES: usize = 5;
pub const DEFAULT_OPERATION_WINDOW: Positive = Positive::from_secs::<30>();

#[derive(Clone)]
pub struct Config {
    connections: side::Capacity,
    inflight: usize,
    admission_waiters: usize,
    request_frame_bytes: usize,
    response_items: usize,
    response_wire:
        bounded::NonZeroU32<{ FRAME_HEADER_BYTES as u32 }, { transfer::MAX_BYTES as u32 }>,
    pub(super) operation_window: Positive,
    memory_bound: Memory,
    runtime: Runtime,
}

#[derive(Clone, Copy)]
pub(super) struct Runtime {
    request_pool: pool::Layout,
    control_pool: pool::Layout,
    request_queue: fair::Plan,
    response_arena: layout::Layout,
    ready: plan::Plan,
    notifications: NonZero<u32>,
    groups: Option<groups::Layout>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Capacities {
    pub connections: usize,
    pub inflight: usize,
    pub admission_waiters: usize,
    pub requests: Request,
    pub responses: Response,
    pub notifications: Notification,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Request {
    pub frames: usize,
    pub frame_bytes: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Response {
    pub items: usize,
    pub bytes: usize,
    /// Largest complete backend frame, including type and length fields.
    pub frame_bytes: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Notification {
    pub items: usize,
    pub bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Groups {
    pub active: NonZero<usize>,
    pub records: NonZero<usize>,
    pub bytes: NonZero<usize>,
}

/// Physical upper bound for driver-retained PostgreSQL payloads and the exact
/// ready/transaction selector allocation. Other fixed metadata, credentials,
/// and values transferred into caller ownership are intentionally excluded.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Memory {
    pub request_bytes: usize,
    pub cancel_bytes: usize,
    /// Logical completed-response allowance.
    pub response_bytes: usize,
    pub ingress: Ingress,
    pub resident_bytes: limits::Bound,
    pub notification_bytes: usize,
    pub diagnostic_bytes: usize,
    pub total_bytes: limits::Bound,
}

impl Config {
    pub fn new(caps: Capacities) -> Result<Self, Error> {
        Self::with_window(caps, DEFAULT_OPERATION_WINDOW)
    }

    pub fn with_window(caps: Capacities, window: Positive) -> Result<Self, Error> {
        let Capacities {
            connections: conns,
            inflight,
            admission_waiters: waiters,
            requests:
                Request {
                    frames,
                    frame_bytes: frame,
                },
            responses:
                Response {
                    items,
                    bytes,
                    frame_bytes: wire,
                },
            notifications:
                Notification {
                    items: notices,
                    bytes: notice,
                },
        } = caps;
        let primary = conn::Capacity::try_from(conns)
            .map_err(|error| Error::Connections(Connection::Primary(error)))?;
        let sides = side::Capacity::try_new(primary)
            .map_err(|error| Error::Connections(Connection::Side(error)))?;
        let capacity = sides.primary();
        let conns = capacity.entries().get();
        if inflight < conns || inflight > frames {
            return Err(Error::Inflight {
                actual: inflight,
                minimum: conns,
                maximum: frames,
            });
        }
        if waiters == 0 {
            return Err(Error::AdmissionWaitersZero);
        }
        if frames > u32::MAX as usize / 2 {
            return Err(Error::RequestFrames {
                actual: frames,
                minimum: conns,
                maximum: u32::MAX as usize / 2,
            });
        }
        if frame == 0 || frame > transfer::MAX_BYTES {
            return Err(Error::RequestFrameBytes {
                actual: frame,
                maximum: transfer::MAX_BYTES,
            });
        }
        let pool = pool::Layout::new(frames, frame).map_err(Error::RequestPool)?;
        let control =
            pool::Layout::new(capacity.scale::<3>().get(), frame).map_err(Error::RequestPool)?;
        let queue = fair::Plan::new(frames, conns).map_err(Error::RequestQueue)?;
        if items < inflight || items > u32::MAX as usize {
            return Err(Error::ResponseItems {
                actual: items,
                minimum: inflight,
                maximum: u32::MAX as usize,
            });
        }
        if bytes == 0 {
            return Err(Error::ResponseBytesZero);
        }
        let bound = bounded::NonZeroU32::from_usize(wire).ok_or(Error::ResponseFrameBytes {
            actual: wire,
            minimum: FRAME_HEADER_BYTES,
            maximum: transfer::MAX_BYTES,
        })?;
        let payload = wire - FRAME_HEADER_BYTES;
        if payload > bytes {
            return Err(Error::ResponseFrameExceedsCapacity {
                frame_bytes: wire,
                payload_bytes: payload,
                available_payload_bytes: bytes,
            });
        }
        let notices = capacity::Count::try_from(notices)
            .map_err(Error::NotificationItems)?
            .nonzero()
            .ok_or(Error::NotificationItemsZero)?;
        if notice == 0 {
            return Err(Error::NotificationBytesZero);
        }
        let capacity = layout::Capacity::new(
            layout::Lanes::new(conns),
            layout::Entries::new(inflight),
            layout::Data::new(items),
            usage::Bytes::new(bytes),
            usage::Values::new(items),
        );
        let arena = layout::Layout::bounded(capacity).map_err(Error::Arena)?;
        let ready = plan::Plan::new(sides.primary(), arena).map_err(Error::Ready)?;
        let limit = arena.limit().bytes().get();
        if payload > limit {
            return Err(Error::ResponseFrameExceedsLaneCapacity {
                frame_bytes: wire,
                payload_bytes: payload,
                available_payload_bytes: limit,
            });
        }
        // Each admitted allocation fits isize::MAX, so their sum fits usize.
        let request = pool.bytes() + control.bytes();
        let cancel = conns
            .checked_mul(16)
            .ok_or(Error::CancelBytesOverflow { connections: conns })?;
        let retained = Layout::try_framed(memory::RETENTION, bytes, conns, wire, notice)
            .map_err(Error::Memory)?;
        let slots = items.checked_add(conns).ok_or(Error::OwnerSlotsOverflow {
            items,
            connections: conns,
        })?;
        let owners = slots
            .checked_add(notices.get() as usize)
            .ok_or(Error::OwnersOverflow {
                slots,
                notifications: notices.get() as usize,
            })?;
        let owners = capacity::Count::try_from(owners).map_err(Error::Owners)?;
        let ingress = retained.ingress(owners);
        let resident = retained.resident();
        let base = request
            .checked_add(cancel)
            .ok_or(Error::FixedBaseOverflow {
                request_bytes: request,
                cancel_bytes: cancel,
            })?;
        let fixed = base
            .checked_add(ready.bytes())
            .ok_or(Error::FixedReadyOverflow {
                base_bytes: base,
                ready_bytes: ready.bytes(),
            })?;
        let total = resident.try_add(fixed).map_err(Error::ResidentFixed)?;
        let total = total.try_add(wire).map_err(Error::ResidentDiagnostic)?;
        Ok(Self {
            connections: sides,
            inflight,
            admission_waiters: waiters,
            request_frame_bytes: frame,
            response_items: items,
            response_wire: bound,
            operation_window: window,
            memory_bound: Memory {
                request_bytes: request,
                cancel_bytes: cancel,
                response_bytes: bytes,
                ingress,
                resident_bytes: resident,
                notification_bytes: notice,
                diagnostic_bytes: wire,
                total_bytes: total,
            },
            runtime: Runtime {
                request_pool: pool,
                control_pool: control,
                request_queue: queue,
                response_arena: arena,
                ready,
                notifications: notices,
                groups: None,
            },
        })
    }

    pub const fn capacities(&self) -> Capacities {
        Capacities {
            connections: self.connections.primary().entries().get(),
            inflight: self.inflight,
            admission_waiters: self.admission_waiters,
            requests: Request {
                frames: self.runtime.request_pool.slots() as usize,
                frame_bytes: self.request_frame_bytes,
            },
            responses: Response {
                items: self.response_items,
                bytes: self.memory_bound.response_bytes,
                frame_bytes: self.response_wire.into_usize(),
            },
            notifications: Notification {
                items: self.runtime.notifications.get() as usize,
                bytes: self.memory_bound.notification_bytes,
            },
        }
    }

    /// Reserves fixed group storage without charging scalar or streaming calls.
    /// Replacements debit the prior group bytes; diagnostics remain bounded by
    /// the checked total, preserving every previously validated base limit.
    pub fn with_groups(mut self, limits: Groups) -> Result<Self, Error> {
        let layout = groups::Layout::try_new(limits)?;
        let limit = self.runtime.response_arena.limit();
        if limits.records.get() > limit.values().get() {
            return Err(Error::GroupRecordsCapacity {
                requested: limits.records,
                available: limit.values().get(),
            });
        }
        if limits.bytes.get() > limit.bytes().get() {
            return Err(Error::GroupBytesCapacity {
                requested: limits.bytes,
                available: limit.bytes().get(),
            });
        }
        let prior = match self.runtime.groups {
            Some(layout) => layout.allocated(),
            None => 0,
        };
        let base = limits::Bound::new(self.memory_bound.total_bytes.bytes() - prior);
        let total = base
            .try_add(layout.allocated())
            .map_err(Error::ResidentDiagnostic)?;
        let bytes = self.memory_bound.diagnostic_bytes - prior + layout.allocated();
        self.memory_bound.diagnostic_bytes = bytes;
        self.memory_bound.total_bytes = total;
        self.runtime.groups = Some(layout);
        Ok(self)
    }

    pub const fn groups(&self) -> Option<Groups> {
        match self.runtime.groups {
            Some(layout) => Some(layout.limits()),
            None => None,
        }
    }

    pub const fn memory_bound(&self) -> &Memory {
        &self.memory_bound
    }

    pub(crate) const fn frame_limit(&self) -> u32 {
        self.response_wire.get()
    }

    pub(crate) const fn ready_layout(&self) -> plan::Plan {
        self.runtime.ready
    }

    pub(crate) const fn notice_limit(&self) -> NonZero<u32> {
        self.runtime.notifications
    }

    /// Maximum connector sockets reserved by this port: one session and one
    /// bounded cancellation socket for every logical connection.
    pub const fn socket_limit(&self) -> usize {
        self.connections.primary().scale::<2>().get()
    }

    pub(crate) const fn side(&self) -> side::Capacity {
        self.connections
    }

    pub(super) const fn runtime(&self) -> &Runtime {
        &self.runtime
    }
}

impl Runtime {
    pub(crate) const fn groups(&self) -> Option<groups::Layout> {
        self.groups
    }
    pub(super) const fn request_pool(&self) -> pool::Layout {
        self.request_pool
    }

    pub(super) const fn control_pool(&self) -> pool::Layout {
        self.control_pool
    }

    pub(super) const fn queue(&self) -> fair::Plan {
        self.request_queue
    }

    pub(super) const fn arena(&self) -> layout::Layout {
        self.response_arena
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Connection {
    Primary(table::Invalid),
    Side(table::Invalid),
}
