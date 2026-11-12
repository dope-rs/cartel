use cartel_core::{memory, reply::layout};
use o3::{
    alloc,
    buffer::{hold::limits, pool},
    depot::{capacity, queue::pool::fair},
};
use std::num::NonZero;

use crate::port::{config::Connection, ready};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Connections(Connection),
    Inflight {
        actual: usize,
        minimum: usize,
        maximum: usize,
    },
    AdmissionWaitersZero,
    RequestFrames {
        actual: usize,
        minimum: usize,
        maximum: usize,
    },
    RequestFrameBytes {
        actual: usize,
        maximum: usize,
    },
    ResponseItems {
        actual: usize,
        minimum: usize,
        maximum: usize,
    },
    ResponseBytesZero,
    ResponseFrameBytes {
        actual: usize,
        minimum: usize,
        maximum: usize,
    },
    ResponseFrameExceedsCapacity {
        frame_bytes: usize,
        payload_bytes: usize,
        available_payload_bytes: usize,
    },
    ResponseFrameExceedsLaneCapacity {
        frame_bytes: usize,
        payload_bytes: usize,
        available_payload_bytes: usize,
    },
    NotificationItems(capacity::Error),
    NotificationItemsZero,
    NotificationBytesZero,
    CancelBytesOverflow {
        connections: usize,
    },
    OwnerSlotsOverflow {
        items: usize,
        connections: usize,
    },
    OwnersOverflow {
        slots: usize,
        notifications: usize,
    },
    Owners(capacity::Error),
    FixedBaseOverflow {
        request_bytes: usize,
        cancel_bytes: usize,
    },
    FixedReadyOverflow {
        base_bytes: usize,
        ready_bytes: usize,
    },
    RequestPool(pool::LayoutError),
    RequestQueue(fair::Error),
    Ready(ready::Error),
    Arena(layout::Error),
    Memory(memory::Error),
    ResidentFixed(limits::Error),
    ResidentDiagnostic(limits::Error),
    RetentionHeadroom(limits::Error),
    GroupSlots(capacity::Error),
    GroupRecordsOverflow {
        groups: NonZero<usize>,
        records: NonZero<usize>,
    },
    GroupBytesOverflow {
        groups: NonZero<usize>,
        bytes: NonZero<usize>,
    },
    GroupRecordsLayout(alloc::Overflow),
    GroupPool(pool::LayoutError),
    GroupRecordsCapacity {
        requested: NonZero<usize>,
        available: usize,
    },
    GroupBytesCapacity {
        requested: NonZero<usize>,
        available: usize,
    },
}
