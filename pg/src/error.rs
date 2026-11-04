use crate::scram::{self, Base64};
use crate::{
    port,
    protocol::{Reason, encode},
};
use cartel_core::{
    deadline::Elapsed,
    queue,
    reply::{self, layout, order, recv::attach, usage},
};
use dope::core::Errno;
use dope::core::driver::route;
use dope::{
    core::driver::schedule::clock::timer,
    manifold::client::{close, conn, deadline},
    net::link::egress::{self, load, reserve::exact},
};
use o3::base::num::bounded;
use o3::buffer::write::consume::scope;
use o3::depot::queue::indexed::shared;
use o3::{
    alloc,
    buffer::{hold, storage, write::consume},
    mem::rc,
};
use std::{mem::size_of, num};

#[derive(Clone, Copy, Debug)]
pub enum Error {
    Input(scope::Error),
    Closed,
    Retirement {
        slot: bounded::U32<0, { route::SLOT_MASK as u32 }>,
        phase: port::Retirement,
    },
    Connection(close::Reason),
    Receive(Errno),
    Transmit(conn::Transmit),
    Timeout(deadline::Kind),
    RequestTimeout(Elapsed),
    Auth(Reason),
    UnsupportedAuthMethod(u32),
    Random(Errno),
    ServerFirst(Reason),
    ServerFirstBase64(Base64),
    ServerFinal(Reason),
    ServerFinalBase64(Base64),
    ScramIterations {
        actual: u32,
        minimum: u32,
        maximum: u32,
    },
    Scram(scram::Error),
    ScramSlot(shared::Bounds),
    TransactionSlot(shared::Bounds),
    EgressSlot(shared::Bounds),
    Protocol(Reason),
    NotificationDropOverflow,
    DescriptionLtreeOidZero,
    DescriptionRowCountTruncated {
        actual: usize,
    },
    DescriptionRowCountNegative {
        actual: i16,
    },
    DescriptionColumnCount {
        actual: u16,
        expected: u16,
    },
    DescriptionDecoderOverflow {
        actual: usize,
        expected: u16,
    },
    DescriptionFieldName {
        actual: usize,
    },
    DescriptionFieldMetadata {
        actual: usize,
    },
    DescriptionFieldFormat {
        actual: i16,
    },
    DescriptionOid {
        actual: u32,
        expected: u32,
    },
    DescriptionStringOid {
        actual: u32,
    },
    DescriptionStringArrayOid {
        actual: u32,
    },
    DescriptionLtreeOid {
        actual: u32,
        expected: u32,
    },
    DescriptionUnvalidated {
        actual: u16,
        expected: u16,
    },
    DescriptionRowTrailing {
        actual: usize,
    },
    DescriptionParameterCountTruncated {
        actual: usize,
    },
    DescriptionParameterCountNegative {
        actual: i16,
    },
    DescriptionParameterCount {
        actual: u16,
        expected: usize,
    },
    DescriptionParameterOidTruncated {
        index: usize,
        actual: usize,
    },
    DescriptionParameterOid {
        index: usize,
        actual: u32,
        expected: u32,
    },
    DescriptionParameterTrailing {
        actual: usize,
    },
    DescriptionUnexpectedRows,
    QueryMetadata,
    UnexpectedStartupMessage(u8),
    UnexpectedSaslMessage(u8),
    UnexpectedEagerPrepareMessage(u8),
    UnexpectedReadyMessage(u8),
    NotFound,
    Deadline(timer::Exhausted),
    RequestEgress(egress::Rejection),
    RequestLoad(load::Error),
    RequestReserve(exact::Failure),
    RequestCapacity,
    RequestDepthOverflow {
        depth: num::NonZero<u32>,
    },
    GroupDisabled,
    GroupOwnerCapacity,
    GroupRecordCapacity,
    GroupBytesCapacity,
    GroupDiagnosticRecords {
        capacity: num::NonZero<usize>,
    },
    GroupDiagnosticBytes(consume::Error),
    GroupOpen {
        depth: num::NonZero<u32>,
    },
    RequestCloseDuringCopy {
        depth: num::NonZero<u32>,
    },
    RequestOpenDuringCopy {
        depth: num::NonZero<u32>,
    },
    CopyDuringPipeline {
        depth: num::NonZero<u32>,
    },
    CopyAlreadyOpen {
        depth: num::NonZero<u32>,
    },
    CopyFinishWithoutStart,
    CopyFinishDuringPipeline {
        depth: num::NonZero<u32>,
    },
    RequestQueue {
        queued: usize,
        cause: queue::Rejection,
    },
    Request(encode::Error),
    ReplyCapacity(usage::Capacity),
    ReplyFront(order::Front),
    ReplyFootprint {
        declared: usage::Usage,
        actual: usage::Usage,
    },
    Reply(attach::Error),
    ResponseRetention(hold::Error),
    ResponseRetain(rc::Error),
    Allocation(alloc::Error),
    StorageCapacity(consume::Error),
    StorageExhausted(alloc::Exhausted),
    Skipped,
    CommitRolledBack,
    Backpressure {
        inflight: reply::Length,
        transactions: num::NonZero<usize>,
        cap: layout::Entries,
    },
}

impl Error {
    pub(crate) fn try_copy(bytes: &[u8]) -> Result<Vec<u8>, Self> {
        let owned: Box<[u8]> =
            alloc::BoxSliceExt::try_copy(bytes).map_err(|error| Self::Allocation(error.into()))?;
        Ok(owned.into_vec())
    }

    pub(crate) fn try_string(value: &str) -> Result<String, Self> {
        alloc::StrExt::try_to_owned(value).map_err(|error| Self::Allocation(error.into()))
    }
}

impl From<storage::AllocError> for Error {
    fn from(error: storage::AllocError) -> Self {
        match error {
            storage::AllocError::Capacity(error) => Self::StorageCapacity(error),
            storage::AllocError::Exhausted(error) => Self::StorageExhausted(error),
        }
    }
}

const _: () = assert!(size_of::<Error>() == size_of::<egress::Rejection>());
