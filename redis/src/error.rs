use dope::core::{Errno, driver::route};
use o3::buffer::write::consume::scope;
use o3::depot::queue::indexed::shared;
use o3::{
    alloc,
    buffer::{hold, resident, storage, write::consume},
    mem::rc,
};

use crate::{init, protocol, protocol::parser::limit, request};
use cartel_core::{deadline::Elapsed, reply::recv::attach};
use dope::core::driver::schedule::clock::timer;
use dope::manifold::client::{attempt, close, deadline};
use dope::net::link::{
    egress::{self, reserve::exact},
    pool::change::opening,
};
use dope_fiber::{
    reply::{order, usage},
    wait,
};

use crate::{Block, Diagnostic, value::source};

/// The originating driver and route identity remain invariant through reporting.
pub enum Error<'d, const ID: u8 = 0> {
    Input(scope::Error),
    ParserWait(shared::Bounds),
    Unavailable,
    ConnectionExhausted {
        key: attempt::Id<'d, route::KeyTag<ID>>,
        cause: opening::Exhaustion,
    },
    Closed(close::Reason),
    Receive(Errno),
    Timeout(deadline::Kind),
    RequestTimeout(Elapsed),
    Protocol(protocol::Error),
    Allocation,
    Exhausted(alloc::Exhausted),
    Handshake(init::Failure<resident::Shared<'d>>),
    UnexpectedResponse,
    ReplySlotEmpty,
    Reply(attach::Error),
    Dispatch {
        blocked: Block,
        wait: wait::Error<'d>,
    },
    ResponseCapacity(usage::Capacity),
    ResponseFront(order::Front),
    Wait(wait::Error<'d>),
    Deadline(timer::Exhausted),
    Request(request::Error),
    RequestEgress(egress::Rejection),
    RequestReserve(exact::Failure),
    ResponseFrameCapacity(limit::Exceeded<limit::Frame>),
    ResponseRetain(rc::Error),
    ResponseRetention(hold::Error),
    ResponseStorageCapacity(consume::Error),
    ResponseValueCapacity(limit::Exceeded<limit::Value>),
}

impl<const ID: u8> source::Sealed for Error<'_, ID> {}

impl<'d, const ID: u8> source::Control for Error<'d, ID> {
    type Diagnostic<'a>
        = Diagnostic<'a, 'd, ID>
    where
        Self: 'a;

    fn diagnostic(&self) -> Diagnostic<'_, 'd, ID> {
        match self {
            Self::Input(error) => Diagnostic::Input(error),
            Self::ParserWait(error) => Diagnostic::ParserWait(error),
            Self::Unavailable => Diagnostic::Unavailable,
            Self::ConnectionExhausted { key, cause } => {
                Diagnostic::ConnectionExhausted { key, cause }
            }
            Self::Closed(error) => Diagnostic::Closed(error),
            Self::Receive(error) => Diagnostic::Receive(error),
            Self::Timeout(error) => Diagnostic::Timeout(error),
            Self::RequestTimeout(error) => Diagnostic::RequestTimeout(error),
            Self::Protocol(error) => Diagnostic::Protocol(error),
            Self::Allocation => Diagnostic::Allocation,
            Self::Exhausted(error) => Diagnostic::Exhausted(error),
            Self::Handshake(error) => Diagnostic::Handshake(error.as_ref()),
            Self::UnexpectedResponse => Diagnostic::UnexpectedResponse,
            Self::ReplySlotEmpty => Diagnostic::ReplySlotEmpty,
            Self::Reply(error) => Diagnostic::Reply(error),
            Self::Dispatch { blocked, wait } => Diagnostic::Dispatch {
                blocked,
                wait: wait.cause(),
            },
            Self::ResponseCapacity(error) => Diagnostic::ResponseCapacity(error),
            Self::ResponseFront(error) => Diagnostic::ResponseFront(error),
            Self::Wait(error) => Diagnostic::Wait(error.cause()),
            Self::Deadline(error) => Diagnostic::Deadline(error),
            Self::Request(error) => Diagnostic::Request(error),
            Self::RequestEgress(error) => Diagnostic::RequestEgress(error),
            Self::RequestReserve(error) => Diagnostic::RequestReserve(error),
            Self::ResponseFrameCapacity(error) => Diagnostic::ResponseFrameCapacity(error),
            Self::ResponseRetain(error) => Diagnostic::ResponseRetain(error),
            Self::ResponseRetention(error) => Diagnostic::ResponseRetention(error),
            Self::ResponseStorageCapacity(error) => Diagnostic::ResponseStorageCapacity(error),
            Self::ResponseValueCapacity(error) => Diagnostic::ResponseValueCapacity(error),
        }
    }
}

impl<'d, const ID: u8> From<Elapsed> for Error<'d, ID> {
    fn from(due: Elapsed) -> Self {
        Self::RequestTimeout(due)
    }
}

impl<'d, const ID: u8> From<request::Error> for Error<'d, ID> {
    fn from(error: request::Error) -> Self {
        Self::Request(error)
    }
}

impl<'d, const ID: u8> From<storage::AllocError> for Error<'d, ID> {
    fn from(error: storage::AllocError) -> Self {
        match error {
            storage::AllocError::Capacity(error) => Self::ResponseStorageCapacity(error),
            storage::AllocError::Exhausted(error) => Self::Exhausted(error),
        }
    }
}
