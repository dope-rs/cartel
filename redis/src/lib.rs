pub mod api;
pub mod client;
pub mod init;
mod port;
pub use port::Port;
#[doc(hidden)]
pub mod protocol;
pub mod request;
pub mod value;

mod error;
pub use error::Error;

pub use cartel_core::deadline::RequestPhase;

use cartel_core::{deadline::Elapsed, queue, reply::recv::attach};
use dope::core::driver::schedule::clock::timer;
use dope::core::{Errno, driver::route};
use dope::manifold::client::{attempt, close, conn, deadline};
use dope::manifold::service::bindings::endpoint;
use dope::net::link::{
    egress::{self, reserve::exact},
    pool::change::opening,
};
use dope_fiber::{
    reply::{order, usage},
    wait,
};
use o3::{
    alloc,
    buffer::{hold, write::consume, write::consume::scope},
    depot::queue::indexed::shared,
    mem::rc,
};
use protocol::parser::limit;

type Owner<'r, 's, 'storage, 'b, 'c, 'd, const ID: u8> = endpoint::State<
    'r,
    'd,
    Port<'s, 'storage, 'b, 'c, 'd, ID>,
    client::Logic<'s, 'd>,
    route::KeyTag<ID>,
    { client::CONNECTOR_CONNECTION_LIMIT },
>;

#[derive(Clone, Copy, Debug)]
pub enum Block {
    Inactive,
    Pool,
    Queue(queue::Rejection),
}

/// Borrows an exact failure without moving its response or wake capabilities.
///
/// Wait failures expose their existing cause projection. Their retained wake
/// capability remains in the original [`Error`].
#[derive(Debug)]
pub enum Diagnostic<'a, 'd, const ID: u8 = 0> {
    Input(&'a scope::Error),
    ParserWait(&'a shared::Bounds),
    Unavailable,
    ConnectionExhausted {
        key: &'a attempt::Id<'d, route::KeyTag<ID>>,
        cause: &'a opening::Exhaustion,
    },
    Closed(&'a close::Reason),
    Receive(&'a Errno),
    Transmit(&'a conn::Transmit),
    Timeout(&'a deadline::Kind),
    RequestTimeout(&'a Elapsed),
    Protocol(&'a protocol::Error),
    Allocation,
    Exhausted(&'a alloc::Exhausted),
    Handshake(init::Failure<&'a [u8]>),
    UnexpectedResponse,
    ReplySlotEmpty,
    Reply(&'a attach::Error),
    Dispatch {
        blocked: &'a Block,
        wait: wait::Cause,
    },
    ResponseCapacity(&'a usage::Capacity),
    ResponseFront(&'a order::Front),
    Wait(wait::Cause),
    Deadline(&'a timer::Exhausted),
    Request(&'a request::Error),
    RequestEgress(&'a egress::Rejection),
    RequestReserve(&'a exact::Failure),
    ResponseFrameCapacity(&'a limit::Exceeded<limit::Frame>),
    ResponseRetain(&'a rc::Error),
    ResponseRetention(&'a hold::Error),
    ResponseStorageCapacity(&'a consume::Error),
    ResponseValueCapacity(&'a limit::Exceeded<limit::Value>),
}
