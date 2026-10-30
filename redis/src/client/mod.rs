//! Redis client capabilities issued by an exact protocol session.
//!
//! Driver storage cannot mint a client before attachment:
//!

use dope::core::Errno;

use o3::buffer::write::consume::scope;
use o3::buffer::{hold, resident};
use o3::depot::queue::indexed::shared;

use crate::api::{
    self,
    decode::{Adapter, Decode},
};
use crate::client::dispatch::command::{Command, Pooled};
use crate::protocol::{self, parser};
use crate::request::{self, encode};
use crate::value::{self, reply};
use cartel_core::{
    deadline::Elapsed,
    reply::{order, usage},
};
use dope::manifold::{
    app::receive,
    client::{close, deadline},
};
use dope::net::link::egress::{self, reserve::exact};
use dope::runtime::client;
use dope_fiber::abi;
use parser::limit;
use std::mem;

pub mod conn;
pub mod dispatch;
mod logic;
mod node;
pub mod nonempty;
pub mod ops;
pub mod plan;
pub mod settings;

use conn::acquire;
pub use logic::Logic;
pub use node::Node;

pub(super) const CONNECTOR_CONNECTION_LIMIT: usize = u32::MAX as usize;

type ReplyError<'b, 'c, 'd, const ID: u8 = 0> =
    reply::Failure<crate::Error<'d, ID>, value::Bytes<'b, 'c, 'd>>;

pub(crate) enum Failure<'d> {
    Input(scope::Error),
    ParserWait(shared::Bounds),
    Closed(close::Reason),
    Receive(Errno),
    Timeout(deadline::Kind),
    RequestTimeout(Elapsed),
    Protocol(protocol::Error),
    Handshake(crate::init::Failure<resident::Shared<'d>>),
    UnexpectedResponse,
    ResponseCapacity(usage::Capacity),
    ResponseFront(order::Front),
    Request(request::Error),
    RequestEgress(egress::Rejection),
    RequestReserve(exact::Failure),
    ResponseFrameCapacity(limit::Exceeded<limit::Frame>),
    ResponseRetention(hold::Error),
    ResponseValueCapacity(limit::Exceeded<limit::Value>),
}

impl<'d> Failure<'d> {
    pub(crate) const fn protocol(error: protocol::Failure) -> Self {
        match error {
            protocol::Failure::Invalid(error) => Self::Protocol(error),
            protocol::Failure::FrameCapacity(error) => Self::ResponseFrameCapacity(error),
            protocol::Failure::ValueCapacity(error) => Self::ResponseValueCapacity(error),
        }
    }

    pub(crate) fn from_close(reason: close::Reason) -> Self {
        if let close::Reason::Timeout(kind) = reason {
            Self::Timeout(kind)
        } else {
            Self::Closed(reason)
        }
    }

    pub(crate) fn close_reason(&self) -> close::Reason {
        match self {
            Self::Closed(reason) => *reason,
            Self::Receive(_) => close::Reason::Transport,
            Self::Timeout(kind) => close::Reason::Timeout(*kind),
            Self::RequestTimeout(_) => close::Reason::Timeout(deadline::Kind::Inbound),
            Self::Input(_)
            | Self::ParserWait(_)
            | Self::Protocol(_)
            | Self::Handshake(_)
            | Self::UnexpectedResponse
            | Self::ResponseFront(_) => close::Reason::Protocol,
            Self::ResponseCapacity(_) => close::Reason::Capacity(close::Capacity::IngressRetention),
            Self::ResponseRetention(error) => receive::Failure::Hold(*error).into(),
            Self::ResponseFrameCapacity(_) | Self::ResponseValueCapacity(_) => {
                close::Reason::Capacity(close::Capacity::IngressStaging)
            }
            Self::RequestEgress(error) => match error {
                egress::Rejection::Empty(_) | egress::Rejection::Changed { .. } => {
                    close::Reason::Local(close::LocalOrigin::ResourceFailure)
                }
                egress::Rejection::LaneUnavailable => close::Reason::Transport,
                egress::Rejection::Transfer(_)
                | egress::Rejection::Resident(_)
                | egress::Rejection::LaneCapacity { .. }
                | egress::Rejection::BatchCapacity { .. }
                | egress::Rejection::Capacity { .. } => {
                    close::Reason::Capacity(close::Capacity::Transport)
                }
            },
            Self::RequestReserve(_) => close::Reason::Capacity(close::Capacity::Transport),
            Self::Request(_) => close::Reason::Local(close::LocalOrigin::ResourceFailure),
        }
    }

    pub(crate) const fn start_policy(&self) -> close::Policy {
        match self {
            Self::Request(request::Error::Entries { .. }) => close::Policy::Reconnect,
            Self::Input(_)
            | Self::ParserWait(_)
            | Self::Protocol(_)
            | Self::Handshake(_)
            | Self::UnexpectedResponse
            | Self::Request(_)
            | Self::ResponseFrameCapacity(_)
            | Self::ResponseValueCapacity(_) => close::Policy::Permanent,
            Self::RequestEgress(
                egress::Rejection::LaneCapacity { .. }
                | egress::Rejection::BatchCapacity { .. }
                | egress::Rejection::Capacity { .. },
            )
            | Self::RequestReserve(_) => close::Policy::Permanent,
            Self::Closed(_)
            | Self::Receive(_)
            | Self::Timeout(_)
            | Self::RequestTimeout(_)
            | Self::ResponseCapacity(_)
            | Self::ResponseFront(_)
            | Self::RequestEgress(
                egress::Rejection::Empty(_)
                | egress::Rejection::Changed { .. }
                | egress::Rejection::LaneUnavailable
                | egress::Rejection::Transfer(_)
                | egress::Rejection::Resident(_),
            )
            | Self::ResponseRetention(_) => close::Policy::Reconnect,
        }
    }

    pub(crate) fn into_error<const ID: u8>(self) -> crate::Error<'d, ID> {
        match self {
            Self::Input(cause) => crate::Error::Input(cause),
            Self::ParserWait(cause) => crate::Error::ParserWait(cause),
            Self::Closed(reason) => crate::Error::Closed(reason),
            Self::Receive(error) => crate::Error::Receive(error),
            Self::Timeout(kind) => crate::Error::Timeout(kind),
            Self::RequestTimeout(due) => crate::Error::RequestTimeout(due),
            Self::Protocol(error) => crate::Error::Protocol(error),
            Self::Handshake(error) => crate::Error::Handshake(error),
            Self::UnexpectedResponse => crate::Error::UnexpectedResponse,
            Self::ResponseCapacity(cause) => crate::Error::ResponseCapacity(cause),
            Self::ResponseFront(front) => crate::Error::ResponseFront(front),
            Self::Request(error) => crate::Error::Request(error),
            Self::RequestEgress(cause) => crate::Error::RequestEgress(cause),
            Self::RequestReserve(cause) => crate::Error::RequestReserve(cause),
            Self::ResponseFrameCapacity(error) => crate::Error::ResponseFrameCapacity(error),
            Self::ResponseRetention(error) => crate::Error::ResponseRetention(error),
            Self::ResponseValueCapacity(error) => crate::Error::ResponseValueCapacity(error),
        }
    }
}

impl usage::Footprint for Failure<'_> {
    fn footprint(&self) -> usage::Usage {
        match self {
            Self::Handshake(error) => match &error.frame {
                Ok(frame) => usage::Usage::new(usage::Bytes::new(frame.len()), usage::Values::ZERO),
                Err(_) => usage::Usage::ZERO,
            },
            _ => usage::Usage::ZERO,
        }
    }
}

impl<'d> From<&Failure<'d>> for Failure<'d> {
    fn from(cause: &Failure<'d>) -> Self {
        match cause {
            Self::Input(error) => Self::Input(*error),
            Self::ParserWait(error) => Self::ParserWait(*error),
            Self::Closed(reason) => Self::Closed(*reason),
            Self::Receive(error) => Self::Receive(*error),
            Self::Timeout(kind) => Self::Timeout(*kind),
            Self::RequestTimeout(error) => Self::RequestTimeout(*error),
            Self::Protocol(error) => Self::Protocol(*error),
            Self::Handshake(error) => Self::Handshake(error.observe()),
            Self::UnexpectedResponse => Self::UnexpectedResponse,
            Self::ResponseCapacity(error) => Self::ResponseCapacity(*error),
            Self::ResponseFront(error) => Self::ResponseFront(*error),
            Self::Request(error) => Self::Request(*error),
            Self::RequestEgress(error) => Self::RequestEgress(*error),
            Self::RequestReserve(error) => Self::RequestReserve(*error),
            Self::ResponseFrameCapacity(error) => Self::ResponseFrameCapacity(*error),
            Self::ResponseRetention(error) => Self::ResponseRetention(*error),
            Self::ResponseValueCapacity(error) => Self::ResponseValueCapacity(*error),
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
pub struct GeoCoord {
    pub longitude: f64,
    pub latitude: f64,
}

impl GeoCoord {
    pub fn new(lon: f64, lat: f64) -> Self {
        Self {
            longitude: lon,
            latitude: lat,
        }
    }
}

#[repr(transparent)]
pub struct Redis<'app, 's, 'b, 'c, 'd: 'app, B: ?Sized, const ID: u8 = 0> {
    pub(super) port: client::Bound<'app, 'd, B, &'app crate::Port<'s, 'app, 'b, 'c, 'd, ID>>,
}

impl<B: ?Sized, const ID: u8> Copy for Redis<'_, '_, '_, '_, '_, B, ID> {}

impl<B: ?Sized, const ID: u8> Clone for Redis<'_, '_, '_, '_, '_, B, ID> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'app, 's, 'b, 'c: 'b, 'd: 'app + 'c, B: ?Sized, const ID: u8> api::Client<'d, ID>
    for Redis<'app, 's, 'b, 'c, 'd, B, ID>
{
    type Call<D: Decode<Self>> =
        Command<'app, 's, 'b, 'c, 'd, Pooled<'app, 's, 'b, 'c, 'd, B, ID>, Adapter<Self, D>, ID>;
    type Connection = conn::Connection<'app, 's, 'b, 'c, 'd, B, ID>;

    fn connection(
        self,
    ) -> impl abi::Fiber<
        'd,
        Output = Result<conn::Connection<'app, 's, 'b, 'c, 'd, B, ID>, crate::Error<'d, ID>>,
    > {
        acquire::Acquire::new(self)
    }

    /// Waits until this client's pool has an active Redis connection.
    ///
    /// The wait retains the application lease and cannot escape it:
    ///
    fn wait_active(self) -> impl abi::Fiber<'d, Output = Result<(), crate::Error<'d, ID>>> {
        use dope_fiber::task::done::Wait;

        let redis = self;
        Wait::new(move |mut cx, waiter| {
            use std::task::Poll;

            if redis.port.is_active() {
                return Poll::Ready(Ok(()));
            }
            if let Some(cause) = redis.port.activity.fatal_cause(cx.as_mut().region_token()) {
                return Poll::Ready(Err(cause));
            }
            match redis.port.activity.try_dispatch(waiter, cx.as_ref()) {
                Ok(()) => Poll::Pending,
                Err(error) => Poll::Ready(Err(crate::Error::Wait(error))),
            }
        })
    }
    fn encode<D: Decode<Self>>(
        self,
        encode: impl FnOnce(encode::Encoder) -> Result<encode::Complete, request::Error>,
    ) -> Self::Call<D> {
        let frame = self.port.encode(encode);
        Command::new(Pooled::new(self), frame)
    }

    fn encode_sized<D: Decode<Self>>(
        self,
        len: Result<encode::Length, request::Error>,
        encode: impl FnOnce(encode::Writer) -> Result<encode::Complete, request::Error>,
    ) -> Self::Call<D> {
        let frame = len.and_then(|len| self.port.encode_sized(len, encode));
        Command::new(Pooled::new(self), frame)
    }
}

const _: () = {
    let word = mem::size_of::<usize>();
    assert!(mem::size_of::<egress::Rejection>() == 7 * word);
    assert!(mem::size_of::<Failure<'static>>() == mem::size_of::<egress::Rejection>());
    assert!(mem::size_of::<Option<Failure<'static>>>() == mem::size_of::<Failure<'static>>());
    assert!(mem::size_of::<Redis<'static, 'static, 'static, 'static, 'static, ()>>() == word);
    assert!(
        mem::align_of::<Redis<'static, 'static, 'static, 'static, 'static, ()>>()
            == mem::align_of::<usize>()
    );
    assert!(
        mem::size_of::<Option<Redis<'static, 'static, 'static, 'static, 'static, ()>>>() == word
    );
    assert!(!mem::needs_drop::<
        Redis<'static, 'static, 'static, 'static, 'static, ()>,
    >());
};

type Snapshot<E> = service::Snapshot<
    service::Endpoint<
        <<E as env::Spec>::Transport as net::Transport>::Addr,
        <<E as env::Spec>::Transport as net::Transport>::Addr,
    >,
>;

use dope::manifold::{app::env, service};
use dope::net;
