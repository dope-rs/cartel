use std::fmt;

use dope::driver::token::Token;
use dope::manifold::connector;
use dope::manifold::connector::{Close, Ctx};
use o3::buffer;
use o3::cell::RegionToken;

use crate::decode::{ParseState, Scan, Scanned, scan};
use crate::port::{Frame as SendFrame, Port};

#[derive(Debug)]
pub enum Error {
    InvalidInteger,
    InvalidLength,
    UnknownMarker,
    NestingDepth,
    BulkTerminator,
    TrailingBytes,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInteger => f.write_str("invalid integer"),
            Self::InvalidLength => f.write_str("invalid length"),
            Self::UnknownMarker => f.write_str("unknown RESP marker"),
            Self::NestingDepth => f.write_str("RESP nesting capacity exceeded"),
            Self::BulkTerminator => f.write_str("missing CRLF after bulk payload"),
            Self::TrailingBytes => f.write_str("RESP frame has trailing bytes"),
        }
    }
}

impl std::error::Error for Error {}

pub enum Head {
    Reply(Scanned),
    Fatal(crate::Error),
}

#[derive(Default)]
pub struct ConnState {
    poisoned: bool,
}

impl connector::Lifecycle for ConnState {
    fn wants_close(&self) -> Close {
        if self.poisoned {
            Close::Reconnect
        } else {
            Close::Keep
        }
    }

    fn defer_close(&self) -> bool {
        false
    }

    fn is_drained(&self) -> bool {
        true
    }
}

pub(super) type Outcome = Result<Scanned, crate::Error>;

pub struct Codec {
    max_frame_capacity: usize,
    response_value_capacity: usize,
}

impl Codec {
    pub fn new(max_frame_capacity: usize, response_value_capacity: usize) -> Self {
        Self {
            max_frame_capacity,
            response_value_capacity,
        }
    }
}

impl connector::Codec for Codec {
    type Head = Head;
    type ParseState = ParseState;

    fn parse(&self, state: &mut ParseState, buf: &buffer::Shared) -> Option<(Head, usize)> {
        match scan(
            state,
            buf,
            self.max_frame_capacity,
            self.response_value_capacity,
        ) {
            Scan::Pending => None,
            Scan::Complete(frame, consumed) => Some((Head::Reply(frame), consumed)),
            Scan::Invalid(error, consumed) => {
                *state = ParseState::default();
                Some((Head::Fatal(crate::Error::Protocol(error)), consumed.max(1)))
            }
            Scan::FrameCapacity(consumed) => {
                *state = ParseState::default();
                Some((
                    Head::Fatal(crate::Error::ResponseFrameCapacity),
                    consumed.max(1),
                ))
            }
            Scan::ValueCapacity(consumed) => {
                *state = ParseState::default();
                Some((
                    Head::Fatal(crate::Error::ResponseValueCapacity),
                    consumed.max(1),
                ))
            }
        }
    }
}

pub(super) struct Session<'d> {
    codec: Codec,
    port: &'d Port<'d>,
}

impl<'d> Session<'d> {
    pub(super) fn new(port: &'d Port<'d>) -> Self {
        Self {
            codec: Codec::new(port.max_frame_capacity(), port.response_value_capacity()),
            port,
        }
    }

    fn fail(&self, token: Token, error: crate::Error, region: &mut RegionToken<'d>) {
        let message = error.to_string();
        self.port.record_fatal(error);
        if let Some(responses) = self.port.responses(token) {
            responses.fail_all(region, || Err(crate::Error::Redis(message.clone())));
        }
        self.port.wake_active();
    }
}

impl<'d> connector::Session<'d> for Session<'d> {
    type Codec = Codec;
    type ConnState = ConnState;
    type Send = SendFrame<'d>;

    fn codec(&self) -> &Self::Codec {
        &self.codec
    }

    fn activate(
        &self,
        token: Token,
        ready: dope::driver::ready::ReadyKey<'d>,
        region: &mut RegionToken<'d>,
    ) {
        assert!(self.port.activate(token, ready, region));
    }

    fn connect(&mut self, ctx: &mut Ctx<'_, 'd, Self>) {
        ctx.state.poisoned = false;
        self.port.clear_fatal();
        self.port.wake_active();
    }

    fn response(&mut self, head: Head, ctx: &mut Ctx<'_, 'd, Self>) {
        match head {
            Head::Reply(value) => {
                let bytes = value.frame_len();
                let credits = value.value_count();
                let Some(responses) = self.port.responses(ctx.conn_id) else {
                    return;
                };
                responses.try_push(ctx.region, Ok(value), bytes, credits);
                responses.complete(ctx.region);
            }
            Head::Fatal(error) => {
                ctx.state.poisoned = true;
                self.fail(ctx.conn_id, error, ctx.region);
            }
        }
    }

    fn disconnect(&mut self, ctx: &mut Ctx<'_, 'd, Self>) {
        let message = self
            .port
            .fatal_message()
            .unwrap_or_else(|| "connection closed".to_string());
        if let Some(responses) = self.port.responses(ctx.conn_id) {
            responses.fail_all(ctx.region, || Err(crate::Error::Redis(message.clone())));
        }
        self.port.deactivate(ctx.conn_id, ctx.region);
        self.port.wake_active();
    }

    fn drain_requests(
        &self,
        token: Token,
        push: impl FnMut(Self::Send) -> Result<(), Self::Send>,
        region: &mut RegionToken<'d>,
    ) -> connector::Requests {
        self.port.drain_requests(token, push, region)
    }

    fn defer_close(
        &self,
        token: Token,
        _state: &Self::ConnState,
        region: &mut RegionToken<'d>,
    ) -> bool {
        self.port
            .responses(token)
            .is_some_and(|responses| !responses.is_empty(region))
    }

    fn is_drained(
        &self,
        token: Token,
        _state: &Self::ConnState,
        region: &mut RegionToken<'d>,
    ) -> bool {
        self.port
            .responses(token)
            .is_none_or(|responses| responses.is_empty(region))
    }
}
