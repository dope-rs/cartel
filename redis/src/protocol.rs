use std::fmt;

use cartel_core::{FatalSlot, Slab};
use dope::WakerSet;
use dope::manifold::connector;
use dope::manifold::connector::{Close, Ctx};
use dope::runtime::token::Token;
use o3::buffer;

use crate::decode::parse_one_bytes;
use crate::value::Value;

#[derive(Debug)]
pub enum Error {
    InvalidInteger,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInteger => f.write_str("invalid integer"),
        }
    }
}

pub enum Head {
    Reply(Value),
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

pub(super) type Outcome = Result<Value, crate::Error>;

pub(super) struct Shared {
    pub(super) slab: Slab<Outcome>,
    pub(super) conn_id: Option<Token>,
    pub(super) max_inflight: usize,
    pub(super) active_wakers: WakerSet,
    pub(super) fatal: FatalSlot<crate::Error>,
}

impl Shared {
    fn new() -> Self {
        Self {
            slab: Slab::new(),
            conn_id: None,
            max_inflight: usize::MAX,
            active_wakers: WakerSet::new(),
            fatal: FatalSlot::default(),
        }
    }
}

pub struct Codec;

impl connector::Codec for Codec {
    type Head = Head;
    type ParseState = ();

    fn parse(&self, _state: &mut (), buf: &buffer::Shared) -> Option<(Head, usize)> {
        match parse_one_bytes(buf) {
            Ok(Some((value, consumed))) => Some((Head::Reply(value), consumed)),
            Ok(None) => None,
            Err(e) => Some((Head::Fatal(e), buf.as_slice().len().max(1))),
        }
    }
}

pub struct Session {
    codec: Codec,
    pub(super) shared: Shared,
}

impl Session {
    pub fn new() -> Self {
        Self {
            codec: Codec,
            shared: Shared::new(),
        }
    }
}

impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}

impl connector::Session for Session {
    type Codec = Codec;
    type ConnState = ConnState;

    fn codec(&self) -> &Codec {
        &self.codec
    }

    fn connect(&mut self, ctx: &mut Ctx<'_, Self>) {
        ctx.state.poisoned = false;
        self.shared.conn_id = Some(ctx.conn_id);
        self.shared.fatal.clear();
        self.shared.active_wakers.drain_wake();
    }

    fn response(&mut self, head: Head, ctx: &mut Ctx<'_, Self>) {
        match head {
            Head::Reply(value) => {
                self.shared.slab.push(Ok(value));
                self.shared.slab.complete();
            }
            Head::Fatal(err) => {
                let msg = err.to_string();
                self.shared.fatal.record(err);
                ctx.state.poisoned = true;
                self.shared.conn_id = None;
                self.shared
                    .slab
                    .fail_all(|| Err(crate::Error::Redis(msg.clone())));
                self.shared.active_wakers.drain_wake();
            }
        }
    }

    fn disconnect(&mut self, ctx: &mut Ctx<'_, Self>) {
        let _ = ctx;
        self.shared.conn_id = None;
        let msg = self
            .shared
            .fatal
            .as_ref()
            .map(|e| e.to_string())
            .unwrap_or_else(|| "connection closed".to_string());
        self.shared
            .slab
            .fail_all(|| Err(crate::Error::Redis(msg.clone())));
        self.shared.active_wakers.drain_wake();
    }
}

#[cfg(test)]
mod tests {
    use dope::manifold::connector::Codec as _;

    use super::*;

    #[test]
    fn malformed_frame_maps_to_fatal_not_reply() {
        let codec = Codec;
        let buf = buffer::Shared::copy_from_slice(b"?bogus\r\n");
        let (head, consumed) = codec.parse(&mut (), &buf).expect("decoder yields head");
        assert!(matches!(head, Head::Fatal(_)));
        assert_eq!(consumed, buf.as_slice().len());
    }

    #[test]
    fn valid_frame_maps_to_reply() {
        let codec = Codec;
        let buf = buffer::Shared::copy_from_slice(b"+OK\r\n");
        let (head, _) = codec.parse(&mut (), &buf).expect("decoder yields head");
        assert!(matches!(head, Head::Reply(Value::Ok)));
    }

    #[test]
    fn partial_frame_yields_none() {
        let codec = Codec;
        let buf = buffer::Shared::copy_from_slice(b"$5\r\nhel");
        assert!(codec.parse(&mut (), &buf).is_none());
    }
}
