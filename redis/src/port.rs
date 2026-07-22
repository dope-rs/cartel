use std::cell::{Cell, RefCell};
use std::pin::Pin;

use cartel_core::{Arena, ArenaConfig, ArenaLane, FatalSlot, Limits, QueueArena, Registrable};
use dope::DriverRef;
use dope::driver::ready::ReadyKey;
use dope::driver::token::Token;
use dope::manifold::connector;
use dope_fiber::WaitQueue;
use o3::buffer::{Lease, Pool};
use o3::cell::RegionToken;

use crate::Error;
use crate::client::Config;
use crate::encode::Sink;
use crate::protocol::Outcome;

pub(super) struct Frame<'d> {
    buffer: Lease<'d>,
    overflowed: bool,
}

impl Frame<'_> {
    fn overflowed(&self) -> bool {
        self.overflowed
    }
}

impl AsRef<[u8]> for Frame<'_> {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_ref()
    }
}

impl Sink for Frame<'_> {
    fn push(&mut self, byte: u8) {
        self.overflowed |= self.buffer.try_push(byte).is_err();
    }

    fn extend_from_slice(&mut self, src: &[u8]) {
        self.overflowed |= self.buffer.try_extend_from_slice(src).is_err();
    }
}

struct Conn<'d> {
    driver: DriverRef<'d>,
    token: Cell<Option<Token>>,
    ready: Cell<Option<ReadyKey<'d>>>,
}

impl<'d> Conn<'d> {
    fn wake(&self) {
        if let Some(ready) = self.ready.get() {
            self.driver.activate_ready(ready);
        }
    }

    fn matches(&self, token: Token) -> bool {
        self.token.get() == Some(token)
    }
}

pub(super) struct Port<'d> {
    conns: Box<[Conn<'d>]>,
    active: Cell<usize>,
    active_waiters: Pin<Box<WaitQueue>>,
    fatal: RefCell<FatalSlot<Error>>,
    max_frame_capacity: usize,
    response_value_capacity: usize,
    requests: Pin<Box<Pool>>,
    request_queue: QueueArena<'d, Frame<'d>>,
    inflight_capacity: usize,
    responses: Arena<'d, Outcome>,
}

impl<'d> Port<'d> {
    pub(super) fn new(config: Config, driver: DriverRef<'d>) -> Self {
        let connection_count = config.connection_capacity();
        let request_queue = QueueArena::with_capacity(config.request_capacity(), connection_count);
        let limits = Limits::new(
            1,
            config.max_frame_capacity(),
            config.response_value_capacity(),
        );
        let responses = Arena::new(ArenaConfig::new(
            connection_count,
            config.inflight_capacity(),
            config.inflight_capacity(),
            config.response_byte_capacity(),
            config.response_value_capacity(),
            limits,
        ));
        let conns = (0..connection_count)
            .map(|_| Conn {
                driver,
                token: Cell::new(None),
                ready: Cell::new(None),
            })
            .collect();
        Self {
            conns,
            active: Cell::new(0),
            active_waiters: Box::pin(WaitQueue::with_capacity(config.waiter_capacity())),
            fatal: RefCell::new(FatalSlot::default()),
            max_frame_capacity: config.max_frame_capacity(),
            response_value_capacity: config.response_value_capacity(),
            requests: Box::pin(Pool::new(config.request_pool())),
            request_queue,
            inflight_capacity: config.inflight_capacity(),
            responses,
        }
    }

    pub(super) fn capacity(&self) -> usize {
        self.conns.len()
    }

    pub(super) fn active(&self) -> bool {
        self.active.get() != 0
    }

    pub(super) fn max_frame_capacity(&self) -> usize {
        self.max_frame_capacity
    }

    pub(super) fn response_value_capacity(&self) -> usize {
        self.response_value_capacity
    }

    pub(super) fn try_register_active(
        &self,
        waiter: Pin<&dope_fiber::Waiter<'d>>,
        context: Pin<&dope_fiber::Context<'_, 'd>>,
    ) -> bool {
        self.active_waiters.as_ref().try_register(waiter, context)
    }

    pub(super) fn fatal_message(&self) -> Option<String> {
        self.fatal.borrow().as_ref().map(ToString::to_string)
    }

    pub(super) fn clear_fatal(&self) {
        self.fatal.borrow_mut().clear();
    }

    pub(super) fn record_fatal(&self, error: Error) {
        self.fatal.borrow_mut().record(error);
    }

    pub(super) fn wake_active(&self) {
        self.active_waiters.as_ref().wake();
    }

    pub(super) fn activate(
        &self,
        token: Token,
        ready: ReadyKey<'d>,
        region: &mut RegionToken<'d>,
    ) -> bool {
        let Some(conn) = self.conns.get(token.slot().raw() as usize) else {
            return false;
        };
        if conn.token.replace(Some(token)).is_none() {
            self.active.set(self.active.get() + 1);
            self.responses.activate(region, token.slot().raw() as usize);
        }
        conn.ready.set(Some(ready));
        true
    }

    pub(super) fn deactivate(&'d self, token: Token, region: &mut RegionToken<'d>) {
        let Some(conn) = self.conn(token) else {
            return;
        };
        conn.token.set(None);
        conn.ready.set(None);
        self.active.set(self.active.get() - 1);
        self.responses
            .deactivate(region, token.slot().raw() as usize);
        self.request_queue
            .lane(token.slot().raw() as usize)
            .clear(region);
    }

    pub(super) fn responses(&'d self, token: Token) -> Option<ArenaLane<'d, Outcome>> {
        self.conn(token)?;
        Some(self.responses.lane(token.slot().raw() as usize))
    }

    pub(super) fn frame(&'d self) -> Result<Frame<'d>, Error> {
        let buffer = self
            .requests
            .as_ref()
            .try_acquire()
            .ok_or(Error::RequestEntryCapacity)?;
        Ok(Frame {
            buffer,
            overflowed: false,
        })
    }

    pub(super) fn encode(
        &'d self,
        encode: impl FnOnce(&mut Frame<'_>),
    ) -> Result<Frame<'d>, Error> {
        let mut frame = self.frame()?;
        encode(&mut frame);
        if frame.overflowed() {
            return Err(Error::RequestBufferCapacity);
        }
        Ok(frame)
    }

    pub(super) fn try_enqueue_reply(
        &'d self,
        frame: Frame<'d>,
        reply: &mut impl Registrable<'d, Outcome>,
        region: &mut RegionToken<'d>,
    ) -> Result<(), (Error, Frame<'d>)> {
        let Some((token, conn)) = self.pick_conn(region) else {
            let error = if self.active() {
                Error::Backpressure {
                    inflight: self.inflight(region),
                    capacity: self.response_capacity(),
                }
            } else {
                Error::Closed
            };
            return Err((error, frame));
        };
        let lane = token.slot().raw() as usize;
        if !reply.try_attach(region, self.responses.lane(lane)) {
            return Err((
                Error::Backpressure {
                    inflight: self.inflight(region),
                    capacity: self.response_capacity(),
                },
                frame,
            ));
        }
        self.request_queue
            .lane(lane)
            .push_reserved(region, frame, 0);
        conn.wake();
        Ok(())
    }

    pub(super) fn drain_requests(
        &'d self,
        token: Token,
        push: impl FnMut(Frame<'d>) -> Result<(), Frame<'d>>,
        region: &mut RegionToken<'d>,
    ) -> connector::Requests {
        if self.conn(token).is_none() {
            return connector::Requests::default();
        }
        self.request_queue
            .lane(token.slot().raw() as usize)
            .drain(region, push);
        connector::Requests::default()
    }

    fn conn(&self, token: Token) -> Option<&Conn<'d>> {
        let conn = self.conns.get(token.slot().raw() as usize)?;
        conn.matches(token).then_some(conn)
    }

    fn pick_conn(&'d self, region: &mut RegionToken<'d>) -> Option<(Token, &'d Conn<'d>)> {
        let lane = self.responses.pick_active(region)?;
        let conn = &self.conns[lane];
        let token = conn.token.get()?;
        debug_assert!(self.responses.can_register(region, lane));
        debug_assert!(self.request_queue.lane(lane).has_capacity(region));
        Some((token, conn))
    }

    fn inflight(&self, region: &mut RegionToken<'d>) -> usize {
        self.responses.inflight(region)
    }

    fn response_capacity(&self) -> usize {
        self.inflight_capacity
    }
}
