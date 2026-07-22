use std::cell::{Cell, RefCell};
use std::pin::Pin;

use cartel_core::{
    Arena, ArenaPool, BoundedQueue, FatalSlot, ItemPool, LaneBudget, Limits, QueuePool, Registrable,
};
use dope::DriverRef;
use dope::driver::ready::ReadyKey;
use dope::driver::token::Token;
use dope::manifold::connector;
use dope_fiber::WaitQueue;
use o3::buffer::{Lease, Pool};

use crate::Error;
use crate::client::Config;
use crate::encode::Sink;
use crate::protocol::Outcome;

pub(super) struct Frame<'d> {
    buffer: Lease<'d>,
    overflowed: bool,
}

impl Frame<'_> {
    pub(super) fn cast<'a>(self) -> Frame<'a> {
        Frame {
            buffer: unsafe { std::mem::transmute::<Lease<'_>, Lease<'a>>(self.buffer) },
            overflowed: self.overflowed,
        }
    }

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
    requests: BoundedQueue<'d, Frame<'static>>,
    responses: Arena<'d, Outcome>,
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
    _request_queue: QueuePool<Frame<'static>>,
    inflight_capacity: usize,
    response_metadata: ArenaPool,
    _response_budget: LaneBudget,
    _response_items: ItemPool<Outcome>,
}

impl<'d> Port<'d> {
    pub(super) fn new(config: Config, driver: DriverRef<'d>) -> Self {
        let connection_count = config.connection_capacity();
        let response_metadata =
            ArenaPool::with_capacity(config.inflight_capacity(), connection_count);
        let response_budget =
            LaneBudget::with_capacity(config.response_byte_capacity(), connection_count);
        let response_items = ItemPool::with_credit_capacity(
            config.inflight_capacity(),
            config.response_value_capacity(),
            connection_count,
        );
        let request_queue = QueuePool::with_capacity(config.request_capacity(), connection_count);
        let limits = Limits::new(
            1,
            config.max_frame_capacity(),
            config.response_value_capacity(),
        );
        let metadata = unsafe { response_metadata.handle().assume_lifetime() };
        let conns = (0..connection_count)
            .map(|lane| {
                let requests = unsafe { request_queue.handle(lane).assume_lifetime() };
                let budget = unsafe { response_budget.handle(lane).assume_lifetime() };
                let items = unsafe { response_items.handle_for(lane).assume_lifetime() };
                Conn {
                    driver,
                    token: Cell::new(None),
                    ready: Cell::new(None),
                    requests: BoundedQueue::new(requests),
                    responses: Arena::with_fair_shared_pools(metadata, lane, limits, budget, items),
                }
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
            _request_queue: request_queue,
            inflight_capacity: config.inflight_capacity(),
            response_metadata,
            _response_budget: response_budget,
            _response_items: response_items,
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

    pub(super) fn activate(&self, token: Token, ready: ReadyKey<'d>) -> bool {
        let Some(conn) = self.conns.get(token.slot().raw() as usize) else {
            return false;
        };
        if conn.token.replace(Some(token)).is_none() {
            self.active.set(self.active.get() + 1);
            self.response_metadata.activate(token.slot().raw() as usize);
        }
        conn.ready.set(Some(ready));
        true
    }

    pub(super) fn deactivate(&self, token: Token) {
        let Some(conn) = self.conn(token) else {
            return;
        };
        conn.token.set(None);
        conn.ready.set(None);
        self.active.set(self.active.get() - 1);
        self.response_metadata
            .deactivate(token.slot().raw() as usize);
        conn.requests.clear();
    }

    pub(super) fn responses(&self, token: Token) -> Option<&Arena<'d, Outcome>> {
        Some(&self.conn(token)?.responses)
    }

    pub(super) fn frame(&self) -> Result<Frame<'_>, Error> {
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

    pub(super) fn encode(&self, encode: impl FnOnce(&mut Frame<'_>)) -> Result<Frame<'_>, Error> {
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
    ) -> Result<(), (Error, Frame<'d>)> {
        let Some(conn) = self.pick_conn() else {
            let error = if self.active() {
                Error::Backpressure {
                    inflight: self.inflight(),
                    capacity: self.response_capacity(),
                }
            } else {
                Error::Closed
            };
            return Err((error, frame));
        };
        if !reply.try_attach(&conn.responses) {
            return Err((
                Error::Backpressure {
                    inflight: self.inflight(),
                    capacity: self.response_capacity(),
                },
                frame,
            ));
        }
        conn.requests.push_reserved(frame.cast(), 0);
        conn.wake();
        Ok(())
    }

    pub(super) fn drain_requests(
        &'d self,
        token: Token,
        mut push: impl FnMut(Frame<'d>) -> Result<(), Frame<'d>>,
    ) -> connector::Requests {
        let Some(conn) = self.conn(token) else {
            return connector::Requests::default();
        };
        conn.requests
            .drain(|frame| push(frame.cast()).map_err(Frame::cast));
        connector::Requests::default()
    }

    fn conn(&self, token: Token) -> Option<&Conn<'d>> {
        let conn = self.conns.get(token.slot().raw() as usize)?;
        conn.matches(token).then_some(conn)
    }

    fn pick_conn(&self) -> Option<&Conn<'d>> {
        let lane = self.response_metadata.pick_active()?;
        let conn = &self.conns[lane];
        debug_assert!(conn.token.get().is_some());
        debug_assert!(conn.responses.can_register());
        debug_assert!(conn.requests.has_capacity());
        Some(conn)
    }

    fn inflight(&self) -> usize {
        self.response_metadata.len()
    }

    fn response_capacity(&self) -> usize {
        self.inflight_capacity
    }
}
