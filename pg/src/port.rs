use std::cell::Cell;
use std::marker::PhantomData;
use std::pin::Pin;

use cartel_core::{
    Arena, ArenaPool, ArenaPoolRef, BoundedQueue, ItemPool, ItemPoolRef, LaneBudget, LaneBudgetRef,
    Limits, QueuePool, QueuePoolRef, Registrable,
};
use dope::driver::ready::ReadyKey;
use dope::driver::token::Token;
use dope::manifold::connector::Connector;
use dope::manifold::connector::source::Dialer;
use dope::manifold::env::Env;
use dope::runtime::StorageFactory;
use dope::{DriverContext, DriverRef};
use dope_net::Transport;
use o3::buffer::{Lease, Pool, PoolLayout};

use crate::protocol::{RowItem, Session, Shared as PoolState};
use crate::query::QuerySet;
use crate::wire::Sink;
use crate::{Config as DatabaseConfig, Error};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    capacities: Capacities,
    request_pool: PoolLayout,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Capacities {
    pub connections: usize,
    pub request_entries: usize,
    pub request_bytes: usize,
    pub response_entries: usize,
    pub response_bytes: usize,
    pub inflight: usize,
    pub waiters: usize,
    pub notifications: usize,
}

impl Config {
    pub fn new(capacities: Capacities) -> Result<Self, ConfigError> {
        let Capacities {
            connections: connection_capacity,
            request_entries: request_capacity,
            request_bytes: request_byte_capacity,
            response_entries: response_capacity,
            response_bytes: response_byte_capacity,
            inflight: inflight_capacity,
            waiters: waiter_capacity,
            notifications: notification_capacity,
        } = capacities;
        if connection_capacity == 0 || u32::try_from(connection_capacity).is_err() {
            return Err(ConfigError::Connections);
        }
        if request_capacity < connection_capacity || request_capacity > u32::MAX as usize / 2 {
            return Err(ConfigError::RequestEntries);
        }
        if request_byte_capacity == 0 || u32::try_from(request_byte_capacity).is_err() {
            return Err(ConfigError::RequestBytes);
        }
        let request_pool = PoolLayout::new(request_capacity as u32, request_byte_capacity as u32)
            .map_err(|_| ConfigError::RequestBytes)?;
        if response_capacity < connection_capacity || u32::try_from(response_capacity).is_err() {
            return Err(ConfigError::ResponseEntries);
        }
        let max_response_bytes = (u32::MAX as usize)
            .saturating_sub(4)
            .min(usize::MAX.saturating_sub(5));
        if response_byte_capacity < connection_capacity
            || response_byte_capacity > max_response_bytes
        {
            return Err(ConfigError::ResponseBytes);
        }
        if inflight_capacity < connection_capacity || inflight_capacity > request_capacity {
            return Err(ConfigError::Inflight);
        }
        if waiter_capacity == 0
            || waiter_capacity
                .checked_mul(2)
                .and_then(usize::checked_next_power_of_two)
                .is_none()
        {
            return Err(ConfigError::Waiters);
        }
        if notification_capacity == 0 || notification_capacity.checked_next_power_of_two().is_none()
        {
            return Err(ConfigError::Notifications);
        }
        Ok(Self {
            capacities,
            request_pool,
        })
    }

    pub const fn connection_capacity(self) -> usize {
        self.capacities.connections
    }

    pub const fn request_capacity(self) -> usize {
        self.capacities.request_entries
    }

    pub const fn request_byte_capacity(self) -> usize {
        self.capacities.request_bytes
    }

    pub const fn request_pool(self) -> PoolLayout {
        self.request_pool
    }

    pub const fn response_capacity(self) -> usize {
        self.capacities.response_entries
    }

    pub const fn response_byte_capacity(self) -> usize {
        self.capacities.response_bytes
    }

    pub const fn inflight_capacity(self) -> usize {
        self.capacities.inflight
    }

    pub const fn waiter_capacity(self) -> usize {
        self.capacities.waiters
    }

    pub const fn notification_capacity(self) -> usize {
        self.capacities.notifications
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigError {
    Connections,
    RequestEntries,
    RequestBytes,
    ResponseEntries,
    ResponseBytes,
    Inflight,
    Waiters,
    Notifications,
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Connections => "connection capacity must be positive and fit in u32",
            Self::RequestEntries => {
                "request entries must cover all connections and fit their backing pools"
            }
            Self::RequestBytes => "request byte capacity must be positive and fit in u32",
            Self::ResponseEntries => "response entries must cover all connections and fit in u32",
            Self::ResponseBytes => {
                "response byte capacity must be positive and fit a PostgreSQL frame"
            }
            Self::Inflight => "inflight capacity must be positive and not exceed request entries",
            Self::Waiters => "waiter capacity must be positive and fit its backing set",
            Self::Notifications => {
                "notification capacity must be positive and fit its backing queue"
            }
        })
    }
}

impl std::error::Error for ConfigError {}

pub struct Frame<'d> {
    buffer: Lease<'d>,
    overflowed: bool,
}

impl Frame<'_> {
    fn cast<'a>(self) -> Frame<'a> {
        Frame {
            buffer: unsafe { std::mem::transmute::<Lease<'_>, Lease<'a>>(self.buffer) },
            overflowed: self.overflowed,
        }
    }

    pub(super) fn overflowed(&self) -> bool {
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

    fn len(&self) -> usize {
        self.buffer.len()
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        self.buffer.as_mut_slice()
    }
}

#[derive(Clone, Copy)]
pub(super) enum Boundary {
    Close,
    Open,
    External,
}

struct Conn<'d> {
    driver: DriverRef<'d>,
    token: Cell<Option<Token>>,
    ready: Cell<Option<ReadyKey<'d>>>,
    requests: BoundedQueue<'d, Frame<'static>>,
    close: Cell<bool>,
    responses: Arena<'d, RowItem>,
    unsynced: Cell<u32>,
    batch_open: Cell<bool>,
}

impl<'d> Conn<'d> {
    fn new(
        lane: usize,
        requests: QueuePoolRef<'d, Frame<'static>>,
        limits: Limits,
        budget: LaneBudgetRef<'d>,
        responses: ItemPoolRef<'d, RowItem>,
        metadata: ArenaPoolRef<'d>,
        driver: DriverRef<'d>,
    ) -> Self {
        Self {
            driver,
            token: Cell::new(None),
            ready: Cell::new(None),
            requests: BoundedQueue::new(requests),
            close: Cell::new(false),
            responses: Arena::with_fair_shared_pools(metadata, lane, limits, budget, responses),
            unsynced: Cell::new(0),
            batch_open: Cell::new(false),
        }
    }

    fn matches(&self, token: Token) -> bool {
        self.token.get() == Some(token)
    }

    fn wake(&self) {
        if let Some(ready) = self.ready.get() {
            self.driver.activate_ready(ready);
        }
    }
}

pub struct Port<'d, I: QuerySet> {
    pub(super) shared: PoolState,
    conns: Box<[Conn<'d>]>,
    requests: Pin<Box<Pool>>,
    _request_queue: QueuePool<Frame<'static>>,
    _response_metadata: ArenaPool,
    _response_budget: LaneBudget,
    _response_rows: ItemPool<RowItem>,
    _instance: PhantomData<fn() -> I>,
}

pub struct PortFactory<I> {
    database: DatabaseConfig,
    config: Config,
    instance: PhantomData<fn() -> I>,
}

impl<I> PortFactory<I> {
    pub fn config(&self) -> Config {
        self.config
    }
}

impl<'d, I: QuerySet> Port<'d, I> {
    pub fn factory(database: DatabaseConfig, config: Config) -> PortFactory<I> {
        PortFactory {
            database,
            config,
            instance: PhantomData,
        }
    }

    pub fn new(database: DatabaseConfig, config: Config, driver: DriverRef<'d>) -> Self {
        let connections = config.connection_capacity();
        let request_entries = config.request_capacity();
        let limits = Limits::new(
            config.response_capacity(),
            config.response_byte_capacity(),
            config.response_capacity(),
        );
        let request_queue = QueuePool::with_capacity(request_entries, connections);
        let response_budget =
            LaneBudget::with_capacity(config.response_byte_capacity(), connections);
        let response_rows = ItemPool::with_lanes(config.response_capacity(), connections);
        let response_metadata = ArenaPool::with_capacity(config.inflight_capacity(), connections);
        let metadata = unsafe { response_metadata.handle().assume_lifetime() };
        Self {
            shared: PoolState::new(database, config),
            conns: (0..connections)
                .map(|lane| {
                    let requests = unsafe { request_queue.handle(lane).assume_lifetime() };
                    let budget = unsafe { response_budget.handle(lane).assume_lifetime() };
                    let rows = unsafe { response_rows.handle_for(lane).assume_lifetime() };
                    Conn::new(lane, requests, limits, budget, rows, metadata, driver)
                })
                .collect(),
            requests: Box::pin(Pool::new(config.request_pool())),
            _request_queue: request_queue,
            _response_metadata: response_metadata,
            _response_budget: response_budget,
            _response_rows: response_rows,
            _instance: PhantomData,
        }
    }

    pub fn capacity(&self) -> usize {
        self.shared.config.connection_capacity()
    }

    pub fn config(&self) -> Config {
        self.shared.config
    }

    pub fn connect<const ID: u8, S, E>(
        &'d self,
        upstreams: S,
        driver: &mut DriverContext<'_, 'd>,
    ) -> std::io::Result<Connector<'d, ID, Session<'d, I>, S, E>>
    where
        S: Dialer<E::Transport> + 'd,
        E: Env + 'd,
        E::Transport: Transport,
    {
        Connector::new(
            Session::new(self),
            upstreams,
            self.config().connection_capacity(),
            driver,
        )
    }

    fn conn(&self, token: Token) -> Option<&Conn<'d>> {
        let conn = self.conns.get(token.slot().raw() as usize)?;
        conn.matches(token).then_some(conn)
    }

    pub(super) fn activate(&self, token: Token, ready: ReadyKey<'d>) {
        let conn = &self.conns[token.slot().raw() as usize];
        conn.token.set(Some(token));
        conn.ready.set(Some(ready));
        conn.close.set(false);
        conn.unsynced.set(0);
        conn.batch_open.set(false);
    }

    pub(super) fn deactivate(&self, token: Token) {
        let Some(conn) = self.conn(token) else {
            return;
        };
        conn.token.set(None);
        conn.ready.set(None);
        conn.close.set(false);
        conn.unsynced.set(0);
        conn.batch_open.set(false);
        conn.requests.clear();
    }

    pub(super) fn frame(&self) -> Result<Frame<'_>, Error> {
        self.requests
            .as_ref()
            .try_acquire()
            .map(|buffer| Frame {
                buffer,
                overflowed: false,
            })
            .ok_or(Error::RequestCapacity)
    }

    pub(super) fn encode(&self, f: impl FnOnce(&mut Frame<'_>)) -> Result<Frame<'_>, Error> {
        let mut frame = self.frame()?;
        f(&mut frame);
        if frame.overflowed() {
            return Err(Error::RequestTooLarge);
        }
        Ok(frame)
    }

    fn enqueue_request<'a>(&self, conn: &Conn<'d>, frame: Frame<'a>) -> Result<(), Frame<'a>> {
        let len = frame.as_ref().len();
        conn.requests
            .try_push(frame.cast(), len)
            .map_err(Frame::cast)
    }

    fn try_enqueue_conn<'a>(
        &self,
        token: Token,
        bytes: Frame<'a>,
    ) -> Result<(), (Error, Frame<'a>)> {
        let Some(conn) = self.conn(token) else {
            return Err((Error::Closed, bytes));
        };
        let queued = conn.requests.weight();
        if !conn.requests.has_capacity() {
            return Err((self.shared.backpressure(queued), bytes));
        }
        self.enqueue_request(conn, bytes)
            .map_err(|bytes| (self.shared.backpressure(queued), bytes))?;
        conn.wake();
        Ok(())
    }

    pub(super) fn try_enqueue_reply(
        &'d self,
        token: Token,
        bytes: Frame<'d>,
        reply: &mut impl Registrable<'d, RowItem>,
        boundary: Boundary,
    ) -> Result<(), (Error, Frame<'d>)> {
        let Some(conn) = self.conn(token) else {
            return Err((Error::Closed, bytes));
        };
        let queued = conn.requests.weight();
        if !conn.requests.has_capacity() {
            return Err((self.shared.backpressure(queued), bytes));
        }
        if !reply.try_attach_with_boundary(&conn.responses, matches!(boundary, Boundary::Close)) {
            return Err((self.shared.backpressure(queued), bytes));
        }
        self.enqueue_request(conn, bytes)
            .map_err(|bytes| (self.shared.backpressure(queued), bytes))?;
        conn.unsynced.set(conn.unsynced.get().saturating_add(1));
        match boundary {
            Boundary::Close => {
                conn.unsynced.set(0);
                conn.batch_open.set(false);
            }
            Boundary::Open => conn.batch_open.set(true),
            Boundary::External => {}
        }
        self.shared.inflight_total.inc();
        self.shared.inc_inflight(token);
        conn.wake();
        Ok(())
    }

    pub(super) fn try_enqueue<'a>(
        &self,
        token: Token,
        bytes: Frame<'a>,
    ) -> Result<(), (Error, Frame<'a>)> {
        self.try_enqueue_conn(token, bytes)
    }

    pub(super) fn drain_requests(
        &'d self,
        token: Token,
        mut push: impl FnMut(Frame<'d>) -> Result<(), Frame<'d>>,
    ) -> dope::manifold::connector::Requests {
        let Some(conn) = self.conn(token) else {
            return dope::manifold::connector::Requests::default();
        };
        conn.requests
            .drain(|send| push(send.cast()).map_err(Frame::cast));
        dope::manifold::connector::Requests {
            shutdown: None,
            close: conn
                .close
                .take()
                .then_some(dope::manifold::connector::CloseKind::Reconnect),
        }
    }

    pub(super) fn close(&self, token: Token) {
        let Some(conn) = self.conn(token) else {
            return;
        };
        conn.close.set(true);
        conn.wake();
    }

    pub(super) fn unsynced(&self, token: Token) -> u32 {
        self.conn(token).map_or(0, |conn| conn.unsynced.get())
    }

    pub(super) fn batch_open(&self, token: Token) -> bool {
        self.conn(token).is_some_and(|conn| conn.batch_open.get())
    }

    pub(super) fn set_batch_open(&self, token: Token, open: bool) {
        if let Some(conn) = self.conn(token) {
            conn.batch_open.set(open);
        }
    }

    pub(super) fn can_push_boundary(&self, token: Token) -> bool {
        self.conn(token)
            .is_some_and(|conn| conn.responses.can_mark_boundary())
    }

    pub(super) fn push_boundary(&self, token: Token) -> bool {
        let Some(conn) = self.conn(token) else {
            return false;
        };
        if !conn.responses.mark_boundary() {
            return false;
        }
        conn.unsynced.set(0);
        conn.batch_open.set(false);
        true
    }

    pub(super) fn responses(&self, token: Token) -> Option<&Arena<'d, RowItem>> {
        Some(&self.conn(token)?.responses)
    }

    pub(super) fn response_len(&self, token: Token) -> usize {
        self.responses(token).map_or(0, Arena::len)
    }

    pub(super) fn responses_empty(&self, token: Token) -> bool {
        self.responses(token).is_none_or(Arena::is_empty)
    }
}

impl<I: QuerySet + 'static> StorageFactory for PortFactory<I> {
    type Output<'d> = Port<'d, I>;

    fn build<'d>(self, driver: &mut DriverContext<'_, 'd>) -> Self::Output<'d> {
        Port::new(self.database, self.config, driver.driver_ref())
    }
}
