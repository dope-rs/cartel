use cartel_core::{Arena, ArenaConfig, ArenaLane, Limits, QueueArena, QueueLane, Registrable};
use dope::driver::ready::ReadyKey;
use dope::driver::token::Token;
use dope::manifold::connector::session::Connector;
use dope::manifold::connector::source::Dialer;
use dope::manifold::env::Env;
use dope::runtime::executor::StorageFactory;
use dope::{DriverContext, DriverRef};
use dope_net::Transport;
use dope_net::link::egress::{self, LeaseBuffer};
use dope_net::wire::Wire;
use o3::buffer::{Pool, PoolLayout};
use o3::cell::RegionToken;
use std::cell::Cell;
use std::marker::PhantomData;

use crate::Error;
use crate::protocol;
use crate::query::QuerySet;
use crate::wire::Sink;

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

pub type Frame<'d> = LeaseBuffer<'d>;

impl Sink for Frame<'_> {
    fn push(&mut self, byte: u8) {
        let _ = self.try_push(byte);
    }

    fn extend_from_slice(&mut self, src: &[u8]) {
        let _ = self.try_extend_from_slice(src);
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        self.as_mut_slice()
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
    close: Cell<bool>,
    unsynced: Cell<u32>,
    batch_open: Cell<bool>,
}

impl<'d> Conn<'d> {
    fn new(driver: DriverRef<'d>) -> Self {
        Self {
            driver,
            token: Cell::new(None),
            ready: Cell::new(None),
            close: Cell::new(false),
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
    pub(super) shared: protocol::Shared,
    conns: Box<[Conn<'d>]>,
    request_queue: QueueArena<'d, Frame<'d>>,
    requests: Pool,
    responses: Arena<'d, protocol::RowItem>,
    egress: egress::storage::Storage,
    _instance: PhantomData<fn() -> I>,
}

impl<'d, I: QuerySet> AsRef<Self> for Port<'d, I> {
    fn as_ref(&self) -> &Self {
        self
    }
}

pub struct PortFactory<I> {
    database: crate::Config,
    config: Config,
    instance: PhantomData<fn() -> I>,
}

impl<I> PortFactory<I> {
    pub fn config(&self) -> Config {
        self.config
    }
}

impl<'d, I: QuerySet> Port<'d, I> {
    pub fn factory(database: crate::Config, config: Config) -> PortFactory<I> {
        PortFactory {
            database,
            config,
            instance: PhantomData,
        }
    }

    pub fn new(database: crate::Config, config: Config, driver: DriverRef<'d>) -> Self {
        let connections = config.connection_capacity();
        let request_entries = config.request_capacity();
        let limits = Limits::new(
            config.response_capacity(),
            config.response_byte_capacity(),
            config.response_capacity(),
        );
        let request_queue = QueueArena::with_capacity(request_entries, connections);
        let responses = Arena::new(ArenaConfig::new(
            connections,
            config.inflight_capacity(),
            config.response_capacity(),
            config.response_byte_capacity(),
            config.response_capacity(),
            limits,
        ));
        Self {
            shared: protocol::Shared::new(database, config),
            conns: (0..connections).map(|_| Conn::new(driver)).collect(),
            request_queue,
            requests: Pool::from_layout(config.request_pool()),
            responses,
            egress: egress::storage::Storage::default(),
            _instance: PhantomData,
        }
    }

    pub fn capacity(&self) -> usize {
        self.shared.config.connection_capacity()
    }

    pub fn config(&self) -> Config {
        self.shared.config
    }

    pub(super) fn backpressure(&self, queued: usize, region: &mut RegionToken<'d>) -> Error {
        Error::Backpressure {
            inflight: self.responses.inflight(region),
            queued,
            cap: self.shared.config.inflight_capacity(),
        }
    }

    pub fn connect<const ID: u8, S, E>(
        &'d self,
        upstreams: S,
        driver: &mut DriverContext<'_, 'd>,
    ) -> std::io::Result<Connector<'d, ID, protocol::Session<'d, I>, S, E>>
    where
        S: Dialer<E::Transport> + 'd,
        E: Env + 'd,
        E::Transport: Transport,
        <E::Wire as Wire>::InitConfig<'d>: Default,
    {
        Connector::new(
            protocol::Session::new(self),
            upstreams,
            self.config().connection_capacity(),
            &self.egress,
            driver,
        )
    }

    /// Creates a connector whose wire runtime is local to this core.
    ///
    /// The configuration is consumed once when the connector's wire runtime is
    /// initialized, rather than cloned into individual connections.
    pub fn connect_configured<const ID: u8, S, E>(
        &'d self,
        upstreams: S,
        wire: <E::Wire as Wire>::InitConfig<'d>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> std::io::Result<Connector<'d, ID, protocol::Session<'d, I>, S, E>>
    where
        S: Dialer<E::Transport> + 'd,
        E: Env + 'd,
        E::Transport: Transport,
    {
        Connector::new_with_wire_config(
            protocol::Session::new(self),
            upstreams,
            self.config().connection_capacity(),
            wire,
            &self.egress,
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

    pub(super) fn deactivate(&'d self, token: Token, region: &mut RegionToken<'d>) {
        let Some(conn) = self.conn(token) else {
            return;
        };
        let lane = token.slot().raw() as usize;
        conn.token.set(None);
        conn.ready.set(None);
        conn.close.set(false);
        conn.unsynced.set(0);
        conn.batch_open.set(false);
        self.request_queue.lane(lane).clear(region);
    }

    pub(super) fn frame(&'d self) -> Result<Frame<'d>, Error> {
        self.requests
            .try_acquire()
            .map(Frame::new)
            .ok_or(Error::RequestCapacity)
    }

    pub(super) fn encode(&'d self, f: impl FnOnce(&mut Frame<'_>)) -> Result<Frame<'d>, Error> {
        let mut frame = self.frame()?;
        f(&mut frame);
        if frame.overflowed() {
            return Err(Error::RequestTooLarge);
        }
        Ok(frame)
    }

    fn request_lane(&self, token: Token) -> QueueLane<'_, 'd, Frame<'d>> {
        self.request_queue.lane(token.slot().raw() as usize)
    }

    fn enqueue_request(
        &'d self,
        token: Token,
        frame: Frame<'d>,
        region: &mut RegionToken<'d>,
    ) -> Result<(), Frame<'d>> {
        let len = frame.as_ref().len();
        self.request_lane(token).try_push(region, frame, len)
    }

    fn try_enqueue_conn(
        &'d self,
        token: Token,
        bytes: Frame<'d>,
        region: &mut RegionToken<'d>,
    ) -> Result<(), (Error, Frame<'d>)> {
        let Some(conn) = self.conn(token) else {
            return Err((Error::Closed, bytes));
        };
        let requests = self.request_lane(token);
        let queued = requests.weight(region);
        if !requests.has_capacity(region) {
            return Err((self.backpressure(queued, region), bytes));
        }
        self.enqueue_request(token, bytes, region)
            .map_err(|bytes| (self.backpressure(queued, region), bytes))?;
        conn.wake();
        Ok(())
    }

    pub(super) fn try_enqueue_reply(
        &'d self,
        token: Token,
        bytes: Frame<'d>,
        reply: &mut impl Registrable<'d, protocol::RowItem>,
        boundary: Boundary,
        region: &mut RegionToken<'d>,
    ) -> Result<(), (Error, Frame<'d>)> {
        let Some(conn) = self.conn(token) else {
            return Err((Error::Closed, bytes));
        };
        let requests = self.request_lane(token);
        let queued = requests.weight(region);
        if !requests.has_capacity(region) {
            return Err((self.backpressure(queued, region), bytes));
        }
        let lane = token.slot().raw() as usize;
        if !reply.try_attach_with_boundary(
            region,
            self.responses.lane(lane),
            matches!(boundary, Boundary::Close),
        ) {
            return Err((self.backpressure(queued, region), bytes));
        }
        self.enqueue_request(token, bytes, region)
            .map_err(|bytes| (self.backpressure(queued, region), bytes))?;
        conn.unsynced.set(conn.unsynced.get().saturating_add(1));
        match boundary {
            Boundary::Close => {
                conn.unsynced.set(0);
                conn.batch_open.set(false);
            }
            Boundary::Open => conn.batch_open.set(true),
            Boundary::External => {}
        }
        self.shared.inc_inflight(token);
        conn.wake();
        Ok(())
    }

    pub(super) fn try_enqueue(
        &'d self,
        token: Token,
        bytes: Frame<'d>,
        region: &mut RegionToken<'d>,
    ) -> Result<(), (Error, Frame<'d>)> {
        self.try_enqueue_conn(token, bytes, region)
    }

    pub(super) fn drain_requests(
        &'d self,
        token: Token,
        push: impl FnMut(&mut RegionToken<'d>, Frame<'d>) -> Result<(), Frame<'d>>,
        region: &mut RegionToken<'d>,
    ) -> dope::manifold::connector::app::Requests {
        let Some(conn) = self.conn(token) else {
            return dope::manifold::connector::app::Requests::default();
        };
        self.request_lane(token).drain(region, push);
        dope::manifold::connector::app::Requests {
            close: conn
                .close
                .take()
                .then_some(dope::manifold::connector::app::CloseKind::Reconnect),
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

    pub(super) fn can_push_boundary(&'d self, token: Token, region: &mut RegionToken<'d>) -> bool {
        self.conn(token).is_some()
            && self
                .responses
                .lane(token.slot().raw() as usize)
                .can_mark_boundary(region)
    }

    pub(super) fn push_boundary(&'d self, token: Token, region: &mut RegionToken<'d>) -> bool {
        let Some(conn) = self.conn(token) else {
            return false;
        };
        if !self
            .responses
            .lane(token.slot().raw() as usize)
            .mark_boundary(region)
        {
            return false;
        }
        conn.unsynced.set(0);
        conn.batch_open.set(false);
        true
    }

    pub(super) fn responses(&'d self, token: Token) -> Option<ArenaLane<'d, protocol::RowItem>> {
        self.conn(token)?;
        Some(self.responses.lane(token.slot().raw() as usize))
    }

    pub(super) fn response_len(&'d self, token: Token, region: &mut RegionToken<'d>) -> usize {
        self.responses(token)
            .map_or(0, |responses| responses.len(region))
    }

    pub(super) fn responses_empty(&'d self, token: Token, region: &mut RegionToken<'d>) -> bool {
        self.responses(token)
            .is_none_or(|responses| responses.is_empty(region))
    }
}

impl<I: QuerySet + 'static> StorageFactory for PortFactory<I> {
    type Output<'d> = Port<'d, I>;

    fn build<'d>(self, driver: &mut DriverContext<'_, 'd>) -> Self::Output<'d> {
        Port::new(self.database, self.config, driver.driver_ref())
    }
}
