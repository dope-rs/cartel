use std::cell::{Cell, RefCell};
use std::marker::PhantomData;
use std::pin::Pin;

use cartel_core::{FrontKind, Inflight};
use dope::driver::token::Token;
use dope::manifold::connector;
use dope::manifold::connector::state::{IOV_CAP, Queue};
use dope::manifold::connector::{Close, Ctx};
use dope_fiber::WaitQueue;
use dope_fiber::{Context, Waiter};
use o3::buffer;
use o3::cell::RegionToken;
use o3::collections::FixedQueue;

use crate::decode::{AuthRequest, parse_auth, parse_db_error, parse_notification};
use crate::port::{self, Frame as SendFrame, Port};
use crate::query::QuerySet;
use crate::scram::Scram;
use crate::wire::Be;
use crate::{Config, Error, Notification, encode};

pub(super) type RowItem = Result<buffer::Shared, Error>;

const OVERSIZE_FRAME: u8 = 0xff;
const NONE: u32 = u32::MAX;
const BUCKET_NONE: u32 = u32::MAX;

#[derive(Clone, Copy, PartialEq, Eq)]
struct TransactionId {
    conn: Token,
    generation: u64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TransactionState {
    Held(TransactionId),
    Finalizing(TransactionId),
    Quarantined(TransactionId),
}

impl TransactionState {
    fn id(self) -> TransactionId {
        match self {
            Self::Held(id) | Self::Finalizing(id) | Self::Quarantined(id) => id,
        }
    }
}

pub struct Frame {
    pub typ: u8,
    pub payload: buffer::Shared,
}

#[derive(Default)]
pub struct ConnState {
    phase: Phase,
    error_skip: bool,
    pending_close: bool,
    close_permanent: bool,
}

impl connector::Lifecycle for ConnState {
    fn wants_close(&self) -> Close {
        if !self.pending_close {
            Close::Keep
        } else if self.close_permanent {
            Close::Permanent
        } else {
            Close::Reconnect
        }
    }

    fn defer_close(&self) -> bool {
        false
    }

    fn is_drained(&self) -> bool {
        true
    }
}

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub enum PickPolicy {
    #[default]
    RoundRobin,
    LeastInflight,
}

pub(super) struct Shared {
    database: Config,
    pub(super) config: port::Config,
    ready_tokens: Box<[Cell<Option<Token>>]>,
    inflight: Box<[Cell<u32>]>,
    transactions: Box<[Cell<Option<TransactionState>>]>,
    transaction_generations: Box<[Cell<u64>]>,
    transaction_waiters: Pin<Box<[WaitQueue]>>,
    ready_next: Box<[Cell<u32>]>,
    ready_prev: Box<[Cell<u32>]>,
    ready_linked: Box<[Cell<bool>]>,
    ready_head: Cell<u32>,
    bucket_next: Box<[Cell<u32>]>,
    bucket_prev: Box<[Cell<u32>]>,
    bucket_of: Box<[Cell<u32>]>,
    bucket_heads: Box<[Cell<u32>]>,
    bucket_bits: Box<[Cell<u64>]>,
    pub(super) policy: Cell<PickPolicy>,
    pub(super) ready_count: Cell<usize>,
    ready: Cell<bool>,
    ready_waiters: Pin<Box<WaitQueue>>,
    fatal: Cell<Option<Error>>,
    notifications: RefCell<FixedQueue<Notification>>,
    notifications_dropped: Cell<u64>,
    notification_waiters: Pin<Box<WaitQueue>>,
    pub(super) inflight_total: Inflight,
    egress_waiters: Pin<Box<WaitQueue>>,
    backend_pids: Box<[Cell<i32>]>,
    backend_keys: Box<[Cell<i32>]>,
}

impl Shared {
    pub(super) fn new(database: Config, config: port::Config) -> Self {
        let max_connections = config.connection_capacity();
        let waiter_capacity = config.waiter_capacity();
        let max_pending_per_conn = config.request_capacity().div_ceil(max_connections);
        let inflight_total = Inflight::with_capacity(config.inflight_capacity());
        Self {
            database,
            config,
            ready_tokens: (0..max_connections).map(|_| Cell::new(None)).collect(),
            inflight: (0..max_connections).map(|_| Cell::new(0u32)).collect(),
            transactions: (0..max_connections).map(|_| Cell::new(None)).collect(),
            transaction_generations: (0..max_connections).map(|_| Cell::new(0u64)).collect(),
            transaction_waiters: Box::into_pin(
                (0..max_connections)
                    .map(|_| WaitQueue::with_capacity(1))
                    .collect(),
            ),
            ready_next: (0..max_connections).map(|_| Cell::new(NONE)).collect(),
            ready_prev: (0..max_connections).map(|_| Cell::new(NONE)).collect(),
            ready_linked: (0..max_connections).map(|_| Cell::new(false)).collect(),
            ready_head: Cell::new(NONE),
            bucket_next: (0..max_connections).map(|_| Cell::new(NONE)).collect(),
            bucket_prev: (0..max_connections).map(|_| Cell::new(NONE)).collect(),
            bucket_of: (0..max_connections)
                .map(|_| Cell::new(BUCKET_NONE))
                .collect(),
            bucket_heads: (0..=max_pending_per_conn)
                .map(|_| Cell::new(NONE))
                .collect(),
            bucket_bits: (0..=(max_pending_per_conn / 64))
                .map(|_| Cell::new(0))
                .collect(),
            policy: Cell::new(PickPolicy::default()),
            ready_count: Cell::new(0),
            ready: Cell::new(false),
            ready_waiters: Box::pin(WaitQueue::with_capacity(waiter_capacity)),
            fatal: Cell::new(None),
            notifications: RefCell::new(FixedQueue::with_capacity(config.notification_capacity())),
            notifications_dropped: Cell::new(0),
            notification_waiters: Box::pin(WaitQueue::with_capacity(waiter_capacity)),
            inflight_total,
            egress_waiters: Box::pin(WaitQueue::with_capacity(waiter_capacity)),
            backend_pids: (0..max_connections).map(|_| Cell::new(0)).collect(),
            backend_keys: (0..max_connections).map(|_| Cell::new(0)).collect(),
        }
    }

    pub(super) fn backend_pid_for(&self, slot: Token) -> Option<i32> {
        self.backend_pids
            .get(slot.slot().raw() as usize)
            .map(Cell::get)
            .filter(|&p| p != 0)
    }

    pub(super) fn backend_key_for(&self, slot: Token) -> Option<i32> {
        let i = slot.slot().raw() as usize;
        if self.backend_pids.get(i)?.get() == 0 {
            return None;
        }
        self.backend_keys.get(i).map(Cell::get)
    }

    pub(super) fn store_backend_key_data(&self, slot: Token, payload: &[u8]) {
        if payload.len() < 8 {
            return;
        }
        let Ok(pid) = payload[0..4].try_into() else {
            return;
        };
        let Ok(key) = payload[4..8].try_into() else {
            return;
        };
        let pid = i32::from_be_bytes(pid);
        let key = i32::from_be_bytes(key);
        let index = slot.slot().raw() as usize;
        if let (Some(stored_pid), Some(stored_key)) =
            (self.backend_pids.get(index), self.backend_keys.get(index))
        {
            stored_pid.set(pid);
            stored_key.set(key);
        }
    }

    pub(super) fn pop_notification(&self) -> Option<Notification> {
        self.notifications.borrow_mut().pop_front()
    }

    pub(super) fn push_notification(&self, n: Notification) {
        let mut q = self.notifications.borrow_mut();
        if q.len() >= self.config.notification_capacity() {
            q.pop_front();
            self.notifications_dropped
                .set(self.notifications_dropped.get().saturating_add(1));
        }
        let Some(entry) = q.vacant_entry() else {
            unreachable!()
        };
        entry.push_back(n);
        self.notification_waiters.as_ref().wake();
    }

    pub(super) fn notifications_dropped(&self) -> u64 {
        self.notifications_dropped.get()
    }

    pub(super) fn try_register_notification<'d>(
        &self,
        waiter: Pin<&Waiter<'d>>,
        context: Pin<&Context<'_, 'd>>,
    ) -> bool {
        self.notification_waiters
            .as_ref()
            .try_register(waiter, context)
    }

    pub(super) fn try_register_egress<'d>(
        &self,
        waiter: Pin<&Waiter<'d>>,
        context: Pin<&Context<'_, 'd>>,
    ) -> bool {
        self.egress_waiters.as_ref().try_register(waiter, context)
    }

    pub(super) fn try_register_ready<'d>(
        &self,
        waiter: Pin<&Waiter<'d>>,
        context: Pin<&Context<'_, 'd>>,
    ) -> bool {
        self.ready_waiters.as_ref().try_register(waiter, context)
    }

    pub(super) fn backpressure(&self, queued: usize) -> Error {
        Error::Backpressure {
            inflight: self.inflight_total.len(),
            queued,
            cap: self.inflight_total.capacity(),
        }
    }

    pub(super) fn is_ready(&self) -> bool {
        self.ready.get()
    }

    pub(super) fn is_failed(&self) -> bool {
        let error = self.fatal.take();
        let failed = error.is_some();
        self.fatal.set(error);
        failed
    }

    pub(super) fn fatal_message(&self) -> Option<String> {
        let error = self.fatal.take();
        let message = error.as_ref().map(ToString::to_string);
        self.fatal.set(error);
        message
    }

    fn clear_fatal(&self) {
        self.fatal.set(None);
    }

    fn record_fatal(&self, error: Error) {
        let current = self.fatal.take();
        self.fatal.set(current.or(Some(error)));
    }

    pub(super) fn is_exclusive(&self, slot: Token) -> bool {
        self.transactions
            .get(slot.slot().raw() as usize)
            .and_then(Cell::get)
            .is_some_and(|state| state.id().conn == slot)
    }

    pub(super) fn try_acquire_transaction(&self, slot: Token) -> Option<(Token, u64)> {
        let index = slot.slot().raw() as usize;
        if self.ready_tokens.get(index).map(Cell::get) != Some(Some(slot))
            || self.transactions.get(index)?.get().is_some()
        {
            return None;
        }
        let generation = self.transaction_generations[index].get().checked_add(1)?;
        self.transaction_generations[index].set(generation);
        let id = TransactionId {
            conn: slot,
            generation,
        };
        self.unlink_ready(index);
        self.unlink_bucket(index);
        self.transactions[index].set(Some(TransactionState::Held(id)));
        Some((slot, generation))
    }

    pub(super) fn is_transaction_held(&self, target: (Token, u64)) -> bool {
        let id = TransactionId {
            conn: target.0,
            generation: target.1,
        };
        self.transactions
            .get(id.conn.slot().raw() as usize)
            .is_some_and(|state| state.get() == Some(TransactionState::Held(id)))
    }

    fn is_transaction(&self, target: (Token, u64)) -> bool {
        let id = TransactionId {
            conn: target.0,
            generation: target.1,
        };
        self.transactions
            .get(id.conn.slot().raw() as usize)
            .and_then(Cell::get)
            .is_some_and(|state| state.id() == id)
    }

    pub(super) fn transaction_settled(&self, target: (Token, u64)) -> Option<bool> {
        if self.is_transaction(target) {
            return None;
        }
        Some(
            self.ready_tokens
                .get(target.0.slot().raw() as usize)
                .is_some_and(|token| token.get() == Some(target.0)),
        )
    }

    pub(super) fn begin_transaction_finalization(&self, target: (Token, u64)) -> bool {
        let id = TransactionId {
            conn: target.0,
            generation: target.1,
        };
        let Some(state) = self.transactions.get(id.conn.slot().raw() as usize) else {
            return false;
        };
        if state.get() != Some(TransactionState::Held(id)) {
            return false;
        }
        state.set(Some(TransactionState::Finalizing(id)));
        true
    }

    pub(super) fn quarantine_transaction(&self, target: (Token, u64)) -> bool {
        let id = TransactionId {
            conn: target.0,
            generation: target.1,
        };
        let Some(state) = self.transactions.get(id.conn.slot().raw() as usize) else {
            return false;
        };
        if state.get() != Some(TransactionState::Held(id)) {
            return false;
        }
        state.set(Some(TransactionState::Quarantined(id)));
        true
    }

    pub(super) fn try_register_transaction<'d>(
        &self,
        target: (Token, u64),
        waiter: Pin<&Waiter<'d>>,
        context: Pin<&Context<'_, 'd>>,
    ) -> bool {
        if !self.is_transaction(target) {
            return false;
        }
        self.transaction_waiter(target.0.slot().raw() as usize)
            .is_some_and(|queue| queue.try_register(waiter, context))
    }

    fn transaction_waiter(&self, index: usize) -> Option<Pin<&WaitQueue>> {
        WaitQueue::get_pinned(self.transaction_waiters.as_ref(), index)
    }

    fn clear_transaction(&self, slot: Token, available: bool) {
        let index = slot.slot().raw() as usize;
        let Some(state) = self.transactions.get(index) else {
            return;
        };
        if !state
            .get()
            .is_some_and(|transaction| transaction.id().conn == slot)
        {
            return;
        }
        state.set(None);
        if available && self.ready_tokens[index].get() == Some(slot) {
            self.link_ready(index);
            self.link_bucket(index, self.inflight[index].get() as usize);
        }
        if let Some(waiters) = self.transaction_waiter(index) {
            waiters.wake();
        }
        self.ready_waiters.as_ref().wake();
        self.egress_waiters.as_ref().wake();
    }

    fn transaction_ready(&self, slot: Token, idle: bool) -> bool {
        let index = slot.slot().raw() as usize;
        match self.transactions[index].get() {
            Some(TransactionState::Finalizing(id)) if id.conn == slot => {
                if idle {
                    self.clear_transaction(slot, true);
                    true
                } else {
                    false
                }
            }
            _ => {
                if let Some(waiters) = self.transaction_waiter(index) {
                    waiters.wake();
                }
                true
            }
        }
    }

    pub(super) fn tx_saturated(&self) -> bool {
        self.transactions
            .iter()
            .any(|state| matches!(state.get(), Some(TransactionState::Held(_))))
    }

    pub(super) fn pick_conn(&self, target: Option<(Token, u64)>) -> Option<Token> {
        if let Some((conn, generation)) = target {
            let index = conn.slot().raw() as usize;
            if self.ready_tokens.get(index)?.get() != Some(conn) {
                return None;
            }
            let transaction = self.transactions[index].get();
            return if generation == 0 {
                transaction.is_none().then_some(conn)
            } else {
                (transaction == Some(TransactionState::Held(TransactionId { conn, generation })))
                    .then_some(conn)
            };
        }
        let index = match self.policy.get() {
            PickPolicy::RoundRobin => {
                let index = self.ready_head.get();
                if index == NONE {
                    return None;
                }
                self.ready_head.set(self.ready_next[index as usize].get());
                index as usize
            }
            PickPolicy::LeastInflight => self.first_bucket_index()?,
        };
        self.ready_tokens[index].get()
    }

    pub(super) fn inc_inflight(&self, slot: Token) {
        let index = slot.slot().raw() as usize;
        if let Some(inflight) = self.inflight.get(index) {
            self.unlink_bucket(index);
            inflight.set(inflight.get().saturating_add(1));
            if self.ready_tokens[index].get() == Some(slot)
                && self.transactions[index].get().is_none()
            {
                self.link_bucket(index, inflight.get() as usize);
            }
        }
    }

    pub(super) fn dec_inflight(&self, slot: Token) {
        let index = slot.slot().raw() as usize;
        if let Some(inflight) = self.inflight.get(index) {
            self.unlink_bucket(index);
            inflight.set(inflight.get().saturating_sub(1));
            if self.ready_tokens[index].get() == Some(slot)
                && self.transactions[index].get().is_none()
            {
                self.link_bucket(index, inflight.get() as usize);
            }
        }
    }

    fn add_ready(&self, conn: Token) {
        let index = conn.slot().raw() as usize;
        self.ready_tokens[index].set(Some(conn));
        if self.transactions[index].get().is_none() {
            self.link_ready(index);
            self.link_bucket(index, self.inflight[index].get() as usize);
        }
        self.ready_count.set(self.ready_count.get() + 1);
        self.ready.set(true);
        self.ready_waiters.as_ref().wake();
    }

    fn remove_ready(&self, conn: Token) {
        let index = conn.slot().raw() as usize;
        let removed = self.ready_tokens[index].get() == Some(conn);
        if removed {
            self.unlink_ready(index);
            self.unlink_bucket(index);
            self.ready_tokens[index].set(None);
            self.ready_count
                .set(self.ready_count.get().saturating_sub(1));
        }
        if self.ready_count.get() == 0 {
            self.ready.set(false);
        }
    }

    fn link_ready(&self, index: usize) {
        if self.ready_linked[index].replace(true) {
            return;
        }
        let head = self.ready_head.get();
        if head == NONE {
            self.ready_next[index].set(index as u32);
            self.ready_prev[index].set(index as u32);
            self.ready_head.set(index as u32);
            return;
        }
        let tail = self.ready_prev[head as usize].get();
        self.ready_next[index].set(head);
        self.ready_prev[index].set(tail);
        self.ready_next[tail as usize].set(index as u32);
        self.ready_prev[head as usize].set(index as u32);
    }

    fn unlink_ready(&self, index: usize) {
        if !self.ready_linked[index].replace(false) {
            return;
        }
        let next = self.ready_next[index].get();
        let prev = self.ready_prev[index].get();
        if next == index as u32 {
            self.ready_head.set(NONE);
        } else {
            self.ready_next[prev as usize].set(next);
            self.ready_prev[next as usize].set(prev);
            if self.ready_head.get() == index as u32 {
                self.ready_head.set(next);
            }
        }
        self.ready_next[index].set(NONE);
        self.ready_prev[index].set(NONE);
    }

    fn link_bucket(&self, index: usize, depth: usize) {
        let depth = depth.min(self.bucket_heads.len() - 1);
        if self.bucket_of[index].get() != BUCKET_NONE {
            return;
        }
        let head = self.bucket_heads[depth].get();
        self.bucket_prev[index].set(NONE);
        self.bucket_next[index].set(head);
        if head != NONE {
            self.bucket_prev[head as usize].set(index as u32);
        }
        self.bucket_heads[depth].set(index as u32);
        self.bucket_of[index].set(depth as u32);
        let word = depth / 64;
        self.bucket_bits[word].set(self.bucket_bits[word].get() | 1 << (depth % 64));
    }

    fn unlink_bucket(&self, index: usize) {
        let depth = self.bucket_of[index].replace(BUCKET_NONE);
        if depth == BUCKET_NONE {
            return;
        }
        let depth = depth as usize;
        let next = self.bucket_next[index].replace(NONE);
        let prev = self.bucket_prev[index].replace(NONE);
        if prev == NONE {
            self.bucket_heads[depth].set(next);
        } else {
            self.bucket_next[prev as usize].set(next);
        }
        if next != NONE {
            self.bucket_prev[next as usize].set(prev);
        }
        if self.bucket_heads[depth].get() == NONE {
            let word = depth / 64;
            self.bucket_bits[word].set(self.bucket_bits[word].get() & !(1 << (depth % 64)));
        }
    }

    fn first_bucket_index(&self) -> Option<usize> {
        for (word_index, word) in self.bucket_bits.iter().enumerate() {
            let bits = word.get();
            if bits != 0 {
                let depth = word_index * 64 + bits.trailing_zeros() as usize;
                return Some(self.bucket_heads[depth].get() as usize);
            }
        }
        None
    }

    fn clear_backend(&self, conn: Token) {
        let index = conn.slot().raw() as usize;
        if let Some(pid) = self.backend_pids.get(index) {
            pid.set(0);
        }
        if let Some(key) = self.backend_keys.get(index) {
            key.set(0);
        }
    }

    fn wake_all(&self) {
        self.ready_waiters.as_ref().wake();
        self.notification_waiters.as_ref().wake();
        self.egress_waiters.as_ref().wake();
    }
}

#[derive(Default)]
enum Phase {
    #[default]
    NeedsStartup,
    StartupSent,
    AwaitingSaslContinue {
        scram: Box<Scram>,
    },
    AwaitingSaslFinal {
        scram: Box<Scram>,
    },
    AwaitingReady,
    Preparing {
        remaining: u32,
    },
    Ready,
    CopyInActive,
    CopyOutActive,
    Failed,
}

pub struct Codec {
    max_response_bytes: usize,
}

impl connector::Codec for Codec {
    type Head = Frame;
    type ParseState = ();

    fn parse(&self, _state: &mut (), buf: &buffer::Shared) -> Option<(Frame, usize)> {
        if buf.len() < 5 {
            return None;
        }
        let typ = buf[0];
        let len_bytes = buf[1..5].try_into().ok()?;
        let len = u32::from_be_bytes(len_bytes) as usize;
        if len < 4 {
            return Some((
                Frame {
                    typ,
                    payload: buffer::Shared::new(),
                },
                buf.len(),
            ));
        }
        let response_len = len - 4;
        if response_len > self.max_response_bytes {
            return Some((
                Frame {
                    typ: OVERSIZE_FRAME,
                    payload: buffer::Shared::new(),
                },
                5,
            ));
        }
        let total = 1 + len;
        if buf.len() < total {
            return None;
        }
        let payload = buf.slice(5..total);
        Some((Frame { typ, payload }, total))
    }
}

pub struct Session<'d, I: QuerySet> {
    codec: Codec,
    port: &'d Port<'d, I>,
    _instance: PhantomData<fn() -> I>,
}

impl<'d, I: QuerySet> Session<'d, I> {
    pub(super) fn new(port: &'d Port<'d, I>) -> Self {
        Self {
            codec: Codec {
                max_response_bytes: port.shared.config.response_byte_capacity(),
            },
            port,
            _instance: PhantomData,
        }
    }

    fn enqueue(out: &Queue<IOV_CAP, SendFrame<'d>>, frame: SendFrame<'d>) -> Result<(), Error> {
        out.try_enqueue(frame).map_err(|_| Error::RequestCapacity)
    }

    fn fail(
        &mut self,
        conn_id: Token,
        conn_state: &mut ConnState,
        err: Error,
        region: &mut RegionToken<'d>,
    ) {
        let msg = err.to_string();
        let was_ready = matches!(conn_state.phase, Phase::Ready);
        let permanent = !was_ready
            && match &err {
                Error::Db(db) => !db.transient(),
                Error::Auth(_) | Error::Protocol(_) | Error::ProtocolOwned(_) => true,
                _ => false,
            };
        conn_state.phase = Phase::Failed;
        conn_state.pending_close = true;
        conn_state.close_permanent = permanent;
        let n = self.port.responses(conn_id).map_or(0, |responses| {
            responses.fail_all(region, || Err(Error::Other(msg.clone())))
        });
        self.port.set_batch_open(conn_id, false);
        let s = &self.port.shared;
        s.inflight_total.dec_n(n);
        if permanent {
            s.record_fatal(err);
        }
        if was_ready {
            s.remove_ready(conn_id);
        }
        s.clear_transaction(conn_id, false);
        s.ready_waiters.as_ref().wake();
        s.egress_waiters.as_ref().wake();
    }

    fn send_prepare(&self, out: &mut Queue<IOV_CAP, SendFrame<'d>>) -> Result<u32, Error> {
        let mut count = 0u32;
        let mut queries = I::GROUPS.iter().flat_map(|group| group.iter()).peekable();
        while let Some(meta) = queries.next() {
            let frame = self.port.encode(|frame| {
                encode::parse(frame, meta.name, meta.sql, meta.param_oids);
                if queries.peek().is_none() {
                    encode::sync(frame);
                }
            })?;
            Self::enqueue(out, frame)?;
            count += 1;
        }
        if count == 0 {
            Self::enqueue(out, self.port.encode(|frame| encode::sync(frame))?)?;
        }
        Ok(count)
    }

    fn handle_startup(
        &mut self,
        conn_id: Token,
        conn_state: &mut ConnState,
        typ: u8,
        payload: &[u8],
        out: &mut Queue<IOV_CAP, SendFrame<'d>>,
    ) -> Result<(), Error> {
        match typ {
            Be::AUTH => {
                let req = parse_auth(payload)?;
                match req {
                    AuthRequest::Ok => {
                        conn_state.phase = Phase::AwaitingReady;
                        Ok(())
                    }
                    AuthRequest::Sasl { mechanisms } => {
                        let scram = Scram::new(self.port.shared.database.password())?;
                        let mech = scram.pick_mechanism(&mechanisms)?;
                        let client_first = scram.client_first();
                        let frame = self.port.encode(|frame| {
                            encode::sasl_initial_response(frame, mech, client_first.as_bytes());
                        })?;
                        Self::enqueue(out, frame)?;
                        conn_state.phase = Phase::AwaitingSaslContinue {
                            scram: Box::new(scram),
                        };
                        Ok(())
                    }
                    AuthRequest::SaslContinue { .. } | AuthRequest::SaslFinal { .. } => Err(
                        Error::Auth("unexpected SASL continuation in startup phase".into()),
                    ),
                    AuthRequest::Other(n) => Err(Error::Auth(format!(
                        "unsupported auth method {n}; only SCRAM-SHA-256 / trust supported"
                    ))),
                }
            }
            Be::PARAMETER_STATUS | Be::NOTICE_RESPONSE => Ok(()),
            Be::BACKEND_KEY_DATA => {
                self.port.shared.store_backend_key_data(conn_id, payload);
                Ok(())
            }
            Be::READY_FOR_QUERY => Ok(()),
            Be::ERROR_RESPONSE => Err(Error::Db(Box::new(parse_db_error(payload)))),
            other => Err(Error::ProtocolOwned(format!(
                "unexpected message {} during startup",
                other as char
            ))),
        }
    }

    fn handle_sasl(
        &mut self,
        conn_state: &mut ConnState,
        typ: u8,
        payload: &[u8],
        out: &mut Queue<IOV_CAP, SendFrame<'d>>,
    ) -> Result<(), Error> {
        if typ == Be::ERROR_RESPONSE {
            return Err(Error::Db(Box::new(parse_db_error(payload))));
        }
        if typ != Be::AUTH {
            return Err(Error::ProtocolOwned(format!(
                "unexpected message {} during SASL exchange",
                typ as char
            )));
        }
        let req = parse_auth(payload)?;
        let phase = std::mem::replace(&mut conn_state.phase, Phase::Failed);
        match (phase, req) {
            (Phase::AwaitingSaslContinue { mut scram }, AuthRequest::SaslContinue { data }) => {
                let client_final = scram.client_final(data)?;
                let frame = self
                    .port
                    .encode(|frame| encode::sasl_response(frame, client_final.as_bytes()))?;
                Self::enqueue(out, frame)?;
                conn_state.phase = Phase::AwaitingSaslFinal { scram };
                Ok(())
            }
            (Phase::AwaitingSaslFinal { scram }, AuthRequest::SaslFinal { data }) => {
                scram.verify_server_final(data)?;
                conn_state.phase = Phase::AwaitingReady;
                Ok(())
            }
            (_, AuthRequest::Ok) => {
                conn_state.phase = Phase::AwaitingReady;
                Ok(())
            }
            _ => Err(Error::Auth("SASL state machine out of sync".into())),
        }
    }

    fn handle_preparing(
        &mut self,
        conn_id: Token,
        typ: u8,
        payload: &[u8],
        conn_state: &mut ConnState,
        remaining: u32,
    ) -> Result<(), Error> {
        match typ {
            Be::PARSE_COMPLETE => {
                let left = remaining.saturating_sub(1);
                conn_state.phase = Phase::Preparing { remaining: left };
                Ok(())
            }
            Be::PARAMETER_STATUS | Be::NOTICE_RESPONSE | Be::BIND_COMPLETE => Ok(()),
            Be::READY_FOR_QUERY => {
                conn_state.phase = Phase::Ready;
                self.port.shared.clear_transaction(conn_id, false);
                let index = conn_id.slot().raw() as usize;
                if let Some(inflight) = self.port.shared.inflight.get(index) {
                    inflight.set(0);
                }
                self.port.shared.add_ready(conn_id);
                Ok(())
            }
            Be::ERROR_RESPONSE => Err(Error::Db(Box::new(parse_db_error(payload)))),
            other => Err(Error::ProtocolOwned(format!(
                "unexpected message {} during eager-prepare",
                other as char
            ))),
        }
    }

    fn handle_ready(
        &mut self,
        conn_id: Token,
        typ: u8,
        head_payload: buffer::Shared,
        conn_state: &mut ConnState,
        region: &mut RegionToken<'d>,
    ) -> Result<(), Error> {
        if matches!(
            typ,
            Be::COMMAND_COMPLETE
                | Be::EMPTY_QUERY_RESPONSE
                | Be::ERROR_RESPONSE
                | Be::READY_FOR_QUERY
        ) && matches!(conn_state.phase, Phase::CopyInActive | Phase::CopyOutActive)
        {
            conn_state.phase = Phase::Ready;
        }
        match typ {
            Be::PARSE_COMPLETE
            | Be::BIND_COMPLETE
            | Be::ROW_DESCRIPTION
            | Be::NO_DATA
            | Be::PARAMETER_STATUS
            | Be::NOTICE_RESPONSE
            | Be::PORTAL_SUSPENDED
            | Be::PARAMETER_DESCRIPTION
            | Be::COPY_DONE => Ok(()),
            Be::COPY_IN_RESPONSE => {
                conn_state.phase = Phase::CopyInActive;
                Ok(())
            }
            Be::COPY_OUT_RESPONSE => {
                conn_state.phase = Phase::CopyOutActive;
                Ok(())
            }
            Be::COMMAND_COMPLETE | Be::EMPTY_QUERY_RESPONSE => {
                let responses = self
                    .port
                    .responses(conn_id)
                    .ok_or(Error::Protocol("CommandComplete with unknown connection"))?;
                let front = responses.front_kind(region);
                match front {
                    FrontKind::Empty => Err(Error::Protocol("CommandComplete with empty pipeline")),
                    FrontKind::Boundary => Err(Error::Protocol(
                        "CommandComplete past pipeline batch boundary",
                    )),
                    FrontKind::Slot(_) | FrontKind::Detached => {
                        responses.complete(region);
                        self.port.shared.inflight_total.dec();
                        self.port.shared.dec_inflight(conn_id);
                        Ok(())
                    }
                }
            }
            Be::COPY_DATA => {
                let bytes = head_payload.len();
                self.port
                    .responses(conn_id)
                    .ok_or(Error::Protocol("CopyData with unknown connection"))?
                    .try_push(region, Ok(head_payload), bytes, 1);
                Ok(())
            }
            Be::NOTIFICATION_RESPONSE => {
                if let Some(n) = parse_notification(&head_payload) {
                    self.port.shared.push_notification(n);
                }
                Ok(())
            }
            Be::DATA_ROW => {
                let responses = self
                    .port
                    .responses(conn_id)
                    .ok_or(Error::Protocol("DataRow with unknown connection"))?;
                let front = responses.front_kind(region);
                match front {
                    FrontKind::Slot(_) | FrontKind::Detached => {
                        let bytes = head_payload.len();
                        responses.try_push(region, Ok(head_payload), bytes, 1);
                        Ok(())
                    }
                    _ => Err(Error::Protocol("DataRow with empty pipeline")),
                }
            }
            Be::ERROR_RESPONSE => {
                let db = Box::new(parse_db_error(&head_payload));
                let responses = self
                    .port
                    .responses(conn_id)
                    .ok_or(Error::Protocol("ErrorResponse with unknown connection"))?;
                let front = responses.front_kind(region);
                match front {
                    FrontKind::Empty | FrontKind::Boundary => Err(Error::Db(db)),
                    FrontKind::Slot(_) | FrontKind::Detached => {
                        responses.fail_one(region, || Err(Error::Db(db)));
                        self.port.shared.inflight_total.dec();
                        self.port.shared.dec_inflight(conn_id);
                        conn_state.error_skip = true;
                        Ok(())
                    }
                }
            }
            Be::READY_FOR_QUERY => {
                let server_in_tx = matches!(head_payload.first().copied(), Some(b'T') | Some(b'E'));
                if server_in_tx && !self.port.shared.is_exclusive(conn_id) {
                    return Err(Error::Protocol(
                        "server transaction escaped its client lease",
                    ));
                }
                let skip = conn_state.error_skip;
                loop {
                    let responses = self
                        .port
                        .responses(conn_id)
                        .ok_or(Error::Protocol("ReadyForQuery with unknown connection"))?;
                    let front = responses.front_kind(region);
                    match front {
                        FrontKind::Empty => break,
                        FrontKind::Boundary => {
                            responses.pop_boundary(region);
                            break;
                        }
                        FrontKind::Slot(_) | FrontKind::Detached => {
                            if skip {
                                responses.fail_one(region, || {
                                    Err(Error::Other(
                                        "query skipped: earlier error in pipeline batch".into(),
                                    ))
                                });
                            } else {
                                responses.complete(region);
                            }
                            self.port.shared.inflight_total.dec();
                            self.port.shared.dec_inflight(conn_id);
                        }
                    }
                }
                conn_state.error_skip = false;
                if self.port.shared.transaction_ready(conn_id, !server_in_tx) {
                    Ok(())
                } else {
                    Err(Error::Protocol(
                        "transaction finalizer left the connection in a transaction",
                    ))
                }
            }
            other => Err(Error::ProtocolOwned(format!(
                "unexpected message {} in ready phase",
                other as char
            ))),
        }
    }
}

impl<'d, I: QuerySet> connector::Session<'d> for Session<'d, I> {
    type Codec = Codec;
    type ConnState = ConnState;
    type Send = SendFrame<'d>;

    fn codec(&self) -> &Codec {
        &self.codec
    }

    fn activate(
        &self,
        token: Token,
        ready: dope::driver::ready::ReadyKey<'d>,
        _region: &mut RegionToken<'d>,
    ) {
        self.port.activate(token, ready);
    }

    fn connect(&mut self, ctx: &mut Ctx<'_, 'd, Self>) {
        let conn_state = &mut *ctx.state;
        let out = &mut *ctx.sink;
        self.port.shared.clear_fatal();
        let frame = self.port.encode(|frame| {
            encode::startup(
                frame,
                self.port.shared.database.user(),
                self.port.shared.database.database(),
                self.port.shared.database.application_name(),
                self.port.shared.database.options(),
                self.port.shared.database.statement_timeout_ms(),
            );
        });
        match frame.and_then(|frame| Self::enqueue(out, frame)) {
            Ok(()) => {
                conn_state.phase = Phase::StartupSent;
            }
            Err(error) => self.fail(ctx.conn_id, conn_state, error, ctx.region),
        }
    }

    fn flush_trailer(&mut self, ctx: &mut Ctx<'_, 'd, Self>) {
        self.port.shared.egress_waiters.as_ref().wake();
        let conn_id = ctx.conn_id;
        let conn_state = &mut *ctx.state;
        let out = &mut *ctx.sink;
        if !matches!(conn_state.phase, Phase::Ready) {
            return;
        }
        if self.port.unsynced(conn_id) == 0 || !self.port.batch_open(conn_id) {
            return;
        }
        if !self.port.can_push_boundary(conn_id, ctx.region) {
            self.port.close(conn_id);
            return;
        }
        let committed = {
            let mut stage = out.wire_stage();
            encode::sync(&mut stage);
            stage.commit()
        };
        if committed != 0 {
            let marked = self.port.push_boundary(conn_id, ctx.region);
            debug_assert!(marked);
        }
    }

    fn response(&mut self, head: Frame, ctx: &mut Ctx<'_, 'd, Self>) {
        let conn_id = ctx.conn_id;
        let conn_state = &mut *ctx.state;
        let out = &mut *ctx.sink;
        let typ = head.typ;
        if typ == OVERSIZE_FRAME {
            self.fail(
                conn_id,
                conn_state,
                Error::Protocol("server frame exceeds maximum size"),
                ctx.region,
            );
            return;
        }
        let prev_phase_was_awaiting_ready = matches!(conn_state.phase, Phase::AwaitingReady);
        let result = match conn_state.phase {
            Phase::StartupSent | Phase::AwaitingReady => {
                self.handle_startup(conn_id, conn_state, typ, &head.payload, out)
            }
            Phase::AwaitingSaslContinue { .. } | Phase::AwaitingSaslFinal { .. } => {
                self.handle_sasl(conn_state, typ, &head.payload, out)
            }
            Phase::Preparing { remaining } => {
                self.handle_preparing(conn_id, typ, &head.payload, conn_state, remaining)
            }
            Phase::Ready | Phase::CopyInActive | Phase::CopyOutActive => {
                let r = self.handle_ready(conn_id, typ, head.payload, conn_state, ctx.region);
                if matches!(
                    typ,
                    Be::COMMAND_COMPLETE | Be::READY_FOR_QUERY | Be::ERROR_RESPONSE
                ) {
                    self.port.shared.egress_waiters.as_ref().wake();
                }
                r
            }
            Phase::NeedsStartup | Phase::Failed => Ok(()),
        };
        if prev_phase_was_awaiting_ready
            && typ == Be::READY_FOR_QUERY
            && matches!(conn_state.phase, Phase::AwaitingReady)
        {
            match self.send_prepare(out) {
                Ok(count) => conn_state.phase = Phase::Preparing { remaining: count },
                Err(error) => self.fail(conn_id, conn_state, error, ctx.region),
            }
        }
        if let Err(e) = result {
            self.fail(conn_id, conn_state, e, ctx.region);
        }
    }

    fn disconnect(&mut self, ctx: &mut Ctx<'_, 'd, Self>) {
        let conn_id = ctx.conn_id;
        let conn_state = &mut *ctx.state;
        let msg = self
            .port
            .shared
            .fatal_message()
            .unwrap_or_else(|| "connection closed".into());
        let was_ready = matches!(conn_state.phase, Phase::Ready);
        conn_state.pending_close = false;
        let n = self.port.responses(conn_id).map_or(0, |responses| {
            responses.fail_all(ctx.region, || Err(Error::Other(msg.clone())))
        });
        self.port.set_batch_open(conn_id, false);
        let s = &self.port.shared;
        s.inflight_total.dec_n(n);
        s.clear_backend(conn_id);
        if was_ready {
            s.remove_ready(conn_id);
        }
        s.clear_transaction(conn_id, false);
        s.wake_all();
        self.port.deactivate(conn_id, ctx.region);
    }

    fn drain_requests(
        &self,
        token: Token,
        push: impl FnMut(Self::Send) -> Result<(), Self::Send>,
        region: &mut RegionToken<'d>,
    ) -> connector::Requests {
        self.port.drain_requests(token, push, region)
    }

    fn sent(&self, _token: Token, _sent: usize) {
        self.port.shared.egress_waiters.as_ref().wake();
    }

    fn defer_close(&self, token: Token, _state: &ConnState, region: &mut RegionToken<'d>) -> bool {
        self.port.response_len(token, region) != 0
    }

    fn is_drained(&self, token: Token, _state: &ConnState, region: &mut RegionToken<'d>) -> bool {
        self.port.responses_empty(token, region)
    }
}
