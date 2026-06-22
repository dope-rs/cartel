use std::cell::Cell;
use std::collections::VecDeque;
use std::marker::PhantomData;

use cartel_core::{FatalSlot, FrontKind, Inflight, Slab};
use dope::manifold::connector;
use dope::manifold::connector::session::{IOV_CAP, Queue};
use dope::manifold::connector::{Close, Ctx};
use dope::runtime::token::Token;
use dope::{WakeRef, WakerSet};
use o3::buffer::{self, Owned};

use crate::decode::{AuthRequest, parse_auth, parse_db_error, parse_notification};
use crate::query::QuerySet;
use crate::scram::Scram;
use crate::wire::Be;
use crate::{Config, Error, Notification, encode};

pub(super) type RowItem = Result<buffer::Shared, Error>;

const MAX_PG_CONNS: usize = 256;

pub struct Frame {
    pub typ: u8,
    pub payload: buffer::Shared,
}

#[derive(Default)]
pub struct ConnState {
    phase: Phase,
    pub(super) responses: Slab<RowItem>,
    pub(super) unsynced: u32,
    pub(super) batch_open: bool,
    error_skip: bool,
    pending_close: bool,
    close_permanent: bool,
}

impl ConnState {
    fn fail_all_in_flight(&mut self, msg: &str) -> usize {
        let n = self
            .responses
            .fail_all(|| Err(Error::Other(msg.to_string())));
        self.unsynced = 0;
        self.error_skip = false;
        self.batch_open = false;
        n
    }

    pub(super) fn push_batch_boundary(&mut self) {
        self.responses.mark_boundary();
        self.unsynced = 0;
        self.batch_open = false;
    }
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
        self.responses.depth() != 0
    }

    fn is_drained(&self) -> bool {
        self.responses.is_drained()
    }
}

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub enum PickPolicy {
    #[default]
    RoundRobin,
    LeastInflight,
}

pub(super) struct Shared {
    config: Config,
    ready_conns: Vec<Token>,
    inflight: Box<[u32]>,
    rr_idx: Cell<usize>,
    pub(super) policy: PickPolicy,
    pub(super) ready_count: usize,
    ready: bool,
    ready_wakers: WakerSet,
    fatal: FatalSlot<Error>,
    notifications: VecDeque<Notification>,
    notification_wakers: WakerSet,
    pub(super) inflight_total: Inflight,
    pub(super) egress_drain_wakers: WakerSet,
    backend_pids: Box<[i32]>,
}

impl Shared {
    fn new(config: Config) -> Self {
        Self {
            config,
            ready_conns: Vec::new(),
            inflight: vec![0u32; MAX_PG_CONNS].into_boxed_slice(),
            rr_idx: Cell::new(0),
            policy: PickPolicy::default(),
            ready_count: 0,
            ready: false,
            ready_wakers: WakerSet::new(),
            fatal: FatalSlot::default(),
            notifications: VecDeque::new(),
            notification_wakers: WakerSet::new(),
            inflight_total: Inflight::default(),
            egress_drain_wakers: WakerSet::new(),
            backend_pids: vec![0i32; MAX_PG_CONNS].into_boxed_slice(),
        }
    }

    pub(super) fn backend_pid_for(&self, slot: Token) -> Option<i32> {
        self.backend_pids
            .get(slot.slot().raw() as usize)
            .copied()
            .filter(|&p| p != 0)
    }

    pub(super) fn pop_notification(&mut self) -> Option<Notification> {
        self.notifications.pop_front()
    }

    pub(super) fn register_notification_waker(&mut self, w: WakeRef) {
        self.notification_wakers.register(w);
    }

    pub(super) fn register_egress_drain_waker(&mut self, w: WakeRef) {
        self.egress_drain_wakers.register(w);
    }

    pub(super) fn register_ready_waker(&mut self, w: WakeRef) {
        self.ready_wakers.register(w);
    }

    pub(super) fn backpressure(&self, queued: usize) -> Error {
        Error::Backpressure {
            inflight: self.inflight_total.total,
            queued,
            cap: self.inflight_total.max,
        }
    }

    pub(super) fn is_ready(&self) -> bool {
        self.ready
    }

    pub(super) fn is_failed(&self) -> bool {
        self.fatal.is_failed()
    }

    pub(super) fn pick_conn(&self, pin: Option<Token>) -> Option<Token> {
        if let Some(p) = pin {
            return self.ready_conns.iter().copied().find(|c| *c == p);
        }
        let n = self.ready_conns.len();
        if n == 0 {
            return None;
        }
        let idx = match self.policy {
            PickPolicy::RoundRobin => {
                let i = self.rr_idx.get() % n;
                self.rr_idx.set(i.wrapping_add(1));
                i
            }
            PickPolicy::LeastInflight => {
                let mut best = 0usize;
                let mut best_v = u32::MAX;
                for (i, c) in self.ready_conns.iter().enumerate() {
                    let v = self
                        .inflight
                        .get(c.slot().raw() as usize)
                        .copied()
                        .unwrap_or(0);
                    if v < best_v {
                        best_v = v;
                        best = i;
                    }
                }
                best
            }
        };
        Some(self.ready_conns[idx])
    }

    pub(super) fn inc_inflight(&mut self, slot: Token) {
        let i = slot.slot().raw() as usize;
        if i < self.inflight.len() {
            self.inflight[i] = self.inflight[i].saturating_add(1);
        }
    }

    fn dec_inflight(&mut self, slot: Token) {
        let i = slot.slot().raw() as usize;
        if i < self.inflight.len() {
            self.inflight[i] = self.inflight[i].saturating_sub(1);
        }
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

pub struct Codec;

impl connector::Codec for Codec {
    type Head = Frame;
    type ParseState = ();

    fn parse(&self, _state: &mut (), buf: &buffer::Shared) -> Option<(Frame, usize)> {
        if buf.len() < 5 {
            return None;
        }
        let typ = buf[0];
        let len = u32::from_be_bytes(buf[1..5].try_into().unwrap()) as usize;
        if len < 4 {
            return Some((
                Frame {
                    typ,
                    payload: buffer::Shared::new(),
                },
                buf.len(),
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

pub struct Session<I: QuerySet> {
    codec: Codec,
    pub(super) shared: Shared,
    _instance: PhantomData<fn() -> I>,
}

impl<I: QuerySet> Session<I> {
    pub fn new(config: Config) -> Self {
        Self {
            codec: Codec,
            shared: Shared::new(config),
            _instance: PhantomData,
        }
    }

    fn fail(&mut self, conn_id: Token, conn_state: &mut ConnState, err: Error) {
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
        let n = conn_state.fail_all_in_flight(&msg);
        let s = &mut self.shared;
        s.inflight_total.dec_n(n);
        if permanent {
            s.fatal.record(err);
        }
        if was_ready && let Some(idx) = s.ready_conns.iter().position(|c| *c == conn_id) {
            s.ready_conns.remove(idx);
            if s.ready_count > 0 {
                s.ready_count -= 1;
            }
        }
        if s.ready_count == 0 {
            s.ready = false;
        }
        s.ready_wakers.drain_wake();
        s.egress_drain_wakers.drain_wake();
    }

    fn send_prepare(&self, out: &mut Queue<IOV_CAP>) -> u32 {
        let mut count = 0u32;
        let mut buf = Owned::with_capacity(256);
        for group in I::GROUPS {
            for meta in *group {
                encode::parse(&mut buf, meta.name, meta.sql, meta.param_oids);
                count += 1;
            }
        }
        encode::sync(&mut buf);
        out.push(buf.freeze());
        count
    }

    fn handle_startup(
        &mut self,
        conn_id: Token,
        conn_state: &mut ConnState,
        typ: u8,
        payload: &[u8],
        out: &mut Queue<IOV_CAP>,
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
                        let scram = Scram::new(&self.shared.config.password);
                        let mech = scram.pick_mechanism(&mechanisms)?;
                        let client_first = scram.client_first();
                        let mut buf = Owned::with_capacity(128);
                        encode::sasl_initial_response(&mut buf, mech, client_first.as_bytes());
                        out.push(buf.freeze());
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
                if payload.len() >= 8 {
                    let pid = i32::from_be_bytes(payload[0..4].try_into().unwrap());
                    let sl = conn_id.slot().raw() as usize;
                    if sl < MAX_PG_CONNS {
                        self.shared.backend_pids[sl] = pid;
                    }
                }
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
        out: &mut Queue<IOV_CAP>,
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
                let mut buf = Owned::with_capacity(128);
                encode::sasl_response(&mut buf, client_final.as_bytes());
                out.push(buf.freeze());
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
                self.shared.ready_conns.push(conn_id);
                let sl = conn_id.slot().raw() as usize;
                if sl < MAX_PG_CONNS {
                    self.shared.inflight[sl] = 0;
                }
                self.shared.ready_count += 1;
                self.shared.ready = true;
                self.shared.ready_wakers.drain_wake();
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
                match conn_state.responses.front_kind() {
                    FrontKind::Empty => Err(Error::Protocol("CommandComplete with empty pipeline")),
                    FrontKind::Boundary => Err(Error::Protocol(
                        "CommandComplete past pipeline batch boundary",
                    )),
                    FrontKind::Slot(_) | FrontKind::Detached => {
                        conn_state.responses.complete();
                        self.shared.inflight_total.dec();
                        self.shared.dec_inflight(conn_id);
                        Ok(())
                    }
                }
            }
            Be::COPY_DATA => {
                conn_state.responses.push(Ok(head_payload));
                Ok(())
            }
            Be::NOTIFICATION_RESPONSE => {
                if let Some(n) = parse_notification(&head_payload) {
                    self.shared.notifications.push_back(n);
                    self.shared.notification_wakers.drain_wake();
                }
                Ok(())
            }
            Be::DATA_ROW => match conn_state.responses.front_kind() {
                FrontKind::Slot(_) | FrontKind::Detached => {
                    conn_state.responses.push(Ok(head_payload));
                    Ok(())
                }
                _ => Err(Error::Protocol("DataRow with empty pipeline")),
            },
            Be::ERROR_RESPONSE => {
                let db = Box::new(parse_db_error(&head_payload));
                match conn_state.responses.front_kind() {
                    FrontKind::Empty | FrontKind::Boundary => Err(Error::Db(db)),
                    FrontKind::Slot(_) | FrontKind::Detached => {
                        conn_state.responses.fail_one(|| Err(Error::Db(db)));
                        self.shared.inflight_total.dec();
                        self.shared.dec_inflight(conn_id);
                        conn_state.error_skip = true;
                        Ok(())
                    }
                }
            }
            Be::READY_FOR_QUERY => {
                let skip = conn_state.error_skip;
                loop {
                    match conn_state.responses.front_kind() {
                        FrontKind::Empty => break,
                        FrontKind::Boundary => {
                            conn_state.responses.pop_boundary();
                            break;
                        }
                        FrontKind::Slot(_) | FrontKind::Detached => {
                            if skip {
                                conn_state.responses.fail_one(|| {
                                    Err(Error::Other(
                                        "query skipped: earlier error in pipeline batch".into(),
                                    ))
                                });
                            } else {
                                conn_state.responses.complete();
                            }
                            self.shared.inflight_total.dec();
                            self.shared.dec_inflight(conn_id);
                        }
                    }
                }
                conn_state.error_skip = false;
                Ok(())
            }
            other => Err(Error::ProtocolOwned(format!(
                "unexpected message {} in ready phase",
                other as char
            ))),
        }
    }
}

impl<I: QuerySet> connector::Session for Session<I> {
    type Codec = Codec;
    type ConnState = ConnState;

    fn codec(&self) -> &Codec {
        &self.codec
    }

    fn connect(&mut self, ctx: &mut Ctx<'_, Self>) {
        let conn_state = &mut *ctx.state;
        let out = &mut *ctx.sink;
        self.shared.fatal.clear();
        let mut buf = Owned::with_capacity(64);
        encode::startup(
            &mut buf,
            &self.shared.config.user,
            &self.shared.config.database,
            &self.shared.config.application_name,
            &self.shared.config.options,
        );
        out.push(buf.freeze());
        conn_state.phase = Phase::StartupSent;
    }

    fn flush_trailer(&mut self, ctx: &mut Ctx<'_, Self>) {
        if !self.shared.egress_drain_wakers.is_empty() {
            self.shared.egress_drain_wakers.drain_wake();
        }
        let conn_state = &mut *ctx.state;
        let out = &mut *ctx.sink;
        if !matches!(conn_state.phase, Phase::Ready) {
            return;
        }
        if conn_state.unsynced == 0 || !conn_state.batch_open {
            return;
        }
        let n = {
            let mut stage = out.wire_stage();
            encode::sync(&mut stage);
            stage.len()
        };
        out.wire_commit(n);
        conn_state.push_batch_boundary();
    }

    fn response(&mut self, head: Frame, ctx: &mut Ctx<'_, Self>) {
        let conn_id = ctx.conn_id;
        let conn_state = &mut *ctx.state;
        let out = &mut *ctx.sink;
        let typ = head.typ;
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
                let r = self.handle_ready(conn_id, typ, head.payload, conn_state);
                if matches!(
                    typ,
                    Be::COMMAND_COMPLETE | Be::READY_FOR_QUERY | Be::ERROR_RESPONSE
                ) && !self.shared.egress_drain_wakers.is_empty()
                {
                    self.shared.egress_drain_wakers.drain_wake();
                }
                r
            }
            Phase::NeedsStartup | Phase::Failed => Ok(()),
        };
        if prev_phase_was_awaiting_ready
            && typ == Be::READY_FOR_QUERY
            && matches!(conn_state.phase, Phase::AwaitingReady)
        {
            let count = self.send_prepare(out);
            conn_state.phase = Phase::Preparing { remaining: count };
        }
        if let Err(e) = result {
            self.fail(conn_id, conn_state, e);
        }
    }

    fn disconnect(&mut self, ctx: &mut Ctx<'_, Self>) {
        let conn_id = ctx.conn_id;
        let conn_state = &mut *ctx.state;
        let msg = self
            .shared
            .fatal
            .as_ref()
            .map(|e| e.to_string())
            .unwrap_or_else(|| "connection closed".into());
        let was_ready = matches!(conn_state.phase, Phase::Ready);
        conn_state.pending_close = false;
        let n = conn_state.fail_all_in_flight(&msg);
        let s = &mut self.shared;
        s.inflight_total.dec_n(n);
        let sl = conn_id.slot().raw() as usize;
        if sl < MAX_PG_CONNS {
            s.backend_pids[sl] = 0;
        }
        if was_ready {
            if let Some(idx) = s.ready_conns.iter().position(|c| *c == conn_id) {
                s.ready_conns.remove(idx);
            }
            if s.ready_count > 0 {
                s.ready_count -= 1;
            }
        }
        if s.ready_count == 0 {
            s.ready = false;
        }
        s.ready_wakers.drain_wake();
        s.notification_wakers.drain_wake();
        s.egress_drain_wakers.drain_wake();
    }
}
