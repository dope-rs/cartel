use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use cartel_core::{Extract, Registrable, Reply, ReplyStream, Slot};
use dope::WakeRef;
use dope::fiber::{Fiber, Holding};
use dope::manifold::connector::Connector;
use dope::manifold::connector::session::Stage;
use dope::manifold::connector::source::{Dialer, Static};
use dope::manifold::env::{Bundle, Env};
use dope::runtime::profile::Production;
use dope::runtime::token::Token;
use dope::transport::{Tcp, Transport};
use dope::wire::Identity;
use o3::buffer::{Owned, Shared};

use crate::protocol::{RowItem, Session};
use crate::query::{HasGroup, QuerySet, TypedQuery};
use crate::value::{BindWriter, RowReader};
use crate::{Error, encode, protocol};

pub trait PgTransport: Transport<Addr: Clone> {}

impl<T: Transport<Addr: Clone>> PgTransport for T {}

pub type PgHolding<'d, I, S, E> = Holding<'d, Connector<0, Session<I>, S, E>>;

type Decoder<R> = fn(&mut RowReader<'_>) -> Result<R, Error>;

fn decode_row<R>(decoder: Decoder<R>, payload: &Shared) -> Result<R, Error> {
    if payload.len() < 2 {
        return Err(Error::Protocol("data row payload truncated"));
    }
    let mut reader = RowReader::new(payload);
    decoder(&mut reader)
}

pub(super) struct ExtractAll;

impl Extract<RowItem> for ExtractAll {
    type Output = Result<Vec<Shared>, Error>;

    fn extract(slot: &mut Slot<RowItem>) -> Option<Self::Output> {
        if !slot.completed() {
            return None;
        }
        let mut rows = Vec::new();
        while let Some(item) = slot.pop() {
            match item {
                Ok(payload) => rows.push(payload),
                Err(e) => return Some(Err(e)),
            }
        }
        Some(Ok(rows))
    }
}

pub struct ExtractUnit;

impl Extract<RowItem> for ExtractUnit {
    type Output = Result<(), Error>;

    fn extract(slot: &mut Slot<RowItem>) -> Option<Self::Output> {
        if !slot.completed() {
            return None;
        }
        while let Some(item) = slot.pop() {
            if let Err(e) = item {
                return Some(Err(e));
            }
        }
        Some(Ok(()))
    }
}

pub(super) struct ExtractOne;

impl Extract<RowItem> for ExtractOne {
    type Output = Result<Shared, Error>;
    const SYNC_AFTER: bool = true;

    fn extract(slot: &mut Slot<RowItem>) -> Option<Self::Output> {
        slot.pop()
    }
}

pub(super) struct ExtractFirst;

impl Extract<RowItem> for ExtractFirst {
    type Output = Result<Option<Shared>, Error>;

    fn extract(slot: &mut Slot<RowItem>) -> Option<Self::Output> {
        if !slot.completed() {
            return None;
        }
        match slot.pop() {
            Some(Ok(payload)) => Some(Ok(Some(payload))),
            Some(Err(e)) => Some(Err(e)),
            None => Some(Ok(None)),
        }
    }
}

pub(super) struct Throttle {
    request: Request,
    conn: Token,
}

struct Emit;

impl Emit {
    fn frame_typed<Q: TypedQuery, X: Extract<RowItem>>(
        stage: &mut Stage<'_>,
        params: Q::Params<'_>,
    ) -> bool {
        let pos = encode::bind_header(
            stage,
            "",
            Q::STATEMENT_NAME,
            Q::PARAM_FORMAT_CODES,
            Q::N_PARAMS,
        );
        {
            let mut bw = BindWriter::new(stage);
            Q::encode_params(params, &mut bw);
        }
        encode::bind_trailer(stage, pos, Q::RESULT_FORMAT_CODES);
        encode::execute(stage);
        if X::SYNC_AFTER {
            encode::sync(stage);
        }
        !stage.overflowed()
    }

    fn frame_request(stage: &mut Stage<'_>, req: &Request) -> bool {
        encode::parse(stage, "", &req.sql, req.param_oids);
        let pos = encode::bind_header(stage, "", "", req.param_formats, req.n_params);
        stage.extend_from_slice(&req.param_buf);
        encode::bind_trailer(stage, pos, req.result_formats);
        encode::execute(stage);
        match &req.extra {
            Extra::Plain => {
                encode::sync(stage);
            }
            Extra::CopyIn { data } => {
                encode::copy_data(stage, data);
                encode::copy_done(stage);
                encode::sync(stage);
            }
            Extra::CopyInOpen => {}
        }
        !stage.overflowed()
    }
}

enum DispatchOutcome {
    Enqueued { conn: Token },
    Throttled { throttle: Throttle },
    NoConn { request: Request },
    Failed(Error),
}

#[derive(Clone, Copy)]
enum BoundaryAction {
    Close,
    Open,
    External,
}

pub(super) enum DispatchedStream<'d, I, S, E, X>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
    X: Extract<RowItem>,
{
    Pending {
        reply: ReplyStream<'d, RowItem, X>,
    },
    Throttled {
        conn: PgHolding<'d, I, S, E>,
        reply: ReplyStream<'d, RowItem, X>,
        throttle: Throttle,
    },
    Connecting {
        conn: PgHolding<'d, I, S, E>,
        pin: Option<Token>,
        reply: ReplyStream<'d, RowItem, X>,
        request: Request,
    },
    Failed(Option<Error>),
}

impl<'d, I, S, E, X> DispatchedStream<'d, I, S, E, X>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
    X: Extract<RowItem>,
{
    fn poll_settle(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if let DispatchedStream::Connecting {
            conn,
            pin,
            reply,
            request,
        } = self
        {
            match Disp::retry_connecting(*conn, *pin, cx, reply, request) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(DispatchOutcome::Enqueued { .. }) => {
                    let reply = std::mem::replace(reply, ReplyStream::new());
                    *self = DispatchedStream::Pending { reply };
                }
                Poll::Ready(DispatchOutcome::Throttled { throttle }) => {
                    let reply = std::mem::replace(reply, ReplyStream::new());
                    *self = DispatchedStream::Throttled {
                        conn: *conn,
                        reply,
                        throttle,
                    };
                }
                Poll::Ready(DispatchOutcome::NoConn { .. }) => {
                    unreachable!("retry_connecting maps NoConn to Pending/Failed")
                }
                Poll::Ready(DispatchOutcome::Failed(e)) => {
                    *self = DispatchedStream::Failed(Some(e));
                }
            }
        }
        if let DispatchedStream::Throttled {
            conn,
            reply,
            throttle,
        } = self
        {
            match Disp::retry_throttled(*conn, cx, reply, throttle) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => {
                    let reply = std::mem::replace(reply, ReplyStream::new());
                    *self = DispatchedStream::Pending { reply };
                }
                Poll::Ready(Err(e)) => {
                    *self = DispatchedStream::Failed(Some(e));
                }
            }
        }
        Poll::Ready(())
    }
}

pub trait PgOps<'d, I, S, E>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    fn holding(&self) -> PgHolding<'d, I, S, E>;

    fn backend_pid(&self) -> Option<i32> {
        None
    }

    fn is_failed(&self) -> bool {
        self.holding().session().shared.is_failed()
    }

    fn is_ready(&self) -> bool {
        self.holding().session().shared.is_ready()
    }

    fn live_count(&self) -> usize {
        self.holding().session().shared.ready_count
    }

    fn set_max_inflight(&self, cap: usize) {
        self.holding()
            .hold()
            .as_mut()
            .session_mut()
            .shared
            .inflight_total
            .set_max(cap);
    }

    fn set_pick_policy(&self, policy: protocol::PickPolicy) {
        self.holding().hold().as_mut().session_mut().shared.policy = policy;
    }

    fn pick_policy(&self) -> protocol::PickPolicy {
        self.holding().session().shared.policy
    }

    fn run_one<Q>(
        &self,
        params: Q::Params<'_>,
    ) -> Fiber<'d, impl Future<Output = Result<Q::Row, Error>> + use<'d, I, S, E, Q, Self>>
    where
        Q: TypedQuery,
        I: HasGroup<Q::Group>,
        Q::Group: crate::query::QueryGroup,
    {
        let decoder = Q::decode_row;
        let dispatched =
            Disp::dispatch_typed::<Q, ExtractFirst, I, S, E>(self.holding(), self.pin(), params);
        Fiber::new(async move {
            match dispatched.await? {
                None => Err(Error::NotFound),
                Some(payload) => decode_row(decoder, &payload),
            }
        })
    }

    fn run_first<Q>(
        &self,
        params: Q::Params<'_>,
    ) -> Fiber<'d, impl Future<Output = Result<Option<Q::Row>, Error>> + use<'d, I, S, E, Q, Self>>
    where
        Q: TypedQuery,
        I: HasGroup<Q::Group>,
        Q::Group: crate::query::QueryGroup,
    {
        let decoder = Q::decode_row;
        let dispatched =
            Disp::dispatch_typed::<Q, ExtractFirst, I, S, E>(self.holding(), self.pin(), params);
        Fiber::new(async move {
            match dispatched.await? {
                None => Ok(None),
                Some(payload) => decode_row(decoder, &payload).map(Some),
            }
        })
    }

    fn run_all<Q>(
        &self,
        params: Q::Params<'_>,
    ) -> Fiber<'d, impl Future<Output = Result<Vec<Q::Row>, Error>> + use<'d, I, S, E, Q, Self>>
    where
        Q: TypedQuery,
        I: HasGroup<Q::Group>,
        Q::Group: crate::query::QueryGroup,
    {
        let decoder = Q::decode_row;
        let dispatched =
            Disp::dispatch_typed::<Q, ExtractAll, I, S, E>(self.holding(), self.pin(), params);
        Fiber::new(async move {
            let rows = dispatched.await?;
            let mut out = Vec::with_capacity(rows.len());
            for payload in &rows {
                out.push(decode_row(decoder, payload)?);
            }
            Ok(out)
        })
    }

    fn run_no_rows<Q>(
        &self,
        params: Q::Params<'_>,
    ) -> Fiber<'d, Dispatched<'d, I, S, E, ExtractUnit>>
    where
        Q: TypedQuery<Row = ()>,
        I: HasGroup<Q::Group>,
        Q::Group: crate::query::QueryGroup,
    {
        Fiber::new(Disp::dispatch_typed::<Q, ExtractUnit, I, S, E>(
            self.holding(),
            self.pin(),
            params,
        ))
    }

    fn run_stream<Q>(&self, params: Q::Params<'_>) -> RunStream<'d, I, S, E, Q::Row>
    where
        Q: TypedQuery,
        I: HasGroup<Q::Group>,
        Q::Group: crate::query::QueryGroup,
    {
        RunStream {
            state: Disp::dispatch_stream::<Q, ExtractOne, I, S, E>(
                self.holding(),
                self.pin(),
                params,
            ),
            decoder: Q::decode_row,
        }
    }

    fn copy_in(&self, sql: &str, data: &[u8]) -> Fiber<'d, Dispatched<'d, I, S, E, ExtractUnit>> {
        let mut buf = Owned::with_capacity(data.len());
        buf.extend_from_slice(data);
        Fiber::new(Disp::dispatch_raw::<ExtractUnit, I, S, E>(
            self.holding(),
            self.pin(),
            Request::raw_extra(sql, Extra::CopyIn { data: buf }),
        ))
    }

    fn copy_in_stream(&self, sql: &str) -> Result<CopyInGuard<'d, I, S, E>, Error> {
        let holding = self.holding();
        let req = Request::raw_extra(sql, Extra::CopyInOpen);
        let mut reply = Reply::<RowItem, ExtractUnit>::new();
        match Disp::try_dispatch_reply(holding, self.pin(), &mut reply, &req) {
            DispatchOutcome::Enqueued { conn } => Ok(CopyInGuard {
                conn: holding,
                pin: conn,
                reply: Some(reply),
            }),
            DispatchOutcome::Throttled { .. } => Err(holding.session().shared.backpressure(0)),
            DispatchOutcome::NoConn { .. } => Err(Error::NoReadyConn),
            DispatchOutcome::Failed(e) => Err(e),
        }
    }

    fn copy_out(&self, sql: &str) -> CopyOutStream<'d, I, S, E> {
        CopyOutStream {
            state: Disp::dispatch_stream_raw::<ExtractOne, I, S, E>(
                self.holding(),
                self.pin(),
                Request::raw(sql),
            ),
        }
    }

    fn dispatch_sql(&self, sql: &str) -> Dispatched<'d, I, S, E, ExtractUnit> {
        Disp::dispatch_raw::<ExtractUnit, I, S, E>(self.holding(), self.pin(), Request::raw(sql))
    }

    fn next_notification(&self) -> NextNotification<'d, I, S, E> {
        NextNotification {
            conn: self.holding(),
        }
    }

    fn listen(
        &self,
        channel: impl Into<String>,
    ) -> Fiber<'d, impl Future<Output = Result<crate::tx::ListenGuard<'d, I, S, E>, Error>>> {
        let ch = channel.into();
        let sql = format!("LISTEN \"{}\"", ch.replace('"', "\"\""));
        let holding = self.holding();
        let dispatched =
            Disp::dispatch_raw::<ExtractUnit, I, S, E>(holding, self.pin(), Request::raw(&sql));
        Fiber::new(async move {
            let conn = dispatched.resolved_conn();
            dispatched.await?;
            let pin = conn.ok_or_else(|| Error::Other("listen lost target conn".into()))?;
            Ok(crate::tx::ListenGuard::from_parts(holding, pin, ch))
        })
    }

    fn pin(&self) -> Option<Token> {
        None
    }

    fn batch_pin(&self) -> Option<Token> {
        self.pin()
            .or_else(|| self.holding().session().shared.pick_conn(None))
    }
}

impl<'d, I, S, E> PgOps<'d, I, S, E> for PgHolding<'d, I, S, E>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    fn holding(&self) -> PgHolding<'d, I, S, E> {
        *self
    }
}

pub struct Runner<'d, I, S, E>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    holding: PgHolding<'d, I, S, E>,
    pin: Option<Token>,
}

impl<'d, I, S, E> Runner<'d, I, S, E>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    pub fn new(holding: PgHolding<'d, I, S, E>, pin: Option<Token>) -> Self {
        Self { holding, pin }
    }
}

impl<'d, I, S, E> Clone for Runner<'d, I, S, E>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    fn clone(&self) -> Self {
        Self {
            holding: self.holding,
            pin: self.pin,
        }
    }
}

impl<'d, I, S, E> PgOps<'d, I, S, E> for Runner<'d, I, S, E>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    fn holding(&self) -> PgHolding<'d, I, S, E> {
        self.holding
    }

    fn pin(&self) -> Option<Token> {
        self.pin
    }
}

pub(super) struct Disp;

impl Disp {
    pub(super) fn dispatch_typed<'d, Q, X, I, S, E>(
        holding: PgHolding<'d, I, S, E>,
        pin: Option<Token>,
        params: Q::Params<'_>,
    ) -> Dispatched<'d, I, S, E, X>
    where
        Q: TypedQuery,
        X: Extract<RowItem>,
        I: QuerySet + HasGroup<Q::Group>,
        Q::Group: crate::query::QueryGroup,
        S: Dialer<E::Transport>,
        E: Env,
        E::Transport: Transport<Addr: Clone>,
    {
        let mut reply = Reply::<RowItem, X>::new();
        let outcome = Self::try_dispatch_typed::<Q, X, I, S, E>(holding, pin, &mut reply, params);
        Self::reply_state(holding, pin, reply, outcome)
    }

    pub(super) fn dispatch_raw<'d, X, I, S, E>(
        holding: PgHolding<'d, I, S, E>,
        pin: Option<Token>,
        req: Request,
    ) -> Dispatched<'d, I, S, E, X>
    where
        X: Extract<RowItem>,
        I: QuerySet,
        S: Dialer<E::Transport>,
        E: Env,
        E::Transport: Transport<Addr: Clone>,
    {
        let mut reply = Reply::<RowItem, X>::new();
        let outcome = Self::try_dispatch_reply(holding, pin, &mut reply, &req);
        Self::reply_state(holding, pin, reply, outcome)
    }

    fn reply_state<'d, I, S, E, X>(
        holding: PgHolding<'d, I, S, E>,
        pin: Option<Token>,
        reply: Reply<'d, RowItem, X>,
        outcome: DispatchOutcome,
    ) -> Dispatched<'d, I, S, E, X>
    where
        X: Extract<RowItem>,
        I: QuerySet,
        S: Dialer<E::Transport>,
        E: Env,
        E::Transport: Transport<Addr: Clone>,
    {
        let state = match outcome {
            DispatchOutcome::Enqueued { conn } => DispatchState::Pending { conn },
            DispatchOutcome::Throttled { throttle } => DispatchState::Throttled {
                conn: holding,
                throttle,
            },
            DispatchOutcome::NoConn { request } => DispatchState::Connecting {
                conn: holding,
                pin,
                request,
            },
            DispatchOutcome::Failed(e) => DispatchState::Failed(Some(e)),
        };
        Dispatched { reply, state }
    }

    pub(super) fn dispatch_stream<'d, Q, X, I, S, E>(
        holding: PgHolding<'d, I, S, E>,
        pin: Option<Token>,
        params: Q::Params<'_>,
    ) -> DispatchedStream<'d, I, S, E, X>
    where
        Q: TypedQuery,
        X: Extract<RowItem>,
        I: QuerySet + HasGroup<Q::Group>,
        Q::Group: crate::query::QueryGroup,
        S: Dialer<E::Transport>,
        E: Env,
        E::Transport: Transport<Addr: Clone>,
    {
        let mut reply = ReplyStream::<RowItem, X>::new();
        let outcome = Self::try_dispatch_typed::<Q, X, I, S, E>(holding, pin, &mut reply, params);
        Self::stream_state(holding, pin, reply, outcome)
    }

    pub(super) fn dispatch_stream_raw<'d, X, I, S, E>(
        holding: PgHolding<'d, I, S, E>,
        pin: Option<Token>,
        req: Request,
    ) -> DispatchedStream<'d, I, S, E, X>
    where
        X: Extract<RowItem>,
        I: QuerySet,
        S: Dialer<E::Transport>,
        E: Env,
        E::Transport: Transport<Addr: Clone>,
    {
        let mut reply = ReplyStream::<RowItem, X>::new();
        let outcome = Self::try_dispatch_reply(holding, pin, &mut reply, &req);
        Self::stream_state(holding, pin, reply, outcome)
    }

    fn stream_state<'d, I, S, E, X>(
        holding: PgHolding<'d, I, S, E>,
        pin: Option<Token>,
        reply: ReplyStream<'d, RowItem, X>,
        outcome: DispatchOutcome,
    ) -> DispatchedStream<'d, I, S, E, X>
    where
        X: Extract<RowItem>,
        I: QuerySet,
        S: Dialer<E::Transport>,
        E: Env,
        E::Transport: Transport<Addr: Clone>,
    {
        match outcome {
            DispatchOutcome::Enqueued { .. } => DispatchedStream::Pending { reply },
            DispatchOutcome::Throttled { throttle } => DispatchedStream::Throttled {
                conn: holding,
                reply,
                throttle,
            },
            DispatchOutcome::NoConn { request } => DispatchedStream::Connecting {
                conn: holding,
                pin,
                reply,
                request,
            },
            DispatchOutcome::Failed(e) => DispatchedStream::Failed(Some(e)),
        }
    }

    fn try_dispatch_typed<'d, Q, X, I, S, E>(
        holding: PgHolding<'d, I, S, E>,
        pin: Option<Token>,
        reply: &mut impl Registrable<'d, RowItem>,
        params: Q::Params<'_>,
    ) -> DispatchOutcome
    where
        Q: TypedQuery,
        X: Extract<RowItem>,
        I: QuerySet + HasGroup<Q::Group>,
        Q::Group: crate::query::QueryGroup,
        S: Dialer<E::Transport>,
        E: Env,
        E::Transport: Transport<Addr: Clone>,
    {
        if let Err(e) = Self::check_can_dispatch(holding) {
            return DispatchOutcome::Failed(e);
        }
        let conn_id = match holding.session().shared.pick_conn(pin) {
            Some(c) => c,
            None => {
                return match pin {
                    Some(_) => {
                        DispatchOutcome::Failed(Error::Other("pinned conn no longer ready".into()))
                    }
                    None => DispatchOutcome::NoConn {
                        request: Request::typed::<Q>(params),
                    },
                };
            }
        };
        let mut h = holding.hold();
        let mut pool = h.as_mut();
        let Some(channel) = pool.as_mut().state_for(conn_id) else {
            return DispatchOutcome::Failed(Error::Closed);
        };
        let queued = channel.egress_len();
        let staged = {
            let mut stage = channel.wire_stage();
            if Emit::frame_typed::<Q, X>(&mut stage, params) {
                Some(stage.len())
            } else {
                None
            }
        };
        let Some(n) = staged else {
            return DispatchOutcome::Failed(pool.session().shared.backpressure(queued));
        };
        let action = if X::SYNC_AFTER {
            BoundaryAction::Close
        } else {
            BoundaryAction::Open
        };
        Self::commit_staged(pool, conn_id, reply, n, action);
        DispatchOutcome::Enqueued { conn: conn_id }
    }

    fn try_dispatch_reply<'d, I, S, E>(
        holding: PgHolding<'d, I, S, E>,
        pin: Option<Token>,
        reply: &mut impl Registrable<'d, RowItem>,
        req: &Request,
    ) -> DispatchOutcome
    where
        I: QuerySet,
        S: Dialer<E::Transport>,
        E: Env,
        E::Transport: Transport<Addr: Clone>,
    {
        if let Err(e) = Self::check_can_dispatch(holding) {
            return DispatchOutcome::Failed(e);
        }
        let conn_id = match holding.session().shared.pick_conn(pin) {
            Some(c) => c,
            None => {
                return match pin {
                    Some(_) => {
                        DispatchOutcome::Failed(Error::Other("pinned conn no longer ready".into()))
                    }
                    None => DispatchOutcome::NoConn {
                        request: req.clone(),
                    },
                };
            }
        };
        let mut h = holding.hold();
        let pool = h.as_mut();
        if !Self::stage_request(pool, conn_id, reply, req) {
            return DispatchOutcome::Throttled {
                throttle: Throttle {
                    request: req.clone(),
                    conn: conn_id,
                },
            };
        }
        DispatchOutcome::Enqueued { conn: conn_id }
    }

    fn stage_request<'d, I, S, E>(
        mut pool: Pin<&mut Connector<0, Session<I>, S, E>>,
        conn_id: Token,
        reply: &mut impl Registrable<'d, RowItem>,
        req: &Request,
    ) -> bool
    where
        I: QuerySet,
        S: Dialer<E::Transport>,
        E: Env,
        E::Transport: Transport<Addr: Clone>,
    {
        let Some(channel) = pool.as_mut().state_for(conn_id) else {
            return false;
        };
        let n = {
            let mut stage = channel.wire_stage();
            if !Emit::frame_request(&mut stage, req) {
                return false;
            }
            stage.len()
        };
        let action = match req.extra {
            Extra::Plain | Extra::CopyIn { .. } => BoundaryAction::Close,
            Extra::CopyInOpen => BoundaryAction::External,
        };
        Self::commit_staged(pool, conn_id, reply, n, action);
        true
    }

    fn commit_staged<'d, I, S, E>(
        mut pool: Pin<&mut Connector<0, Session<I>, S, E>>,
        conn_id: Token,
        reply: &mut impl Registrable<'d, RowItem>,
        n: usize,
        action: BoundaryAction,
    ) where
        I: QuerySet,
        S: Dialer<E::Transport>,
        E: Env,
        E::Transport: Transport<Addr: Clone>,
    {
        let channel = pool
            .as_mut()
            .state_for(conn_id)
            .expect("channel vanished mid-dispatch");
        channel.wire_commit(n);
        reply.attach(&mut channel.conn_state_mut().responses);
        {
            let st = channel.conn_state_mut();
            st.unsynced += 1;
            match action {
                BoundaryAction::Close => st.push_batch_boundary(),
                BoundaryAction::Open => st.batch_open = true,
                BoundaryAction::External => {}
            }
        }
        let s = &mut pool.as_mut().session_mut().shared;
        s.inflight_total.inc();
        s.inc_inflight(conn_id);
        pool.request_flush(conn_id);
    }

    fn retry_connecting<'d, I, S, E>(
        holding: PgHolding<'d, I, S, E>,
        pin: Option<Token>,
        cx: &mut Context<'_>,
        reply: &mut impl Registrable<'d, RowItem>,
        req: &Request,
    ) -> Poll<DispatchOutcome>
    where
        I: QuerySet,
        S: Dialer<E::Transport>,
        E: Env,
        E::Transport: Transport<Addr: Clone>,
    {
        let outcome = Self::try_dispatch_reply(holding, pin, reply, req);
        if matches!(outcome, DispatchOutcome::NoConn { .. }) {
            let mut h = holding.hold();
            let s = &mut h.as_mut().session_mut().shared;
            if s.is_failed() {
                return Poll::Ready(DispatchOutcome::Failed(Error::Closed));
            }
            s.register_ready_waker(WakeRef::verified(cx.waker()));
            return Poll::Pending;
        }
        Poll::Ready(outcome)
    }

    fn retry_throttled<'d, I, S, E>(
        holding: PgHolding<'d, I, S, E>,
        cx: &mut Context<'_>,
        reply: &mut impl Registrable<'d, RowItem>,
        throttle: &Throttle,
    ) -> Poll<Result<(), Error>>
    where
        I: QuerySet,
        S: Dialer<E::Transport>,
        E: Env,
        E::Transport: Transport<Addr: Clone>,
    {
        if holding.session().shared.is_failed() {
            return Poll::Ready(Err(Error::Closed));
        }
        let mut h = holding.hold();
        let mut pool = h.as_mut();
        if pool.as_mut().state_for(throttle.conn).is_none() {
            return Poll::Ready(Err(Error::Closed));
        }
        if Self::stage_request(pool.as_mut(), throttle.conn, reply, &throttle.request) {
            return Poll::Ready(Ok(()));
        }
        pool.as_mut().request_flush(throttle.conn);
        pool.session_mut()
            .shared
            .register_egress_drain_waker(WakeRef::verified(cx.waker()));
        Poll::Pending
    }

    fn dispatch_copy_data<'d, I, S, E>(
        holding: PgHolding<'d, I, S, E>,
        pin: Token,
        data: &[u8],
    ) -> Result<(), Error>
    where
        I: QuerySet,
        S: Dialer<E::Transport>,
        E: Env,
        E::Transport: Transport<Addr: Clone>,
    {
        Self::check_can_dispatch(holding)?;
        let mut h = holding.hold();
        let mut pool = h.as_mut();
        let channel = pool.as_mut().state_for(pin).ok_or(Error::Closed)?;
        let queued = channel.egress_len();
        let staged = {
            let mut stage = channel.wire_stage();
            encode::copy_data(&mut stage, data);
            (!stage.overflowed()).then_some(stage.len())
        };
        let Some(n) = staged else {
            return Err(pool.session().shared.backpressure(queued));
        };
        let channel = pool
            .as_mut()
            .state_for(pin)
            .expect("channel vanished mid-dispatch");
        channel.wire_commit(n);
        pool.request_flush(pin);
        Ok(())
    }

    fn dispatch_copy_finish<'d, I, S, E>(
        holding: PgHolding<'d, I, S, E>,
        pin: Token,
    ) -> Result<(), Error>
    where
        I: QuerySet,
        S: Dialer<E::Transport>,
        E: Env,
        E::Transport: Transport<Addr: Clone>,
    {
        Self::check_can_dispatch(holding)?;
        let mut h = holding.hold();
        let mut pool = h.as_mut();
        let channel = pool.as_mut().state_for(pin).ok_or(Error::Closed)?;
        let queued = channel.egress_len();
        let staged = {
            let mut stage = channel.wire_stage();
            encode::copy_done(&mut stage);
            encode::sync(&mut stage);
            (!stage.overflowed()).then_some(stage.len())
        };
        let Some(n) = staged else {
            return Err(pool.session().shared.backpressure(queued));
        };
        let channel = pool
            .as_mut()
            .state_for(pin)
            .expect("channel vanished mid-dispatch");
        channel.wire_commit(n);
        channel.conn_state_mut().push_batch_boundary();
        pool.request_flush(pin);
        Ok(())
    }

    fn check_can_dispatch<'d, I, S, E>(holding: PgHolding<'d, I, S, E>) -> Result<(), Error>
    where
        I: QuerySet,
        S: Dialer<E::Transport>,
        E: Env,
        E::Transport: Transport<Addr: Clone>,
    {
        let s = &holding.session().shared;
        if s.is_failed() {
            return Err(Error::Closed);
        }
        s.inflight_total.check().map_err(Error::from)
    }
}

enum DispatchState<'d, I, S, E>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    Pending {
        conn: Token,
    },
    Throttled {
        conn: PgHolding<'d, I, S, E>,
        throttle: Throttle,
    },
    Connecting {
        conn: PgHolding<'d, I, S, E>,
        pin: Option<Token>,
        request: Request,
    },
    Failed(Option<Error>),
}

pub struct Dispatched<
    'd,
    I,
    S = Static<Tcp>,
    E = Bundle<Tcp, Identity, Production>,
    X = ExtractUnit,
> where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
    X: Extract<RowItem>,
{
    reply: Reply<'d, RowItem, X>,
    state: DispatchState<'d, I, S, E>,
}

impl<'d, I, S, E, X> Dispatched<'d, I, S, E, X>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
    X: Extract<RowItem>,
{
    pub(super) fn resolved_conn(&self) -> Option<Token> {
        match &self.state {
            DispatchState::Pending { conn } => Some(*conn),
            DispatchState::Throttled { throttle, .. } => Some(throttle.conn),
            DispatchState::Connecting { .. } | DispatchState::Failed(_) => None,
        }
    }
}

impl<'d, I, S, E, T, X> Future for Dispatched<'d, I, S, E, X>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
    X: Extract<RowItem, Output = Result<T, Error>>,
{
    type Output = Result<T, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = self.get_mut();
        if let DispatchState::Connecting { conn, pin, request } = &mut me.state {
            match Disp::retry_connecting(*conn, *pin, cx, &mut me.reply, request) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(DispatchOutcome::Enqueued { conn: target }) => {
                    me.state = DispatchState::Pending { conn: target };
                }
                Poll::Ready(DispatchOutcome::Throttled { throttle }) => {
                    me.state = DispatchState::Throttled {
                        conn: *conn,
                        throttle,
                    };
                }
                Poll::Ready(DispatchOutcome::NoConn { .. }) => {
                    unreachable!("retry_connecting maps NoConn to Pending/Failed")
                }
                Poll::Ready(DispatchOutcome::Failed(e)) => {
                    return Poll::Ready(Err(e));
                }
            }
        }
        if let DispatchState::Throttled { conn, throttle, .. } = &mut me.state {
            match Disp::retry_throttled(*conn, cx, &mut me.reply, throttle) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => {
                    let target = throttle.conn;
                    me.state = DispatchState::Pending { conn: target };
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            }
        }
        match &mut me.state {
            DispatchState::Connecting { .. } => unreachable!("Connecting resolved above"),
            DispatchState::Throttled { .. } => unreachable!("Throttled resolved above"),
            DispatchState::Failed(e) => Poll::Ready(Err(e
                .take()
                .expect("dispatch future polled after failure delivered"))),
            DispatchState::Pending { .. } => Pin::new(&mut me.reply).poll(cx),
        }
    }
}

pub(super) struct Request {
    sql: String,
    param_oids: &'static [u32],
    n_params: u16,
    param_formats: &'static [u16],
    result_formats: &'static [u16],
    param_buf: Owned,
    extra: Extra,
}

impl Clone for Request {
    fn clone(&self) -> Self {
        Self {
            sql: self.sql.clone(),
            param_oids: self.param_oids,
            n_params: self.n_params,
            param_formats: self.param_formats,
            result_formats: self.result_formats,
            param_buf: self.param_buf.clone(),
            extra: self.extra.clone(),
        }
    }
}

impl Request {
    fn typed<Q: TypedQuery>(params: Q::Params<'_>) -> Self {
        let mut param_buf = Owned::with_capacity(64);
        {
            let mut bw = BindWriter::new(&mut param_buf);
            Q::encode_params(params, &mut bw);
        }
        Self {
            sql: Q::SQL.to_string(),
            param_oids: Q::PARAM_OIDS,
            n_params: Q::N_PARAMS,
            param_formats: Q::PARAM_FORMAT_CODES,
            result_formats: Q::RESULT_FORMAT_CODES,
            param_buf,
            extra: Extra::Plain,
        }
    }

    pub(super) fn raw(sql: &str) -> Self {
        Self::raw_extra(sql, Extra::Plain)
    }

    pub(super) fn raw_extra(sql: &str, extra: Extra) -> Self {
        Self {
            sql: sql.to_string(),
            param_oids: &[],
            n_params: 0,
            param_formats: &[1],
            result_formats: &[1],
            param_buf: Owned::with_capacity(0),
            extra,
        }
    }
}

pub(super) enum Extra {
    Plain,
    CopyIn { data: Owned },
    CopyInOpen,
}

impl Clone for Extra {
    fn clone(&self) -> Self {
        match self {
            Extra::Plain => Extra::Plain,
            Extra::CopyIn { data } => Extra::CopyIn { data: data.clone() },
            Extra::CopyInOpen => Extra::CopyInOpen,
        }
    }
}

pub struct RunStream<'d, I, S = Static<Tcp>, E = Bundle<Tcp, Identity, Production>, R = ()>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
    R: 'static,
{
    state: DispatchedStream<'d, I, S, E, ExtractOne>,
    decoder: Decoder<R>,
}

impl<'d, I, S, E, R> RunStream<'d, I, S, E, R>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
    R: 'static,
{
    pub fn next_row(&mut self) -> Fiber<'d, impl Future<Output = Result<Option<R>, Error>> + '_> {
        Fiber::new(async move {
            let decoder = self.decoder;
            std::future::poll_fn(|cx| self.state.poll_settle(cx)).await;
            match &mut self.state {
                DispatchedStream::Throttled { .. } => unreachable!("poll_settle drains Throttled"),
                DispatchedStream::Connecting { .. } => {
                    unreachable!("poll_settle drains Connecting")
                }
                DispatchedStream::Failed(e) => Err(e
                    .take()
                    .expect("stream future polled after failure delivered")),
                DispatchedStream::Pending { reply } => {
                    let item = std::future::poll_fn(|cx| Pin::new(&mut *reply).poll_next(cx)).await;
                    match item {
                        None => Ok(None),
                        Some(Ok(payload)) => decode_row(decoder, &payload).map(Some),
                        Some(Err(e)) => Err(e),
                    }
                }
            }
        })
    }
}

pub struct CopyInGuard<'d, I, S, E>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    conn: PgHolding<'d, I, S, E>,
    pin: Token,
    reply: Option<Reply<'d, RowItem, ExtractUnit>>,
}

impl<'d, I, S, E> CopyInGuard<'d, I, S, E>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    pub fn write(&mut self, chunk: &[u8]) -> Result<(), Error> {
        Disp::dispatch_copy_data(self.conn, self.pin, chunk)
    }

    pub fn finish(mut self) -> Fiber<'d, Dispatched<'d, I, S, E, ExtractUnit>> {
        let reply = self.reply.take().expect("CopyInGuard polled twice");
        let state = match Disp::dispatch_copy_finish(self.conn, self.pin) {
            Ok(()) => DispatchState::Pending { conn: self.pin },
            Err(e) => DispatchState::Failed(Some(e)),
        };
        Fiber::new(Dispatched { reply, state })
    }
}

pub struct CopyOutStream<'d, I, S = Static<Tcp>, E = Bundle<Tcp, Identity, Production>>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    state: DispatchedStream<'d, I, S, E, ExtractOne>,
}

impl<'d, I, S, E> CopyOutStream<'d, I, S, E>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    pub fn next_chunk(
        &mut self,
    ) -> Fiber<'d, impl Future<Output = Result<Option<Vec<u8>>, Error>> + '_> {
        Fiber::new(async move {
            std::future::poll_fn(|cx| self.state.poll_settle(cx)).await;
            match &mut self.state {
                DispatchedStream::Throttled { .. } => unreachable!("poll_settle drains Throttled"),
                DispatchedStream::Connecting { .. } => {
                    unreachable!("poll_settle drains Connecting")
                }
                DispatchedStream::Failed(e) => Err(e
                    .take()
                    .expect("copy_out future polled after failure delivered")),
                DispatchedStream::Pending { reply } => {
                    let item = std::future::poll_fn(|cx| Pin::new(&mut *reply).poll_next(cx)).await;
                    match item {
                        None => Ok(None),
                        Some(Ok(payload)) => Ok(Some(payload.to_vec())),
                        Some(Err(e)) => Err(e),
                    }
                }
            }
        })
    }
}

pub struct NextNotification<'d, I, S, E>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    conn: PgHolding<'d, I, S, E>,
}

impl<'d, I, S, E> Future for NextNotification<'d, I, S, E>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    type Output = Result<crate::Notification, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut h = self.conn.hold();
        let s = &mut h.as_mut().session_mut().shared;
        if let Some(n) = s.pop_notification() {
            return Poll::Ready(Ok(n));
        }
        if s.is_failed() {
            return Poll::Ready(Err(Error::Closed));
        }
        s.register_notification_waker(WakeRef::verified(cx.waker()));
        Poll::Pending
    }
}
