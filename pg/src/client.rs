use std::marker::PhantomData;
use std::pin::Pin;
use std::task::Poll;

use cartel_core::{Extract, Registrable, Reply, ReplyStream, Slot};
use dope::driver::token::Token;
use dope_fiber::{Context, Fiber, Waiter};
use o3::buffer::Shared;
use pin_project::pin_project;

use crate::port::{Boundary, Frame, Port};
use crate::protocol::RowItem;
use crate::query::{HasGroup, QuerySet, TypedQuery};
use crate::value::{BindWriter, RowReader};
use crate::{Error, encode, protocol};

pub struct Client<'d, I: QuerySet> {
    pub(super) port: &'d Port<'d, I>,
}

impl<I: QuerySet> Copy for Client<'_, I> {}

impl<I: QuerySet> Clone for Client<'_, I> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'d, I: QuerySet> Port<'d, I> {
    pub fn client(&'d self) -> Client<'d, I> {
        Client { port: self }
    }
}

type Decoder<R> = fn(&mut RowReader<'_>) -> Result<R, Error>;

fn decode_row<R>(decoder: Decoder<R>, payload: &Shared) -> Result<R, Error> {
    if payload.len() < 2 {
        return Err(Error::Protocol("data row payload truncated"));
    }
    let mut reader = RowReader::new(payload);
    decoder(&mut reader)
}

fn overflow_error() -> Error {
    Error::ResponseCapacity
}

fn no_conn_outcome<'d>(shared: &protocol::Shared, request: Pending<'d>) -> DispatchOutcome<'d> {
    if shared.tx_saturated() {
        DispatchOutcome::Failed(shared.backpressure(0))
    } else {
        DispatchOutcome::NoConn { request }
    }
}

pub(super) struct ExtractAll<Q>(PhantomData<fn() -> Q>);

unsafe impl<Q: TypedQuery> Extract<RowItem> for ExtractAll<Q> {
    type Output = Result<Vec<Q::Row>, Error>;

    fn extract(slot: &mut Slot<'_, RowItem>) -> Option<Self::Output> {
        if !slot.completed() {
            return None;
        }
        if slot.overflowed() {
            return Some(Err(overflow_error()));
        }
        let mut rows = Vec::with_capacity(slot.len());
        while let Some(item) = slot.pop() {
            match item {
                Ok(payload) => match decode_row(Q::decode_row, &payload) {
                    Ok(row) => rows.push(row),
                    Err(error) => return Some(Err(error)),
                },
                Err(e) => return Some(Err(e)),
            }
        }
        Some(Ok(rows))
    }
}

pub struct ExtractUnit;

unsafe impl Extract<RowItem> for ExtractUnit {
    type Output = Result<(), Error>;

    fn extract(slot: &mut Slot<'_, RowItem>) -> Option<Self::Output> {
        if !slot.completed() {
            return None;
        }
        if slot.overflowed() {
            return Some(Err(overflow_error()));
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

unsafe impl Extract<RowItem> for ExtractOne {
    type Output = Result<Shared, Error>;
    const SYNC_AFTER: bool = true;

    fn extract(slot: &mut Slot<'_, RowItem>) -> Option<Self::Output> {
        match slot.pop() {
            Some(item) => Some(item),
            None if slot.take_overflow() => Some(Err(overflow_error())),
            None => None,
        }
    }
}

pub(super) struct ExtractFirst;

unsafe impl Extract<RowItem> for ExtractFirst {
    type Output = Result<Option<Shared>, Error>;

    fn extract(slot: &mut Slot<'_, RowItem>) -> Option<Self::Output> {
        if !slot.completed() {
            return None;
        }
        if slot.overflowed() {
            return Some(Err(overflow_error()));
        }
        match slot.pop() {
            Some(Ok(payload)) => Some(Ok(Some(payload))),
            Some(Err(e)) => Some(Err(e)),
            None => Some(Ok(None)),
        }
    }
}

pub(super) struct Throttle<'d> {
    request: Option<Pending<'d>>,
    conn: Token,
}

struct Emit;

impl Emit {
    fn typed<Q: TypedQuery>(out: &mut Frame<'_>, params: Q::Params<'_>, sync: bool) {
        let pos = encode::bind_header(
            out,
            "",
            Q::STATEMENT_NAME,
            Q::PARAM_FORMAT_CODES,
            Q::N_PARAMS,
        );
        Q::encode_params(params, &mut BindWriter::new(out));
        encode::bind_trailer(out, pos, Q::RESULT_FORMAT_CODES);
        encode::execute(out);
        if sync {
            encode::sync(out);
        }
    }

    fn raw(out: &mut Frame<'_>, req: Request<'_>) {
        encode::parse(out, "", req.sql, &[]);
        let pos = encode::bind_header(out, "", "", &[1], 0);
        encode::bind_trailer(out, pos, &[1]);
        encode::execute(out);
        match req.extra {
            Extra::Plain => {
                encode::sync(out);
            }
            Extra::CopyIn { data } => {
                encode::copy_data(out, data);
                encode::copy_done(out);
                encode::sync(out);
            }
            Extra::CopyInOpen => {}
        }
    }
}

pub(super) struct Pending<'d> {
    frame: Frame<'d>,
    boundary: Boundary,
}

pub(super) struct TransactionLease<'d, I>
where
    I: QuerySet + 'd,
{
    client: Client<'d, I>,
    target: (Token, u64),
    armed: bool,
}

impl<'d, I: QuerySet + 'd> TransactionLease<'d, I> {
    fn acquire(client: Client<'d, I>, conn: Token) -> Option<Self> {
        let target = client.port.shared.try_acquire_transaction(conn)?;
        Some(Self {
            client,
            target,
            armed: true,
        })
    }

    pub(super) fn client(&self) -> Client<'d, I> {
        self.client
    }

    pub(super) fn target(&self) -> (Token, u64) {
        self.target
    }

    pub(super) fn transfer(&mut self) -> bool {
        if !self.armed {
            return false;
        }
        if !self
            .client
            .port
            .shared
            .begin_transaction_finalization(self.target)
        {
            return false;
        }
        self.armed = false;
        true
    }
}

impl<I: QuerySet> Drop for TransactionLease<'_, I> {
    fn drop(&mut self) {
        if self.armed {
            Disp::quarantine_transaction(self.client, self.target);
        }
    }
}

enum DispatchOutcome<'d> {
    Enqueued { conn: Token },
    Throttled { throttle: Throttle<'d> },
    NoConn { request: Pending<'d> },
    Failed(Error),
}

enum TransactionDispatchOutcome<'d, I>
where
    I: QuerySet + 'd,
{
    Enqueued {
        lease: TransactionLease<'d, I>,
    },
    Throttled {
        lease: TransactionLease<'d, I>,
        throttle: Throttle<'d>,
    },
    NoConn {
        request: Pending<'d>,
    },
    Failed(Error),
}

pub(super) enum DispatchedStream<'d, I, X>
where
    I: QuerySet + 'd,
    X: Extract<RowItem>,
{
    Pending {
        reply: ReplyStream<'d, RowItem, X>,
    },
    Throttled {
        client: Client<'d, I>,
        reply: ReplyStream<'d, RowItem, X>,
        throttle: Throttle<'d>,
    },
    Connecting {
        client: Client<'d, I>,
        target: Option<(Token, u64)>,
        reply: ReplyStream<'d, RowItem, X>,
        request: Option<Pending<'d>>,
    },
    Failed(Option<Error>),
}

impl<'d, I, X> DispatchedStream<'d, I, X>
where
    I: QuerySet + 'd,
    X: Extract<RowItem>,
{
    fn poll_settle(
        &mut self,
        mut cx: Pin<&mut Context<'_, 'd>>,
        waiter: Pin<&Waiter<'d>>,
    ) -> Poll<()> {
        if let DispatchedStream::Connecting {
            client,
            target,
            reply,
            request,
        } = self
        {
            match Disp::retry_connecting(*client, *target, cx.as_mut(), waiter, reply, request) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(DispatchOutcome::Enqueued { .. }) => {
                    let reply = std::mem::replace(reply, ReplyStream::new());
                    *self = DispatchedStream::Pending { reply };
                }
                Poll::Ready(DispatchOutcome::Throttled { throttle }) => {
                    let reply = std::mem::replace(reply, ReplyStream::new());
                    *self = DispatchedStream::Throttled {
                        client: *client,
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
            client,
            reply,
            throttle,
        } = self
        {
            match Disp::retry_throttled(*client, cx.as_mut(), waiter, reply, throttle) {
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

pub trait PgOps<'d, I>
where
    I: QuerySet + 'd,
{
    fn client(&self) -> Client<'d, I>;

    fn backend_pid(&self) -> Option<i32> {
        None
    }

    fn is_failed(&self) -> bool {
        self.client().port.shared.is_failed()
    }

    fn is_ready(&self) -> bool {
        self.client().port.shared.is_ready()
    }

    fn live_count(&self) -> usize {
        self.client().port.shared.ready_count.get()
    }

    fn notifications_dropped(&self) -> u64 {
        self.client().port.shared.notifications_dropped()
    }

    fn set_pick_policy(&self, policy: protocol::PickPolicy) {
        self.client().port.shared.policy.set(policy);
    }

    fn pick_policy(&self) -> protocol::PickPolicy {
        self.client().port.shared.policy.get()
    }

    fn run_one<Q>(
        &self,
        params: Q::Params<'_>,
    ) -> impl Fiber<'d, Output = Result<Q::Row, Error>> + use<'d, I, Q, Self>
    where
        Q: TypedQuery,
        I: HasGroup<Q::Group>,
        Q::Group: crate::query::QueryGroup,
    {
        let decoder = Q::decode_row;
        let dispatched =
            Disp::dispatch_typed::<Q, ExtractFirst, I>(self.client(), self.target(), params);
        dope_fiber::fiber!('d => async move {
            match dispatched.await? {
                None => Err(Error::NotFound),
                Some(payload) => decode_row(decoder, &payload),
            }
        })
    }

    fn run_first<Q>(
        &self,
        params: Q::Params<'_>,
    ) -> impl Fiber<'d, Output = Result<Option<Q::Row>, Error>> + use<'d, I, Q, Self>
    where
        Q: TypedQuery,
        I: HasGroup<Q::Group>,
        Q::Group: crate::query::QueryGroup,
    {
        let decoder = Q::decode_row;
        let dispatched =
            Disp::dispatch_typed::<Q, ExtractFirst, I>(self.client(), self.target(), params);
        dope_fiber::fiber!('d => async move {
            match dispatched.await? {
                None => Ok(None),
                Some(payload) => decode_row(decoder, &payload).map(Some),
            }
        })
    }

    fn run_all<Q>(
        &self,
        params: Q::Params<'_>,
    ) -> impl Fiber<'d, Output = Result<Vec<Q::Row>, Error>> + use<'d, I, Q, Self>
    where
        Q: TypedQuery,
        I: HasGroup<Q::Group>,
        Q::Group: crate::query::QueryGroup,
    {
        Disp::dispatch_typed::<Q, ExtractAll<Q>, I>(self.client(), self.target(), params)
    }

    fn run_no_rows<Q>(&self, params: Q::Params<'_>) -> Dispatched<'d, I, ExtractUnit>
    where
        Q: TypedQuery<Row = ()>,
        I: HasGroup<Q::Group>,
        Q::Group: crate::query::QueryGroup,
    {
        Disp::dispatch_typed::<Q, ExtractUnit, I>(self.client(), self.target(), params)
    }

    fn run_stream<Q>(&self, params: Q::Params<'_>) -> RunStream<'d, I, Q::Row>
    where
        Q: TypedQuery,
        I: HasGroup<Q::Group>,
        Q::Group: crate::query::QueryGroup,
    {
        RunStream {
            state: Disp::dispatch_stream::<Q, ExtractOne, I>(self.client(), self.target(), params),
            decoder: Q::decode_row,
        }
    }

    fn copy_in(&self, sql: &str, data: &[u8]) -> Dispatched<'d, I, ExtractUnit> {
        Disp::dispatch_raw::<ExtractUnit, I>(
            self.client(),
            self.target(),
            Request::raw_extra(sql, Extra::CopyIn { data }),
        )
    }

    fn copy_in_stream(&self, sql: &str) -> Result<CopyInGuard<'d, I>, Error> {
        let client = self.client();
        let target = self.target();
        let req = Request::raw_extra(sql, Extra::CopyInOpen);
        let mut reply = Reply::<RowItem, ExtractUnit>::new();
        let outcome = match Disp::raw(client, req) {
            Ok(request) => Disp::try_dispatch_reply(client, target, &mut reply, request),
            Err(error) => DispatchOutcome::Failed(error),
        };
        match outcome {
            DispatchOutcome::Enqueued { conn } => Ok(CopyInGuard {
                client,
                target: target.unwrap_or((conn, 0)),
                reply,
            }),
            DispatchOutcome::Throttled { .. } => Err(client.port.shared.backpressure(0)),
            DispatchOutcome::NoConn { .. } => Err(Error::NoReadyConn),
            DispatchOutcome::Failed(e) => Err(e),
        }
    }

    fn copy_out(&self, sql: &str) -> CopyOutStream<'d, I> {
        CopyOutStream {
            state: Disp::dispatch_stream_raw::<ExtractOne, I>(
                self.client(),
                self.target(),
                Request::raw(sql),
            ),
        }
    }

    fn dispatch_sql(&self, sql: &str) -> Dispatched<'d, I, ExtractUnit> {
        Disp::dispatch_raw::<ExtractUnit, I>(self.client(), self.target(), Request::raw(sql))
    }

    fn next_notification(&self) -> NextNotification<'d, I> {
        NextNotification {
            client: self.client(),
            waiter: Waiter::new(),
        }
    }

    fn listen(
        &self,
        channel: impl Into<String>,
    ) -> impl Fiber<'d, Output = Result<crate::tx::ListenGuard<'d, I>, Error>> {
        let ch = channel.into();
        let sql = format!("LISTEN \"{}\"", ch.replace('"', "\"\""));
        let client = self.client();
        let target = self.target();
        let dispatched = Disp::dispatch_raw::<ExtractUnit, I>(client, target, Request::raw(&sql));
        dope_fiber::fiber!('d => async move {
            let conn = dispatched.resolved_conn();
            dispatched.await?;
            let pin = conn.ok_or_else(|| Error::Other("listen lost target conn".into()))?;
            Ok(crate::tx::ListenGuard::from_parts(
                client,
                target.unwrap_or((pin, 0)),
                ch,
            ))
        })
    }

    fn target(&self) -> Option<(Token, u64)> {
        None
    }

    fn batch_pin(&self) -> Option<(Token, u64)> {
        self.target().or_else(|| {
            self.client()
                .port
                .shared
                .pick_conn(None)
                .map(|conn| (conn, 0))
        })
    }
}

impl<'d, I: QuerySet + 'd> PgOps<'d, I> for Client<'d, I> {
    fn client(&self) -> Client<'d, I> {
        *self
    }
}

pub struct Runner<'d, I>
where
    I: QuerySet + 'd,
{
    client: Client<'d, I>,
    target: Option<(Token, u64)>,
}

impl<'d, I: QuerySet + 'd> Runner<'d, I> {
    pub fn new(client: Client<'d, I>, target: Option<(Token, u64)>) -> Self {
        Self { client, target }
    }
}

impl<I: QuerySet> Clone for Runner<'_, I> {
    fn clone(&self) -> Self {
        Self {
            client: self.client,
            target: self.target,
        }
    }
}

impl<'d, I: QuerySet + 'd> PgOps<'d, I> for Runner<'d, I> {
    fn client(&self) -> Client<'d, I> {
        self.client
    }

    fn target(&self) -> Option<(Token, u64)> {
        self.target
    }
}

pub(super) struct Disp;

impl Disp {
    pub(super) fn dispatch_typed<'d, Q, X, I>(
        client: Client<'d, I>,
        target: Option<(Token, u64)>,
        params: Q::Params<'_>,
    ) -> Dispatched<'d, I, X>
    where
        Q: TypedQuery,
        X: Extract<RowItem>,
        I: QuerySet + HasGroup<Q::Group>,
        Q::Group: crate::query::QueryGroup,
    {
        let mut reply = Reply::<RowItem, X>::new();
        let outcome = match Self::typed::<Q, I>(client, params, X::SYNC_AFTER) {
            Ok(request) => Self::try_dispatch_reply(client, target, &mut reply, request),
            Err(error) => DispatchOutcome::Failed(error),
        };
        Self::reply_state(client, target, reply, outcome)
    }

    pub(super) fn dispatch_raw<'d, X, I>(
        client: Client<'d, I>,
        target: Option<(Token, u64)>,
        req: Request<'_>,
    ) -> Dispatched<'d, I, X>
    where
        X: Extract<RowItem>,
        I: QuerySet,
    {
        let mut reply = Reply::<RowItem, X>::new();
        let outcome = match Self::raw(client, req) {
            Ok(request) => Self::try_dispatch_reply(client, target, &mut reply, request),
            Err(error) => DispatchOutcome::Failed(error),
        };
        Self::reply_state(client, target, reply, outcome)
    }

    pub(super) fn dispatch_transaction<'d, I>(
        client: Client<'d, I>,
        req: Request<'_>,
    ) -> TransactionDispatched<'d, I>
    where
        I: QuerySet,
    {
        let mut reply = Reply::<RowItem, ExtractUnit>::new();
        let outcome = match Self::raw(client, req) {
            Ok(request) => Self::try_dispatch_transaction(client, &mut reply, request),
            Err(error) => TransactionDispatchOutcome::Failed(error),
        };
        TransactionDispatched::new(client, reply, outcome)
    }

    fn reply_state<'d, I, X>(
        client: Client<'d, I>,
        target: Option<(Token, u64)>,
        reply: Reply<'d, RowItem, X>,
        outcome: DispatchOutcome<'d>,
    ) -> Dispatched<'d, I, X>
    where
        X: Extract<RowItem>,
        I: QuerySet,
    {
        let state = match outcome {
            DispatchOutcome::Enqueued { conn } => DispatchState::Pending { conn },
            DispatchOutcome::Throttled { throttle } => {
                DispatchState::Throttled { client, throttle }
            }
            DispatchOutcome::NoConn { request } => DispatchState::Connecting {
                client,
                target,
                request: Some(request),
            },
            DispatchOutcome::Failed(e) => DispatchState::Failed(Some(e)),
        };
        Dispatched::new(reply, state)
    }

    pub(super) fn dispatch_stream<'d, Q, X, I>(
        client: Client<'d, I>,
        target: Option<(Token, u64)>,
        params: Q::Params<'_>,
    ) -> DispatchedStream<'d, I, X>
    where
        Q: TypedQuery,
        X: Extract<RowItem>,
        I: QuerySet + HasGroup<Q::Group>,
        Q::Group: crate::query::QueryGroup,
    {
        let mut reply = ReplyStream::<RowItem, X>::new();
        let outcome = match Self::typed::<Q, I>(client, params, X::SYNC_AFTER) {
            Ok(request) => Self::try_dispatch_reply(client, target, &mut reply, request),
            Err(error) => DispatchOutcome::Failed(error),
        };
        Self::stream_state(client, target, reply, outcome)
    }

    pub(super) fn dispatch_stream_raw<'d, X, I>(
        client: Client<'d, I>,
        target: Option<(Token, u64)>,
        req: Request<'_>,
    ) -> DispatchedStream<'d, I, X>
    where
        X: Extract<RowItem>,
        I: QuerySet,
    {
        let mut reply = ReplyStream::<RowItem, X>::new();
        let outcome = match Self::raw(client, req) {
            Ok(request) => Self::try_dispatch_reply(client, target, &mut reply, request),
            Err(error) => DispatchOutcome::Failed(error),
        };
        Self::stream_state(client, target, reply, outcome)
    }

    fn stream_state<'d, I, X>(
        client: Client<'d, I>,
        target: Option<(Token, u64)>,
        reply: ReplyStream<'d, RowItem, X>,
        outcome: DispatchOutcome<'d>,
    ) -> DispatchedStream<'d, I, X>
    where
        X: Extract<RowItem>,
        I: QuerySet,
    {
        match outcome {
            DispatchOutcome::Enqueued { .. } => DispatchedStream::Pending { reply },
            DispatchOutcome::Throttled { throttle } => DispatchedStream::Throttled {
                client,
                reply,
                throttle,
            },
            DispatchOutcome::NoConn { request } => DispatchedStream::Connecting {
                client,
                target,
                reply,
                request: Some(request),
            },
            DispatchOutcome::Failed(e) => DispatchedStream::Failed(Some(e)),
        }
    }

    fn typed<'d, Q, I>(
        client: Client<'d, I>,
        params: Q::Params<'_>,
        sync: bool,
    ) -> Result<Pending<'d>, Error>
    where
        Q: TypedQuery,
        I: QuerySet,
    {
        let frame = client
            .port
            .encode(|frame| Emit::typed::<Q>(frame, params, sync))?;
        Ok(Pending {
            frame,
            boundary: if sync {
                Boundary::Close
            } else {
                Boundary::Open
            },
        })
    }

    fn raw<'d, I>(client: Client<'d, I>, req: Request<'_>) -> Result<Pending<'d>, Error>
    where
        I: QuerySet,
    {
        let boundary = match req.extra {
            Extra::Plain | Extra::CopyIn { .. } => Boundary::Close,
            Extra::CopyInOpen => Boundary::External,
        };
        let frame = client.port.encode(|frame| Emit::raw(frame, req))?;
        Ok(Pending { frame, boundary })
    }

    fn try_dispatch_reply<'d, I>(
        client: Client<'d, I>,
        target: Option<(Token, u64)>,
        reply: &mut impl Registrable<'d, RowItem>,
        request: Pending<'d>,
    ) -> DispatchOutcome<'d>
    where
        I: QuerySet,
    {
        if let Err(e) = Self::check_can_dispatch(client) {
            return DispatchOutcome::Failed(e);
        }
        let shared = &client.port.shared;
        let conn_id = match shared.pick_conn(target) {
            Some(c) => c,
            None => {
                return match target {
                    Some(_) => {
                        DispatchOutcome::Failed(Error::Other("pinned conn no longer ready".into()))
                    }
                    None => no_conn_outcome(shared, request),
                };
            }
        };
        match Self::stage_request(client, conn_id, reply, request) {
            Ok(()) => DispatchOutcome::Enqueued { conn: conn_id },
            Err((Error::Backpressure { .. }, request)) => DispatchOutcome::Throttled {
                throttle: Throttle {
                    request: Some(request),
                    conn: conn_id,
                },
            },
            Err((error, _)) => DispatchOutcome::Failed(error),
        }
    }

    fn try_dispatch_transaction<'d, I>(
        client: Client<'d, I>,
        reply: &mut impl Registrable<'d, RowItem>,
        request: Pending<'d>,
    ) -> TransactionDispatchOutcome<'d, I>
    where
        I: QuerySet,
    {
        if let Err(error) = Self::check_can_dispatch(client) {
            return TransactionDispatchOutcome::Failed(error);
        }
        let shared = &client.port.shared;
        let Some(conn) = shared.pick_conn(None) else {
            return if shared.tx_saturated() {
                TransactionDispatchOutcome::Failed(shared.backpressure(0))
            } else {
                TransactionDispatchOutcome::NoConn { request }
            };
        };
        let Some(lease) = TransactionLease::acquire(client, conn) else {
            return TransactionDispatchOutcome::NoConn { request };
        };
        match Self::stage_request(client, conn, reply, request) {
            Ok(()) => TransactionDispatchOutcome::Enqueued { lease },
            Err((Error::Backpressure { .. }, request)) => TransactionDispatchOutcome::Throttled {
                lease,
                throttle: Throttle {
                    request: Some(request),
                    conn,
                },
            },
            Err((error, _)) => TransactionDispatchOutcome::Failed(error),
        }
    }

    fn stage_request<'d, I>(
        client: Client<'d, I>,
        conn_id: Token,
        reply: &mut impl Registrable<'d, RowItem>,
        request: Pending<'d>,
    ) -> Result<(), (Error, Pending<'d>)>
    where
        I: QuerySet,
    {
        let Pending { frame, boundary } = request;
        match client
            .port
            .try_enqueue_reply(conn_id, frame, reply, boundary)
        {
            Ok(()) => Ok(()),
            Err((error, frame)) => Err((error, Pending { frame, boundary })),
        }
    }

    fn retry_connecting<'d, I>(
        client: Client<'d, I>,
        target: Option<(Token, u64)>,
        cx: Pin<&mut Context<'_, 'd>>,
        waiter: Pin<&Waiter<'d>>,
        reply: &mut impl Registrable<'d, RowItem>,
        request: &mut Option<Pending<'d>>,
    ) -> Poll<DispatchOutcome<'d>>
    where
        I: QuerySet,
    {
        let outcome = Self::try_dispatch_reply(
            client,
            target,
            reply,
            request.take().expect("missing pending request"),
        );
        if let DispatchOutcome::NoConn { request: pending } = outcome {
            *request = Some(pending);
            let shared = &client.port.shared;
            if shared.is_failed() {
                waiter.unregister();
                return Poll::Ready(DispatchOutcome::Failed(Error::Closed));
            }
            return if shared.try_register_ready(waiter, cx.as_ref()) {
                Poll::Pending
            } else {
                Poll::Ready(DispatchOutcome::Failed(Error::WaiterCapacity))
            };
        }
        waiter.unregister();
        Poll::Ready(outcome)
    }

    fn retry_transaction_connecting<'d, I>(
        client: Client<'d, I>,
        cx: Pin<&mut Context<'_, 'd>>,
        waiter: Pin<&Waiter<'d>>,
        reply: &mut impl Registrable<'d, RowItem>,
        request: &mut Option<Pending<'d>>,
    ) -> Poll<TransactionDispatchOutcome<'d, I>>
    where
        I: QuerySet,
    {
        let outcome = Self::try_dispatch_transaction(
            client,
            reply,
            request.take().expect("missing pending transaction request"),
        );
        if let TransactionDispatchOutcome::NoConn { request: pending } = outcome {
            *request = Some(pending);
            let shared = &client.port.shared;
            if shared.is_failed() {
                waiter.unregister();
                return Poll::Ready(TransactionDispatchOutcome::Failed(Error::Closed));
            }
            return if shared.try_register_ready(waiter, cx.as_ref()) {
                Poll::Pending
            } else {
                Poll::Ready(TransactionDispatchOutcome::Failed(Error::WaiterCapacity))
            };
        }
        waiter.unregister();
        Poll::Ready(outcome)
    }

    fn retry_throttled<'d, I>(
        client: Client<'d, I>,
        cx: Pin<&mut Context<'_, 'd>>,
        waiter: Pin<&Waiter<'d>>,
        reply: &mut impl Registrable<'d, RowItem>,
        throttle: &mut Throttle<'d>,
    ) -> Poll<Result<(), Error>>
    where
        I: QuerySet,
    {
        let shared = &client.port.shared;
        if shared.is_failed() {
            waiter.unregister();
            return Poll::Ready(Err(Error::Closed));
        }
        let request = throttle.request.take().expect("missing throttled request");
        match Self::stage_request(client, throttle.conn, reply, request) {
            Ok(()) => {
                waiter.unregister();
                return Poll::Ready(Ok(()));
            }
            Err((Error::Backpressure { .. }, request)) => throttle.request = Some(request),
            Err((error, _)) => {
                waiter.unregister();
                return Poll::Ready(Err(error));
            }
        }
        if shared.try_register_egress(waiter, cx.as_ref()) {
            Poll::Pending
        } else {
            Poll::Ready(Err(Error::WaiterCapacity))
        }
    }

    fn dispatch_copy_data<I: QuerySet>(
        client: Client<'_, I>,
        target: (Token, u64),
        data: &[u8],
    ) -> Result<(), Error> {
        Self::check_can_dispatch(client)?;
        let pin = client
            .port
            .shared
            .pick_conn(Some(target))
            .ok_or(Error::Closed)?;
        let frame = client.port.encode(|frame| encode::copy_data(frame, data))?;
        client
            .port
            .try_enqueue(pin, frame)
            .map_err(|(error, _)| error)
    }

    fn dispatch_copy_finish<I: QuerySet>(
        client: Client<'_, I>,
        target: (Token, u64),
    ) -> Result<(), Error> {
        Self::check_can_dispatch(client)?;
        let pin = client
            .port
            .shared
            .pick_conn(Some(target))
            .ok_or(Error::Closed)?;
        if !client.port.can_push_boundary(pin) {
            return Err(Error::ResponseCapacity);
        }
        let frame = client.port.encode(|frame| {
            encode::copy_done(frame);
            encode::sync(frame);
        })?;
        client
            .port
            .try_enqueue(pin, frame)
            .map_err(|(error, _)| error)?;
        let marked = client.port.push_boundary(pin);
        debug_assert!(marked);
        Ok(())
    }

    pub(super) fn rollback_on_drop<I: QuerySet>(
        client: Client<'_, I>,
        target: (Token, u64),
        sql: &str,
    ) {
        let req = Request::raw(sql);
        let mut reply = Reply::<RowItem, ExtractUnit>::new();
        let outcome = match Self::raw(client, req) {
            Ok(request) => Self::try_dispatch_reply(client, Some(target), &mut reply, request),
            Err(error) => DispatchOutcome::Failed(error),
        };
        if matches!(outcome, DispatchOutcome::Enqueued { .. }) {
            return;
        }
        client.port.close(target.0);
    }

    fn quarantine_transaction<I: QuerySet>(client: Client<'_, I>, target: (Token, u64)) {
        if !client.port.shared.quarantine_transaction(target) {
            return;
        }
        if let Ok(request) = Self::raw(client, Request::raw("ROLLBACK")) {
            let mut reply = Reply::<RowItem, ExtractUnit>::new();
            let _ = Self::stage_request(client, target.0, &mut reply, request);
        }
        client.port.close(target.0);
    }

    fn check_can_dispatch<I: QuerySet>(client: Client<'_, I>) -> Result<(), Error> {
        let s = &client.port.shared;
        if s.is_failed() {
            return Err(Error::Closed);
        }
        s.inflight_total.check().map_err(Error::from)
    }
}

enum DispatchState<'d, I>
where
    I: QuerySet + 'd,
{
    Pending {
        conn: Token,
    },
    Throttled {
        client: Client<'d, I>,
        throttle: Throttle<'d>,
    },
    Connecting {
        client: Client<'d, I>,
        target: Option<(Token, u64)>,
        request: Option<Pending<'d>>,
    },
    Failed(Option<Error>),
}

enum TransactionDispatchState<'d> {
    Pending,
    Throttled(Throttle<'d>),
    Connecting(Option<Pending<'d>>),
    Failed(Option<Error>),
}

#[pin_project]
pub(super) struct TransactionDispatched<'d, I>
where
    I: QuerySet + 'd,
{
    client: Client<'d, I>,
    reply: Reply<'d, RowItem, ExtractUnit>,
    lease: Option<TransactionLease<'d, I>>,
    state: TransactionDispatchState<'d>,
    #[pin]
    waiter: Waiter<'d>,
}

impl<'d, I: QuerySet + 'd> TransactionDispatched<'d, I> {
    fn new(
        client: Client<'d, I>,
        reply: Reply<'d, RowItem, ExtractUnit>,
        outcome: TransactionDispatchOutcome<'d, I>,
    ) -> Self {
        let (lease, state) = match outcome {
            TransactionDispatchOutcome::Enqueued { lease } => {
                (Some(lease), TransactionDispatchState::Pending)
            }
            TransactionDispatchOutcome::Throttled { lease, throttle } => {
                (Some(lease), TransactionDispatchState::Throttled(throttle))
            }
            TransactionDispatchOutcome::NoConn { request } => {
                (None, TransactionDispatchState::Connecting(Some(request)))
            }
            TransactionDispatchOutcome::Failed(error) => {
                (None, TransactionDispatchState::Failed(Some(error)))
            }
        };
        Self {
            client,
            reply,
            lease,
            state,
            waiter: Waiter::new(),
        }
    }
}

impl<'d, I: QuerySet + 'd> Fiber<'d> for TransactionDispatched<'d, I> {
    type Output = Result<TransactionLease<'d, I>, Error>;
    fn poll(self: Pin<&mut Self>, mut cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let me = self.project();
        let client = *me.client;
        let waiter = me.waiter.as_ref();
        if let TransactionDispatchState::Connecting(request) = &mut *me.state {
            match Disp::retry_transaction_connecting(client, cx.as_mut(), waiter, me.reply, request)
            {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(TransactionDispatchOutcome::Enqueued { lease }) => {
                    *me.lease = Some(lease);
                    *me.state = TransactionDispatchState::Pending;
                }
                Poll::Ready(TransactionDispatchOutcome::Throttled { lease, throttle }) => {
                    *me.lease = Some(lease);
                    *me.state = TransactionDispatchState::Throttled(throttle);
                }
                Poll::Ready(TransactionDispatchOutcome::NoConn { .. }) => {
                    unreachable!("transaction retry maps NoConn to Pending/Failed")
                }
                Poll::Ready(TransactionDispatchOutcome::Failed(error)) => {
                    *me.state = TransactionDispatchState::Failed(None);
                    return Poll::Ready(Err(error));
                }
            }
        }
        if let TransactionDispatchState::Throttled(throttle) = &mut *me.state {
            match Disp::retry_throttled(client, cx.as_mut(), waiter, me.reply, throttle) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => *me.state = TransactionDispatchState::Pending,
                Poll::Ready(Err(error)) => {
                    drop(me.lease.take());
                    *me.state = TransactionDispatchState::Failed(None);
                    return Poll::Ready(Err(error));
                }
            }
        }
        match &mut *me.state {
            TransactionDispatchState::Pending => match Fiber::poll(Pin::new(me.reply), cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Ok(())) => {
                    Poll::Ready(Ok(me.lease.take().expect("transaction lease missing")))
                }
                Poll::Ready(Err(error)) => {
                    drop(me.lease.take());
                    Poll::Ready(Err(error))
                }
            },
            TransactionDispatchState::Failed(error) => Poll::Ready(Err(error
                .take()
                .expect("transaction dispatch polled after failure"))),
            TransactionDispatchState::Throttled(_) => {
                unreachable!("transaction throttle resolved above")
            }
            TransactionDispatchState::Connecting(_) => {
                unreachable!("transaction connection resolved above")
            }
        }
    }
}

#[pin_project]
pub struct Dispatched<'d, I, X = ExtractUnit>
where
    I: QuerySet + 'd,
    X: Extract<RowItem>,
{
    reply: Reply<'d, RowItem, X>,
    state: DispatchState<'d, I>,
    #[pin]
    waiter: Waiter<'d>,
}

impl<'d, I, X> Dispatched<'d, I, X>
where
    I: QuerySet + 'd,
    X: Extract<RowItem>,
{
    fn new(reply: Reply<'d, RowItem, X>, state: DispatchState<'d, I>) -> Self {
        Self {
            reply,
            state,
            waiter: Waiter::new(),
        }
    }

    pub(super) fn resolved_conn(&self) -> Option<Token> {
        match &self.state {
            DispatchState::Pending { conn } => Some(*conn),
            DispatchState::Throttled { throttle, .. } => Some(throttle.conn),
            DispatchState::Connecting { .. } | DispatchState::Failed(_) => None,
        }
    }

    pub(super) fn is_enqueued(&self) -> bool {
        matches!(self.state, DispatchState::Pending { .. })
    }
}

impl<'d, I, T, X> Fiber<'d> for Dispatched<'d, I, X>
where
    I: QuerySet + 'd,
    X: Extract<RowItem, Output = Result<T, Error>>,
{
    type Output = Result<T, Error>;
    fn poll(self: Pin<&mut Self>, mut cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let me = self.project();
        let waiter = me.waiter.as_ref();
        if let DispatchState::Connecting {
            client,
            target,
            request,
        } = &mut *me.state
        {
            match Disp::retry_connecting(*client, *target, cx.as_mut(), waiter, me.reply, request) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(DispatchOutcome::Enqueued { conn: target }) => {
                    *me.state = DispatchState::Pending { conn: target };
                }
                Poll::Ready(DispatchOutcome::Throttled { throttle }) => {
                    *me.state = DispatchState::Throttled {
                        client: *client,
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
        if let DispatchState::Throttled {
            client, throttle, ..
        } = &mut *me.state
        {
            match Disp::retry_throttled(*client, cx.as_mut(), waiter, me.reply, throttle) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => {
                    let target = throttle.conn;
                    *me.state = DispatchState::Pending { conn: target };
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            }
        }
        match &mut *me.state {
            DispatchState::Connecting { .. } => unreachable!("Connecting resolved above"),
            DispatchState::Throttled { .. } => unreachable!("Throttled resolved above"),
            DispatchState::Failed(e) => Poll::Ready(Err(e
                .take()
                .expect("dispatch future polled after failure delivered"))),
            DispatchState::Pending { .. } => Fiber::poll(Pin::new(me.reply), cx),
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct Request<'a> {
    sql: &'a str,
    extra: Extra<'a>,
}

impl<'a> Request<'a> {
    pub(super) fn raw(sql: &'a str) -> Self {
        Self::raw_extra(sql, Extra::Plain)
    }

    pub(super) fn raw_extra(sql: &'a str, extra: Extra<'a>) -> Self {
        Self { sql, extra }
    }
}

#[derive(Clone, Copy)]
pub(super) enum Extra<'a> {
    Plain,
    CopyIn { data: &'a [u8] },
    CopyInOpen,
}

pub struct RunStream<'d, I, R = ()>
where
    I: QuerySet + 'd,
    R: 'static,
{
    state: DispatchedStream<'d, I, ExtractOne>,
    decoder: Decoder<R>,
}

impl<'d, I, R> RunStream<'d, I, R>
where
    I: QuerySet + 'd,
    R: 'static,
{
    pub fn next_row(&mut self) -> impl Fiber<'d, Output = Result<Option<R>, Error>> + '_ {
        NextRow {
            stream: self,
            waiter: Waiter::new(),
        }
    }
}

#[pin_project]
struct NextRow<'a, 'd, I, R>
where
    I: QuerySet + 'd,
    R: 'static,
{
    stream: &'a mut RunStream<'d, I, R>,
    #[pin]
    waiter: Waiter<'d>,
}

impl<'d, I, R> Fiber<'d> for NextRow<'_, 'd, I, R>
where
    I: QuerySet + 'd,
    R: 'static,
{
    type Output = Result<Option<R>, Error>;
    fn poll(self: Pin<&mut Self>, mut cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = self.project();
        let waiter = this.waiter.as_ref();
        let stream = &mut **this.stream;
        if stream.state.poll_settle(cx.as_mut(), waiter).is_pending() {
            return Poll::Pending;
        }
        match &mut stream.state {
            DispatchedStream::Throttled { .. } => unreachable!("poll_settle drains Throttled"),
            DispatchedStream::Connecting { .. } => unreachable!("poll_settle drains Connecting"),
            DispatchedStream::Failed(error) => Poll::Ready(Err(error
                .take()
                .expect("stream fiber polled after failure delivered"))),
            DispatchedStream::Pending { reply } => match Pin::new(reply).poll_next(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(None) => Poll::Ready(Ok(None)),
                Poll::Ready(Some(Ok(payload))) => {
                    Poll::Ready(decode_row(stream.decoder, &payload).map(Some))
                }
                Poll::Ready(Some(Err(error))) => Poll::Ready(Err(error)),
            },
        }
    }
}

pub struct CopyInGuard<'d, I>
where
    I: QuerySet + 'd,
{
    client: Client<'d, I>,
    target: (Token, u64),
    reply: Reply<'d, RowItem, ExtractUnit>,
}

impl<'d, I: QuerySet + 'd> CopyInGuard<'d, I> {
    pub fn write(&mut self, chunk: &[u8]) -> Result<(), Error> {
        Disp::dispatch_copy_data(self.client, self.target, chunk)
    }

    pub fn finish(self) -> Dispatched<'d, I, ExtractUnit> {
        let state = match Disp::dispatch_copy_finish(self.client, self.target) {
            Ok(()) => DispatchState::Pending {
                conn: self.target.0,
            },
            Err(e) => DispatchState::Failed(Some(e)),
        };
        Dispatched::new(self.reply, state)
    }
}

pub struct CopyOutStream<'d, I>
where
    I: QuerySet + 'd,
{
    state: DispatchedStream<'d, I, ExtractOne>,
}

impl<'d, I: QuerySet + 'd> CopyOutStream<'d, I> {
    pub fn next_chunk(&mut self) -> impl Fiber<'d, Output = Result<Option<Vec<u8>>, Error>> + '_ {
        NextChunk {
            stream: self,
            waiter: Waiter::new(),
        }
    }
}

#[pin_project]
struct NextChunk<'a, 'd, I>
where
    I: QuerySet + 'd,
{
    stream: &'a mut CopyOutStream<'d, I>,
    #[pin]
    waiter: Waiter<'d>,
}

impl<'d, I> Fiber<'d> for NextChunk<'_, 'd, I>
where
    I: QuerySet + 'd,
{
    type Output = Result<Option<Vec<u8>>, Error>;
    fn poll(self: Pin<&mut Self>, mut cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = self.project();
        let waiter = this.waiter.as_ref();
        let state = &mut this.stream.state;
        if state.poll_settle(cx.as_mut(), waiter).is_pending() {
            return Poll::Pending;
        }
        match state {
            DispatchedStream::Throttled { .. } => unreachable!("poll_settle drains Throttled"),
            DispatchedStream::Connecting { .. } => unreachable!("poll_settle drains Connecting"),
            DispatchedStream::Failed(error) => Poll::Ready(Err(error
                .take()
                .expect("copy out fiber polled after failure delivered"))),
            DispatchedStream::Pending { reply } => match Pin::new(reply).poll_next(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(None) => Poll::Ready(Ok(None)),
                Poll::Ready(Some(Ok(payload))) => Poll::Ready(Ok(Some(payload.to_vec()))),
                Poll::Ready(Some(Err(error))) => Poll::Ready(Err(error)),
            },
        }
    }
}

#[pin_project]
pub struct NextNotification<'d, I>
where
    I: QuerySet + 'd,
{
    client: Client<'d, I>,
    #[pin]
    waiter: Waiter<'d>,
}

impl<'d, I: QuerySet + 'd> Fiber<'d> for NextNotification<'d, I> {
    type Output = Result<crate::Notification, Error>;
    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = self.project();
        let waiter = this.waiter.as_ref();
        let s = &this.client.port.shared;
        if let Some(n) = s.pop_notification() {
            waiter.unregister();
            return Poll::Ready(Ok(n));
        }
        if s.is_failed() {
            waiter.unregister();
            return Poll::Ready(Err(Error::Closed));
        }
        if s.try_register_notification(waiter, cx.as_ref()) {
            Poll::Pending
        } else {
            Poll::Ready(Err(Error::WaiterCapacity))
        }
    }
}
