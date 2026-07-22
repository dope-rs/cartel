use std::marker::PhantomData;
use std::pin::Pin;
use std::task::Poll;

use cartel_core::{Extract, Registrable, Reply, ReplyStream, Slot};
use dope::driver::token::Token;
use dope_fiber::{Context, Fiber, Waiter};
use o3::buffer::Shared;
use o3::cell::RegionToken;
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

impl<Q: TypedQuery> Extract<RowItem> for ExtractAll<Q> {
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

impl Extract<RowItem> for ExtractUnit {
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

impl Extract<RowItem> for ExtractOne {
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

impl Extract<RowItem> for ExtractFirst {
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
    request: Pending<'d>,
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

enum DispatchRetry<'d> {
    Pending(Pending<'d>),
    Enqueued { conn: Token },
    Throttled { throttle: Throttle<'d> },
    Failed(Error),
}

enum TransactionDispatchRetry<'d, I>
where
    I: QuerySet + 'd,
{
    Pending(Pending<'d>),
    Enqueued {
        lease: TransactionLease<'d, I>,
    },
    Throttled {
        lease: TransactionLease<'d, I>,
        throttle: Throttle<'d>,
    },
    Failed(Error),
}

enum ThrottleRetry<'d> {
    Pending(Throttle<'d>),
    Ready(Result<(), Error>),
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
        request: Pending<'d>,
    },
    Failed(Error),
    Done,
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
        loop {
            match std::mem::replace(self, DispatchedStream::Done) {
                DispatchedStream::Pending { reply } => {
                    *self = DispatchedStream::Pending { reply };
                    return Poll::Ready(());
                }
                DispatchedStream::Connecting {
                    client,
                    target,
                    mut reply,
                    request,
                } => match Disp::retry_connecting(
                    client,
                    target,
                    cx.as_mut(),
                    waiter,
                    &mut reply,
                    request,
                ) {
                    DispatchRetry::Pending(request) => {
                        *self = DispatchedStream::Connecting {
                            client,
                            target,
                            reply,
                            request,
                        };
                        return Poll::Pending;
                    }
                    DispatchRetry::Enqueued { .. } => {
                        *self = DispatchedStream::Pending { reply };
                    }
                    DispatchRetry::Throttled { throttle } => {
                        *self = DispatchedStream::Throttled {
                            client,
                            reply,
                            throttle,
                        };
                    }
                    DispatchRetry::Failed(error) => {
                        *self = DispatchedStream::Failed(error);
                    }
                },
                DispatchedStream::Throttled {
                    client,
                    mut reply,
                    throttle,
                } => match Disp::retry_throttled(client, cx.as_mut(), waiter, &mut reply, throttle)
                {
                    ThrottleRetry::Pending(throttle) => {
                        *self = DispatchedStream::Throttled {
                            client,
                            reply,
                            throttle,
                        };
                        return Poll::Pending;
                    }
                    ThrottleRetry::Ready(Ok(())) => {
                        *self = DispatchedStream::Pending { reply };
                    }
                    ThrottleRetry::Ready(Err(error)) => {
                        *self = DispatchedStream::Failed(error);
                    }
                },
                DispatchedStream::Failed(error) => {
                    *self = DispatchedStream::Failed(error);
                    return Poll::Ready(());
                }
                DispatchedStream::Done => {
                    return Poll::Ready(());
                }
            }
        }
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
        Ok(CopyInGuard {
            client,
            dispatched: Box::pin(Disp::dispatch_raw::<ExtractUnit, I>(
                client,
                target,
                Request::raw_extra(sql, Extra::CopyInOpen),
            )),
        })
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
        let reply = Reply::<RowItem, X>::new();
        let outcome = match Self::typed::<Q, I>(client, params, X::SYNC_AFTER) {
            Ok(request) => DispatchOutcome::NoConn { request },
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
        let reply = Reply::<RowItem, X>::new();
        let outcome = match Self::raw(client, req) {
            Ok(request) => DispatchOutcome::NoConn { request },
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
        let reply = Reply::<RowItem, ExtractUnit>::new();
        let outcome = match Self::raw(client, req) {
            Ok(request) => TransactionDispatchOutcome::NoConn { request },
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
                request,
            },
            DispatchOutcome::Failed(e) => DispatchState::Failed(e),
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
        let reply = ReplyStream::<RowItem, X>::new();
        let outcome = match Self::typed::<Q, I>(client, params, X::SYNC_AFTER) {
            Ok(request) => DispatchOutcome::NoConn { request },
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
        let reply = ReplyStream::<RowItem, X>::new();
        let outcome = match Self::raw(client, req) {
            Ok(request) => DispatchOutcome::NoConn { request },
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
                request,
            },
            DispatchOutcome::Failed(e) => DispatchedStream::Failed(e),
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
        region: &mut RegionToken<'d>,
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
        match Self::stage_request(client, conn_id, reply, request, region) {
            Ok(()) => DispatchOutcome::Enqueued { conn: conn_id },
            Err((Error::Backpressure { .. }, request)) => DispatchOutcome::Throttled {
                throttle: Throttle {
                    request,
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
        region: &mut RegionToken<'d>,
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
        match Self::stage_request(client, conn, reply, request, region) {
            Ok(()) => TransactionDispatchOutcome::Enqueued { lease },
            Err((Error::Backpressure { .. }, request)) => TransactionDispatchOutcome::Throttled {
                lease,
                throttle: Throttle { request, conn },
            },
            Err((error, _)) => TransactionDispatchOutcome::Failed(error),
        }
    }

    fn stage_request<'d, I>(
        client: Client<'d, I>,
        conn_id: Token,
        reply: &mut impl Registrable<'d, RowItem>,
        request: Pending<'d>,
        region: &mut RegionToken<'d>,
    ) -> Result<(), (Error, Pending<'d>)>
    where
        I: QuerySet,
    {
        let Pending { frame, boundary } = request;
        match client
            .port
            .try_enqueue_reply(conn_id, frame, reply, boundary, region)
        {
            Ok(()) => Ok(()),
            Err((error, frame)) => Err((error, Pending { frame, boundary })),
        }
    }

    fn retry_connecting<'d, I>(
        client: Client<'d, I>,
        target: Option<(Token, u64)>,
        mut cx: Pin<&mut Context<'_, 'd>>,
        waiter: Pin<&Waiter<'d>>,
        reply: &mut impl Registrable<'d, RowItem>,
        request: Pending<'d>,
    ) -> DispatchRetry<'d>
    where
        I: QuerySet,
    {
        let outcome = {
            let region = cx.as_mut().region_token();
            Self::try_dispatch_reply(client, target, reply, request, region)
        };
        match outcome {
            DispatchOutcome::NoConn { request } => {
                let shared = &client.port.shared;
                if shared.is_failed() {
                    waiter.unregister();
                    return DispatchRetry::Failed(Error::Closed);
                }
                if shared.try_register_ready(waiter, cx.as_ref()) {
                    DispatchRetry::Pending(request)
                } else {
                    DispatchRetry::Failed(Error::WaiterCapacity)
                }
            }
            DispatchOutcome::Enqueued { conn } => {
                waiter.unregister();
                DispatchRetry::Enqueued { conn }
            }
            DispatchOutcome::Throttled { throttle } => {
                waiter.unregister();
                DispatchRetry::Throttled { throttle }
            }
            DispatchOutcome::Failed(error) => {
                waiter.unregister();
                DispatchRetry::Failed(error)
            }
        }
    }

    fn retry_transaction_connecting<'d, I>(
        client: Client<'d, I>,
        mut cx: Pin<&mut Context<'_, 'd>>,
        waiter: Pin<&Waiter<'d>>,
        reply: &mut impl Registrable<'d, RowItem>,
        request: Pending<'d>,
    ) -> TransactionDispatchRetry<'d, I>
    where
        I: QuerySet,
    {
        let outcome = {
            let region = cx.as_mut().region_token();
            Self::try_dispatch_transaction(client, reply, request, region)
        };
        match outcome {
            TransactionDispatchOutcome::NoConn { request } => {
                let shared = &client.port.shared;
                if shared.is_failed() {
                    waiter.unregister();
                    return TransactionDispatchRetry::Failed(Error::Closed);
                }
                if shared.try_register_ready(waiter, cx.as_ref()) {
                    TransactionDispatchRetry::Pending(request)
                } else {
                    TransactionDispatchRetry::Failed(Error::WaiterCapacity)
                }
            }
            TransactionDispatchOutcome::Enqueued { lease } => {
                waiter.unregister();
                TransactionDispatchRetry::Enqueued { lease }
            }
            TransactionDispatchOutcome::Throttled { lease, throttle } => {
                waiter.unregister();
                TransactionDispatchRetry::Throttled { lease, throttle }
            }
            TransactionDispatchOutcome::Failed(error) => {
                waiter.unregister();
                TransactionDispatchRetry::Failed(error)
            }
        }
    }

    fn retry_throttled<'d, I>(
        client: Client<'d, I>,
        mut cx: Pin<&mut Context<'_, 'd>>,
        waiter: Pin<&Waiter<'d>>,
        reply: &mut impl Registrable<'d, RowItem>,
        throttle: Throttle<'d>,
    ) -> ThrottleRetry<'d>
    where
        I: QuerySet,
    {
        let shared = &client.port.shared;
        if shared.is_failed() {
            waiter.unregister();
            return ThrottleRetry::Ready(Err(Error::Closed));
        }
        let Throttle { request, conn } = throttle;
        let staged = {
            let region = cx.as_mut().region_token();
            Self::stage_request(client, conn, reply, request, region)
        };
        match staged {
            Ok(()) => {
                waiter.unregister();
                ThrottleRetry::Ready(Ok(()))
            }
            Err((Error::Backpressure { .. }, request)) => {
                if shared.try_register_egress(waiter, cx.as_ref()) {
                    ThrottleRetry::Pending(Throttle { request, conn })
                } else {
                    ThrottleRetry::Ready(Err(Error::WaiterCapacity))
                }
            }
            Err((error, _)) => {
                waiter.unregister();
                ThrottleRetry::Ready(Err(error))
            }
        }
    }

    fn dispatch_copy_data<'d, I: QuerySet>(
        client: Client<'d, I>,
        target: Token,
        data: &[u8],
        region: &mut RegionToken<'d>,
    ) -> Result<(), Error> {
        Self::check_can_dispatch(client)?;
        let frame = client.port.encode(|frame| encode::copy_data(frame, data))?;
        client
            .port
            .try_enqueue(target, frame, region)
            .map_err(|(error, _)| error)
    }

    fn dispatch_copy_finish<'d, I: QuerySet>(
        client: Client<'d, I>,
        target: Token,
        region: &mut RegionToken<'d>,
    ) -> Result<(), Error> {
        Self::check_can_dispatch(client)?;
        if !client.port.can_push_boundary(target, region) {
            return Err(Error::ResponseCapacity);
        }
        let frame = client.port.encode(|frame| {
            encode::copy_done(frame);
            encode::sync(frame);
        })?;
        client
            .port
            .try_enqueue(target, frame, region)
            .map_err(|(error, _)| error)?;
        let marked = client.port.push_boundary(target, region);
        debug_assert!(marked);
        Ok(())
    }

    pub(super) fn rollback_on_drop<I: QuerySet>(
        client: Client<'_, I>,
        target: (Token, u64),
        _sql: &str,
    ) {
        // Drop has no runtime state permission. Closing quarantines the
        // connection and lets normal async recovery perform rollback safely.
        client.port.close(target.0);
    }

    fn quarantine_transaction<I: QuerySet>(client: Client<'_, I>, target: (Token, u64)) {
        if !client.port.shared.quarantine_transaction(target) {
            return;
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
        request: Pending<'d>,
    },
    Failed(Error),
    Done,
}

enum TransactionDispatchState<'d, I>
where
    I: QuerySet + 'd,
{
    Pending {
        lease: TransactionLease<'d, I>,
    },
    Throttled {
        lease: TransactionLease<'d, I>,
        throttle: Throttle<'d>,
    },
    Connecting {
        request: Pending<'d>,
    },
    Failed(Error),
    Done,
}

#[pin_project]
pub(super) struct TransactionDispatched<'d, I>
where
    I: QuerySet + 'd,
{
    client: Client<'d, I>,
    reply: Reply<'d, RowItem, ExtractUnit>,
    state: TransactionDispatchState<'d, I>,
    #[pin]
    waiter: Waiter<'d>,
}

impl<'d, I: QuerySet + 'd> TransactionDispatched<'d, I> {
    fn new(
        client: Client<'d, I>,
        reply: Reply<'d, RowItem, ExtractUnit>,
        outcome: TransactionDispatchOutcome<'d, I>,
    ) -> Self {
        let state = match outcome {
            TransactionDispatchOutcome::Enqueued { lease } => {
                TransactionDispatchState::Pending { lease }
            }
            TransactionDispatchOutcome::Throttled { lease, throttle } => {
                TransactionDispatchState::Throttled { lease, throttle }
            }
            TransactionDispatchOutcome::NoConn { request } => {
                TransactionDispatchState::Connecting { request }
            }
            TransactionDispatchOutcome::Failed(error) => TransactionDispatchState::Failed(error),
        };
        Self {
            client,
            reply,
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
        loop {
            match std::mem::replace(me.state, TransactionDispatchState::Done) {
                TransactionDispatchState::Connecting { request } => {
                    match Disp::retry_transaction_connecting(
                        client,
                        cx.as_mut(),
                        waiter,
                        me.reply,
                        request,
                    ) {
                        TransactionDispatchRetry::Pending(request) => {
                            *me.state = TransactionDispatchState::Connecting { request };
                            return Poll::Pending;
                        }
                        TransactionDispatchRetry::Enqueued { lease } => {
                            *me.state = TransactionDispatchState::Pending { lease };
                        }
                        TransactionDispatchRetry::Throttled { lease, throttle } => {
                            *me.state = TransactionDispatchState::Throttled { lease, throttle };
                        }
                        TransactionDispatchRetry::Failed(error) => {
                            return Poll::Ready(Err(error));
                        }
                    }
                }
                TransactionDispatchState::Throttled { lease, throttle } => {
                    match Disp::retry_throttled(client, cx.as_mut(), waiter, me.reply, throttle) {
                        ThrottleRetry::Pending(throttle) => {
                            *me.state = TransactionDispatchState::Throttled { lease, throttle };
                            return Poll::Pending;
                        }
                        ThrottleRetry::Ready(Ok(())) => {
                            *me.state = TransactionDispatchState::Pending { lease };
                        }
                        ThrottleRetry::Ready(Err(error)) => {
                            drop(lease);
                            return Poll::Ready(Err(error));
                        }
                    }
                }
                TransactionDispatchState::Pending { lease } => {
                    match Fiber::poll(Pin::new(me.reply), cx.as_mut()) {
                        Poll::Pending => {
                            *me.state = TransactionDispatchState::Pending { lease };
                            return Poll::Pending;
                        }
                        Poll::Ready(Ok(())) => return Poll::Ready(Ok(lease)),
                        Poll::Ready(Err(error)) => {
                            drop(lease);
                            return Poll::Ready(Err(error));
                        }
                    }
                }
                TransactionDispatchState::Failed(error) => {
                    return Poll::Ready(Err(error));
                }
                TransactionDispatchState::Done => {
                    return Poll::Ready(Err(Error::Closed));
                }
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
            DispatchState::Connecting { .. } | DispatchState::Failed(_) | DispatchState::Done => {
                None
            }
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
        loop {
            match std::mem::replace(me.state, DispatchState::Done) {
                DispatchState::Connecting {
                    client,
                    target,
                    request,
                } => match Disp::retry_connecting(
                    client,
                    target,
                    cx.as_mut(),
                    waiter,
                    me.reply,
                    request,
                ) {
                    DispatchRetry::Pending(request) => {
                        *me.state = DispatchState::Connecting {
                            client,
                            target,
                            request,
                        };
                        return Poll::Pending;
                    }
                    DispatchRetry::Enqueued { conn } => {
                        *me.state = DispatchState::Pending { conn };
                    }
                    DispatchRetry::Throttled { throttle } => {
                        *me.state = DispatchState::Throttled { client, throttle };
                    }
                    DispatchRetry::Failed(error) => return Poll::Ready(Err(error)),
                },
                DispatchState::Throttled { client, throttle } => {
                    let conn = throttle.conn;
                    match Disp::retry_throttled(client, cx.as_mut(), waiter, me.reply, throttle) {
                        ThrottleRetry::Pending(throttle) => {
                            *me.state = DispatchState::Throttled { client, throttle };
                            return Poll::Pending;
                        }
                        ThrottleRetry::Ready(Ok(())) => {
                            *me.state = DispatchState::Pending { conn };
                        }
                        ThrottleRetry::Ready(Err(error)) => return Poll::Ready(Err(error)),
                    }
                }
                DispatchState::Pending { conn } => {
                    let outcome = Fiber::poll(Pin::new(me.reply), cx.as_mut());
                    if outcome.is_pending() {
                        *me.state = DispatchState::Pending { conn };
                    }
                    return outcome;
                }
                DispatchState::Failed(error) => return Poll::Ready(Err(error)),
                DispatchState::Done => return Poll::Ready(Err(Error::Closed)),
            }
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
        match std::mem::replace(&mut stream.state, DispatchedStream::Done) {
            DispatchedStream::Failed(error) => Poll::Ready(Err(error)),
            DispatchedStream::Pending { mut reply } => {
                let outcome = Pin::new(&mut reply).poll_next(cx);
                stream.state = DispatchedStream::Pending { reply };
                match outcome {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(None) => Poll::Ready(Ok(None)),
                    Poll::Ready(Some(Ok(payload))) => {
                        Poll::Ready(decode_row(stream.decoder, &payload).map(Some))
                    }
                    Poll::Ready(Some(Err(error))) => Poll::Ready(Err(error)),
                }
            }
            DispatchedStream::Throttled { .. }
            | DispatchedStream::Connecting { .. }
            | DispatchedStream::Done => Poll::Ready(Err(Error::Closed)),
        }
    }
}

pub struct CopyInGuard<'d, I>
where
    I: QuerySet + 'd,
{
    client: Client<'d, I>,
    dispatched: Pin<Box<Dispatched<'d, I, ExtractUnit>>>,
}

impl<'d, I: QuerySet + 'd> CopyInGuard<'d, I> {
    pub fn write<'a>(
        &'a mut self,
        chunk: &'a [u8],
    ) -> impl Fiber<'d, Output = Result<(), Error>> + 'a {
        let client = self.client;
        let mut completed = false;
        dope_fiber::poll_fn(move |mut cx| {
            if completed {
                return Poll::Ready(Err(Error::Closed));
            }
            match Fiber::poll(self.dispatched.as_mut(), cx.as_mut()) {
                Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) => Poll::Ready(Err(Error::Closed)),
                Poll::Pending => {
                    let Some(target) = self.dispatched.as_ref().get_ref().resolved_conn() else {
                        return Poll::Pending;
                    };
                    completed = true;
                    let region = cx.as_mut().region_token();
                    Poll::Ready(Disp::dispatch_copy_data(client, target, chunk, region))
                }
            }
        })
    }

    pub fn finish(self) -> impl Fiber<'d, Output = Result<(), Error>> {
        let client = self.client;
        let mut dispatched = self.dispatched;
        let mut finished = false;
        dope_fiber::poll_fn(move |mut cx| {
            if !finished {
                match Fiber::poll(dispatched.as_mut(), cx.as_mut()) {
                    Poll::Ready(output) => return Poll::Ready(output),
                    Poll::Pending => {
                        let Some(target) = dispatched.as_ref().get_ref().resolved_conn() else {
                            return Poll::Pending;
                        };
                        let result = {
                            let region = cx.as_mut().region_token();
                            Disp::dispatch_copy_finish(client, target, region)
                        };
                        if let Err(error) = result {
                            return Poll::Ready(Err(error));
                        }
                        finished = true;
                    }
                }
            }
            Fiber::poll(dispatched.as_mut(), cx)
        })
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
        match std::mem::replace(state, DispatchedStream::Done) {
            DispatchedStream::Failed(error) => Poll::Ready(Err(error)),
            DispatchedStream::Pending { mut reply } => {
                let outcome = Pin::new(&mut reply).poll_next(cx);
                *state = DispatchedStream::Pending { reply };
                match outcome {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(None) => Poll::Ready(Ok(None)),
                    Poll::Ready(Some(Ok(payload))) => Poll::Ready(Ok(Some(payload.to_vec()))),
                    Poll::Ready(Some(Err(error))) => Poll::Ready(Err(error)),
                }
            }
            DispatchedStream::Throttled { .. }
            | DispatchedStream::Connecting { .. }
            | DispatchedStream::Done => Poll::Ready(Err(Error::Closed)),
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
