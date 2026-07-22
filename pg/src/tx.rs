use std::pin::Pin;
use std::task::Poll;

use dope::driver::token::Token;
use dope_fiber::{Context, Fiber, IntoFiber, Waiter};
use pin_project::pin_project;

use crate::Error;
use crate::client::{Client, Disp, Dispatched, ExtractUnit, PgOps, Request, TransactionLease};
use crate::query::QuerySet;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum IsolationLevel {
    #[default]
    Default,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AccessMode {
    #[default]
    Default,
    ReadWrite,
    ReadOnly,
}

pub trait PgPool<'d, I>: PgOps<'d, I>
where
    I: QuerySet + 'd,
{
    fn begin(self) -> impl Fiber<'d, Output = Result<TxGuard<'d, I>, Error>>
    where
        Self: Sized,
    {
        TxBuilder::new(self.client()).begin()
    }

    fn tx<F, B, T>(self, body: F) -> impl Fiber<'d, Output = Result<T, Error>>
    where
        Self: Sized,
        F: FnOnce(Tx<'d, I>) -> B,
        B: IntoFiber<'d, Output = Result<T, Error>>,
    {
        TxBuilder::new(self.client()).run(body)
    }

    fn tx_with(self) -> TxBuilder<'d, I>
    where
        Self: Sized,
    {
        TxBuilder::new(self.client())
    }
}

impl<'d, I: QuerySet + 'd> PgPool<'d, I> for Client<'d, I> {}

pub struct TxBuilder<'d, I>
where
    I: QuerySet + 'd,
{
    client: Client<'d, I>,
    isolation: IsolationLevel,
    access_mode: AccessMode,
    deferrable: bool,
    timeout_ms: Option<u32>,
}

impl<'d, I: QuerySet + 'd> TxBuilder<'d, I> {
    fn new(client: Client<'d, I>) -> Self {
        Self {
            client,
            isolation: IsolationLevel::Default,
            access_mode: AccessMode::Default,
            deferrable: false,
            timeout_ms: None,
        }
    }

    pub fn isolation(mut self, level: IsolationLevel) -> Self {
        self.isolation = level;
        self
    }

    pub fn read_only(mut self) -> Self {
        self.access_mode = AccessMode::ReadOnly;
        self
    }

    pub fn read_write(mut self) -> Self {
        self.access_mode = AccessMode::ReadWrite;
        self
    }

    pub fn deferrable(mut self) -> Self {
        self.deferrable = true;
        self
    }

    pub fn statement_timeout(mut self, dur: std::time::Duration) -> Self {
        let ms = dur.as_millis().min(u32::MAX as u128) as u32;
        self.timeout_ms = Some(ms);
        self
    }

    pub fn begin(self) -> impl Fiber<'d, Output = Result<TxGuard<'d, I>, Error>> {
        let sql = self.build_sql();
        let timeout_ms = self.timeout_ms;
        let client = self.client;
        let begin = Disp::dispatch_transaction(client, Request::raw(&sql));
        dope_fiber::fiber!('d => async move {
            let lease = begin.await?;
            let target = lease.target();
            if let Some(ms) = timeout_ms {
                let sql_set = ::std::format!("SET LOCAL statement_timeout TO {}", ms);
                let setting = Disp::dispatch_raw::<ExtractUnit, I>(
                    client,
                    Some(target),
                    Request::raw(&sql_set),
                );
                setting.await?;
            }
            Ok(TxGuard::new(lease))
        })
    }

    pub fn run<F, B, T>(self, body: F) -> impl Fiber<'d, Output = Result<T, Error>>
    where
        F: FnOnce(Tx<'d, I>) -> B,
        B: IntoFiber<'d, Output = Result<T, Error>>,
    {
        let begin = self.begin();
        dope_fiber::fiber!('d => async move {
            let transaction = begin.await?;
            let outcome = body(transaction.tx()).into_fiber().await;
            let finalizer = transaction.finalize(if outcome.is_ok() {
                "COMMIT"
            } else {
                "ROLLBACK"
            });
            let finalized = finalizer.await;
            match outcome {
                Ok(value) => finalized.map(|()| value),
                Err(error) => Err(error),
            }
        })
    }

    fn build_sql(&self) -> String {
        let mut s = String::with_capacity(64);
        s.push_str("BEGIN");
        match self.isolation {
            IsolationLevel::Default => {}
            IsolationLevel::ReadCommitted => s.push_str(" ISOLATION LEVEL READ COMMITTED"),
            IsolationLevel::RepeatableRead => s.push_str(" ISOLATION LEVEL REPEATABLE READ"),
            IsolationLevel::Serializable => s.push_str(" ISOLATION LEVEL SERIALIZABLE"),
        }
        match self.access_mode {
            AccessMode::Default => {}
            AccessMode::ReadWrite => s.push_str(" READ WRITE"),
            AccessMode::ReadOnly => s.push_str(" READ ONLY"),
        }
        if self.deferrable {
            s.push_str(" DEFERRABLE");
        }
        s
    }
}

pub struct TxGuard<'d, I>
where
    I: QuerySet + 'd,
{
    lease: TransactionLease<'d, I>,
}

pub struct Tx<'d, I>
where
    I: QuerySet + 'd,
{
    client: Client<'d, I>,
    target: (Token, u64),
}

impl<I: QuerySet> Copy for Tx<'_, I> {}

impl<I: QuerySet> Clone for Tx<'_, I> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'d, I: QuerySet + 'd> PgOps<'d, I> for Tx<'d, I> {
    fn client(&self) -> Client<'d, I> {
        self.client
    }

    fn target(&self) -> Option<(Token, u64)> {
        Some(self.target)
    }

    fn backend_pid(&self) -> Option<i32> {
        self.client.port.shared.backend_pid_for(self.target.0)
    }
}

impl<'d, I: QuerySet + 'd> PgOps<'d, I> for TxGuard<'d, I> {
    fn client(&self) -> Client<'d, I> {
        self.lease.client()
    }

    fn target(&self) -> Option<(Token, u64)> {
        Some(self.lease.target())
    }

    fn backend_pid(&self) -> Option<i32> {
        self.client()
            .port
            .shared
            .backend_pid_for(self.lease.target().0)
    }
}

impl<'d, I: QuerySet + 'd> TxGuard<'d, I> {
    fn new(lease: TransactionLease<'d, I>) -> Self {
        Self { lease }
    }

    fn tx(&self) -> Tx<'d, I> {
        Tx {
            client: self.lease.client(),
            target: self.lease.target(),
        }
    }

    fn finalize(self, sql: &'static str) -> TransactionFinalizer<'d, I> {
        let client = self.lease.client();
        let target = self.lease.target();
        TransactionFinalizer {
            client,
            target,
            lease: Some(self.lease),
            dispatched: None,
            outcome: None,
            sql,
            waiter: Waiter::new(),
        }
    }

    pub fn commit(self) -> impl Fiber<'d, Output = Result<(), Error>> {
        self.finalize("COMMIT")
    }

    pub fn rollback(self) -> impl Fiber<'d, Output = Result<(), Error>> {
        self.finalize("ROLLBACK")
    }

    pub fn savepoint(
        &self,
        name: impl Into<String>,
    ) -> impl Fiber<'d, Output = Result<SavepointGuard<'d, I>, Error>> {
        SavepointGuard::open(self.lease.client(), self.lease.target(), name.into())
    }

    pub fn cancel_token(&self) -> Option<CancelToken<'d, I>> {
        let pid = self.backend_pid()?;
        let secret_key = self
            .lease
            .client()
            .port
            .shared
            .backend_key_for(self.lease.target().0)?;
        Some(CancelToken {
            client: self.lease.client(),
            pid,
            secret_key,
        })
    }
}

#[pin_project]
struct TransactionFinalizer<'d, I>
where
    I: QuerySet + 'd,
{
    client: Client<'d, I>,
    target: (Token, u64),
    lease: Option<TransactionLease<'d, I>>,
    dispatched: Option<Pin<Box<Dispatched<'d, I, ExtractUnit>>>>,
    outcome: Option<Result<(), Error>>,
    sql: &'static str,
    #[pin]
    waiter: Waiter<'d>,
}

impl<'d, I: QuerySet + 'd> TransactionFinalizer<'d, I> {
    fn transfer(
        dispatched: &Option<Pin<Box<Dispatched<'d, I, ExtractUnit>>>>,
        lease: &mut Option<TransactionLease<'d, I>>,
    ) -> Result<(), Error> {
        if !dispatched
            .as_ref()
            .is_some_and(|future| future.as_ref().get_ref().is_enqueued())
        {
            return Ok(());
        }
        let Some(mut lease) = lease.take() else {
            return Ok(());
        };
        if lease.transfer() {
            Ok(())
        } else {
            Err(Error::Closed)
        }
    }
}

impl<'d, I: QuerySet + 'd> Fiber<'d> for TransactionFinalizer<'d, I> {
    type Output = Result<(), Error>;
    fn poll(self: Pin<&mut Self>, mut cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = self.project();
        let client = *this.client;
        let target = *this.target;
        let sql = *this.sql;
        let waiter = this.waiter.as_ref();
        if this.outcome.is_none() && this.dispatched.is_none() {
            let lease = this.lease.as_ref().expect("transaction lease missing");
            let lease_client = lease.client();
            let lease_target = lease.target();
            if !lease_client.port.shared.is_transaction_held(lease_target) {
                drop(this.lease.take());
                waiter.unregister();
                return Poll::Ready(Err(Error::Closed));
            }
            if lease_client.port.response_len(lease_target.0) != 0 {
                if !lease_client.port.shared.try_register_transaction(
                    lease_target,
                    waiter,
                    cx.as_ref(),
                ) {
                    drop(this.lease.take());
                    waiter.unregister();
                    return Poll::Ready(Err(Error::Closed));
                }
                if lease_client.port.response_len(lease_target.0) != 0 {
                    return Poll::Pending;
                }
                waiter.unregister();
            }
            *this.dispatched = Some(Box::pin(Disp::dispatch_raw::<ExtractUnit, I>(
                lease_client,
                Some(lease_target),
                Request::raw(sql),
            )));
            if let Err(error) = Self::transfer(this.dispatched, this.lease) {
                drop(this.lease.take());
                waiter.unregister();
                return Poll::Ready(Err(error));
            }
        }
        if this.outcome.is_none() {
            let outcome = Fiber::poll(
                this.dispatched
                    .as_mut()
                    .expect("transaction finalizer missing")
                    .as_mut(),
                cx.as_mut(),
            );
            if let Err(error) = Self::transfer(this.dispatched, this.lease) {
                drop(this.lease.take());
                waiter.unregister();
                return Poll::Ready(Err(error));
            }
            let Poll::Ready(outcome) = outcome else {
                return Poll::Pending;
            };
            if this.lease.is_some() {
                drop(this.lease.take());
            }
            *this.outcome = Some(outcome);
            *this.dispatched = None;
        }
        loop {
            match client.port.shared.transaction_settled(target) {
                Some(true) => {
                    waiter.unregister();
                    return Poll::Ready(this.outcome.take().expect("transaction outcome missing"));
                }
                Some(false) => {
                    waiter.unregister();
                    return Poll::Ready(match this.outcome.take() {
                        Some(Err(error)) => Err(error),
                        Some(Ok(())) | None => Err(Error::Closed),
                    });
                }
                None => {}
            }
            if !client
                .port
                .shared
                .try_register_transaction(target, waiter, cx.as_ref())
            {
                continue;
            }
            if client.port.shared.transaction_settled(target).is_none() {
                return Poll::Pending;
            }
        }
    }
}

pub struct CancelToken<'d, I>
where
    I: QuerySet + 'd,
{
    client: Client<'d, I>,
    pid: i32,
    secret_key: i32,
}

impl<I: QuerySet> Clone for CancelToken<'_, I> {
    fn clone(&self) -> Self {
        Self {
            client: self.client,
            pid: self.pid,
            secret_key: self.secret_key,
        }
    }
}

impl<'d, I: QuerySet + 'd> CancelToken<'d, I> {
    pub fn pid(&self) -> i32 {
        self.pid
    }

    pub fn secret_key(&self) -> i32 {
        self.secret_key
    }

    pub fn cancel_request_message(&self) -> [u8; 16] {
        crate::encode::cancel_request_message(self.pid, self.secret_key)
    }

    pub fn cancel(&self) -> Dispatched<'d, I, ExtractUnit> {
        let sql = format!("SELECT pg_cancel_backend({})", self.pid);
        Disp::dispatch_raw::<ExtractUnit, I>(self.client, None, Request::raw(&sql))
    }
}

pub struct SavepointGuard<'d, I>
where
    I: QuerySet + 'd,
{
    client: Client<'d, I>,
    target: (Token, u64),
    name: String,
    finalized: bool,
}

impl<'d, I: QuerySet + 'd> PgOps<'d, I> for SavepointGuard<'d, I> {
    fn client(&self) -> Client<'d, I> {
        self.client
    }

    fn target(&self) -> Option<(Token, u64)> {
        Some(self.target)
    }
}

impl<'d, I: QuerySet + 'd> SavepointGuard<'d, I> {
    fn raw_pinned(&self, sql: &str) -> Dispatched<'d, I, ExtractUnit> {
        Disp::dispatch_raw::<ExtractUnit, I>(self.client, Some(self.target), Request::raw(sql))
    }

    fn open(
        client: Client<'d, I>,
        target: (Token, u64),
        name: String,
    ) -> impl Fiber<'d, Output = Result<SavepointGuard<'d, I>, Error>> {
        let sql = format!("SAVEPOINT \"{}\"", name.replace('"', "\"\""));
        let opening =
            Disp::dispatch_raw::<ExtractUnit, I>(client, Some(target), Request::raw(&sql));
        dope_fiber::fiber!('d => async move {
            opening.await?;
            Ok(SavepointGuard {
                client,
                target,
                name,
                finalized: false,
            })
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn release(mut self) -> Dispatched<'d, I, ExtractUnit> {
        self.finalized = true;
        let sql = format!("RELEASE SAVEPOINT \"{}\"", self.name.replace('"', "\"\""));
        self.raw_pinned(&sql)
    }

    pub fn rollback(mut self) -> Dispatched<'d, I, ExtractUnit> {
        self.finalized = true;
        let sql = format!(
            "ROLLBACK TO SAVEPOINT \"{}\"",
            self.name.replace('"', "\"\"")
        );
        self.raw_pinned(&sql)
    }

    pub fn savepoint(
        &self,
        name: impl Into<String>,
    ) -> impl Fiber<'d, Output = Result<SavepointGuard<'d, I>, Error>> {
        SavepointGuard::open(self.client, self.target, name.into())
    }
}

impl<I: QuerySet> Drop for SavepointGuard<'_, I> {
    fn drop(&mut self) {
        if !self.finalized {
            let sql = format!(
                "ROLLBACK TO SAVEPOINT \"{}\"",
                self.name.replace('"', "\"\"")
            );
            Disp::rollback_on_drop(self.client, self.target, &sql);
        }
    }
}

pub struct ListenGuard<'d, I>
where
    I: QuerySet + 'd,
{
    client: Client<'d, I>,
    target: (Token, u64),
    channel: String,
    finalized: bool,
}

impl<'d, I: QuerySet + 'd> PgOps<'d, I> for ListenGuard<'d, I> {
    fn client(&self) -> Client<'d, I> {
        self.client
    }

    fn target(&self) -> Option<(Token, u64)> {
        Some(self.target)
    }
}

impl<'d, I: QuerySet + 'd> ListenGuard<'d, I> {
    pub(super) fn from_parts(client: Client<'d, I>, target: (Token, u64), channel: String) -> Self {
        Self {
            client,
            target,
            channel,
            finalized: false,
        }
    }

    fn raw_pinned(&self, sql: &str) -> Dispatched<'d, I, ExtractUnit> {
        Disp::dispatch_raw::<ExtractUnit, I>(self.client, Some(self.target), Request::raw(sql))
    }

    pub fn channel(&self) -> &str {
        &self.channel
    }

    pub fn unlisten(mut self) -> Dispatched<'d, I, ExtractUnit> {
        self.finalized = true;
        let sql = format!("UNLISTEN \"{}\"", self.channel.replace('"', "\"\""));
        self.raw_pinned(&sql)
    }
}

impl<I: QuerySet> Drop for ListenGuard<'_, I> {
    fn drop(&mut self) {
        if !self.finalized {
            let sql = format!("UNLISTEN \"{}\"", self.channel.replace('"', "\"\""));
            Disp::rollback_on_drop(self.client, self.target, &sql);
        }
    }
}
