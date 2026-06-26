use std::future::Future;

use dope::fiber::Fiber;
use dope::manifold::connector::source::Dialer;
use dope::manifold::env::Env;
use dope::runtime::token::Token;
use dope::transport::Transport;

use crate::Error;
use crate::client::{Disp, Dispatched, DropAction, ExtractUnit, PgHolding, PgOps, Request};
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

pub trait PgPool<'d, I, S, E>: PgOps<'d, I, S, E>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    fn begin(&self) -> Fiber<'d, impl Future<Output = Result<TxGuard<'d, I, S, E>, Error>>> {
        TxBuilder::new(self.holding()).begin()
    }

    fn tx<F, T>(&self, body: F) -> Fiber<'d, impl Future<Output = Result<T, Error>>>
    where
        F: for<'tx> AsyncFnOnce(&'tx TxGuard<'d, I, S, E>) -> Result<T, Error>,
    {
        TxBuilder::new(self.holding()).run(body)
    }

    fn tx_with(&self) -> TxBuilder<'d, I, S, E> {
        TxBuilder::new(self.holding())
    }
}

impl<'d, I, S, E> PgPool<'d, I, S, E> for PgHolding<'d, I, S, E>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
}

pub struct TxBuilder<'d, I, S, E>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    conn: PgHolding<'d, I, S, E>,
    isolation: IsolationLevel,
    access_mode: AccessMode,
    deferrable: bool,
    timeout_ms: Option<u32>,
}

impl<'d, I, S, E> TxBuilder<'d, I, S, E>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    fn new(conn: PgHolding<'d, I, S, E>) -> Self {
        Self {
            conn,
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

    pub fn begin(self) -> Fiber<'d, impl Future<Output = Result<TxGuard<'d, I, S, E>, Error>>> {
        let sql = self.build_sql();
        let timeout_ms = self.timeout_ms;
        let conn = self.conn;
        let begin = Disp::dispatch_raw::<ExtractUnit, I, S, E>(conn, None, Request::raw(&sql));
        let begin_pin = begin.resolved_conn();
        if let Some(pin) = begin_pin {
            Disp::acquire_exclusive(conn, pin);
        }
        Fiber::new(async move {
            if let Err(e) = begin.await {
                if let Some(pin) = begin_pin {
                    Disp::release_exclusive(conn, pin);
                }
                return Err(e);
            }
            let pin = begin_pin.ok_or(Error::NoReadyConn)?;
            if let Some(ms) = timeout_ms {
                let sql_set = format!("SET LOCAL statement_timeout TO {}", ms);
                if let Err(e) = Disp::dispatch_raw::<ExtractUnit, I, S, E>(
                    conn,
                    Some(pin),
                    Request::raw(&sql_set),
                )
                .await
                {
                    if matches!(
                        Disp::rollback_on_drop(conn, pin, "ROLLBACK"),
                        DropAction::Delivered
                    ) {
                        Disp::release_exclusive(conn, pin);
                    }
                    return Err(e);
                }
            }
            Ok(TxGuard {
                conn,
                pin,
                finalised: false,
            })
        })
    }

    pub fn run<F, T>(self, body: F) -> Fiber<'d, impl Future<Output = Result<T, Error>>>
    where
        F: for<'tx> AsyncFnOnce(&'tx TxGuard<'d, I, S, E>) -> Result<T, Error>,
    {
        let begin = self.begin();
        Fiber::new(async move {
            let tx = begin.await?;
            let outcome = body(&tx).await;
            match outcome {
                Ok(v) => {
                    tx.commit().await?;
                    Ok(v)
                }
                Err(e) => {
                    tx.rollback().await.ok();
                    Err(e)
                }
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

pub struct TxGuard<'d, I, S, E>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    conn: PgHolding<'d, I, S, E>,
    pin: Token,
    finalised: bool,
}

impl<'d, I, S, E> PgOps<'d, I, S, E> for TxGuard<'d, I, S, E>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    fn holding(&self) -> PgHolding<'d, I, S, E> {
        self.conn
    }

    fn pin(&self) -> Option<Token> {
        Some(self.pin)
    }

    fn backend_pid(&self) -> Option<i32> {
        self.conn.session().shared.backend_pid_for(self.pin)
    }
}

impl<'d, I, S, E> TxGuard<'d, I, S, E>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    fn finalise(&self, sql: &str) -> Fiber<'d, Dispatched<'d, I, S, E, ExtractUnit>> {
        Fiber::new(Disp::dispatch_raw::<ExtractUnit, I, S, E>(
            self.conn,
            Some(self.pin),
            Request::raw(sql),
        ))
    }

    pub fn commit(mut self) -> Fiber<'d, Dispatched<'d, I, S, E, ExtractUnit>> {
        self.finalised = true;
        self.finalise("COMMIT")
    }

    pub fn rollback(mut self) -> Fiber<'d, Dispatched<'d, I, S, E, ExtractUnit>> {
        self.finalised = true;
        self.finalise("ROLLBACK")
    }

    pub fn savepoint(
        &self,
        name: impl Into<String>,
    ) -> Fiber<'d, impl Future<Output = Result<SavepointGuard<'d, I, S, E>, Error>>> {
        SavepointGuard::open(self.conn, self.pin, name.into())
    }

    pub fn cancel_token(&self) -> Option<CancelToken<'d, I, S, E>> {
        let pid = self.backend_pid()?;
        let secret_key = self
            .conn
            .session()
            .shared
            .backend_key_for(self.pin)
            .unwrap_or(0);
        Some(CancelToken {
            conn: self.conn,
            pid,
            secret_key,
        })
    }
}

pub struct CancelToken<'d, I, S, E>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    conn: PgHolding<'d, I, S, E>,
    pid: i32,
    secret_key: i32,
}

impl<'d, I, S, E> Clone for CancelToken<'d, I, S, E>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    fn clone(&self) -> Self {
        Self {
            conn: self.conn,
            pid: self.pid,
            secret_key: self.secret_key,
        }
    }
}

impl<'d, I, S, E> CancelToken<'d, I, S, E>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    pub fn pid(&self) -> i32 {
        self.pid
    }

    /// Backend secret from `BackendKeyData`, paired with [`pid`](Self::pid)
    /// to authorize an out-of-band CancelRequest.
    pub fn secret_key(&self) -> i32 {
        self.secret_key
    }

    /// Raw CancelRequest packet to send on a *fresh* connection to abort the
    /// in-flight query, per the postgres cancellation protocol.
    pub fn cancel_request_message(&self) -> [u8; 16] {
        let mut buf = o3::buffer::Owned::with_capacity(16);
        crate::encode::cancel_request(&mut buf, self.pid, self.secret_key);
        let mut out = [0u8; 16];
        out.copy_from_slice(buf.as_mut_slice());
        out
    }

    pub fn cancel(&self) -> Fiber<'d, Dispatched<'d, I, S, E, ExtractUnit>> {
        let sql = format!("SELECT pg_cancel_backend({})", self.pid);
        Fiber::new(Disp::dispatch_raw::<ExtractUnit, I, S, E>(
            self.conn,
            None,
            Request::raw(&sql),
        ))
    }
}

impl<'d, I, S, E> Drop for TxGuard<'d, I, S, E>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    fn drop(&mut self) {
        if !self.finalised {
            if matches!(
                Disp::rollback_on_drop(self.conn, self.pin, "ROLLBACK"),
                DropAction::Delivered
            ) {
                Disp::release_exclusive(self.conn, self.pin);
            }
        } else {
            Disp::release_exclusive(self.conn, self.pin);
        }
    }
}

pub struct SavepointGuard<'d, I, S, E>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    conn: PgHolding<'d, I, S, E>,
    pin: Token,
    name: String,
    finalised: bool,
}

impl<'d, I, S, E> PgOps<'d, I, S, E> for SavepointGuard<'d, I, S, E>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    fn holding(&self) -> PgHolding<'d, I, S, E> {
        self.conn
    }

    fn pin(&self) -> Option<Token> {
        Some(self.pin)
    }
}

impl<'d, I, S, E> SavepointGuard<'d, I, S, E>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    fn raw_pinned(&self, sql: &str) -> Fiber<'d, Dispatched<'d, I, S, E, ExtractUnit>> {
        Fiber::new(Disp::dispatch_raw::<ExtractUnit, I, S, E>(
            self.conn,
            Some(self.pin),
            Request::raw(sql),
        ))
    }

    fn open(
        conn: PgHolding<'d, I, S, E>,
        pin: Token,
        name: String,
    ) -> Fiber<'d, impl Future<Output = Result<SavepointGuard<'d, I, S, E>, Error>>> {
        let sql = format!("SAVEPOINT \"{}\"", name.replace('"', "\"\""));
        let opening =
            Disp::dispatch_raw::<ExtractUnit, I, S, E>(conn, Some(pin), Request::raw(&sql));
        Fiber::new(async move {
            opening.await?;
            Ok(SavepointGuard {
                conn,
                pin,
                name,
                finalised: false,
            })
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn release(mut self) -> Fiber<'d, Dispatched<'d, I, S, E, ExtractUnit>> {
        self.finalised = true;
        let sql = format!("RELEASE SAVEPOINT \"{}\"", self.name.replace('"', "\"\""));
        self.raw_pinned(&sql)
    }

    pub fn rollback(mut self) -> Fiber<'d, Dispatched<'d, I, S, E, ExtractUnit>> {
        self.finalised = true;
        let sql = format!(
            "ROLLBACK TO SAVEPOINT \"{}\"",
            self.name.replace('"', "\"\"")
        );
        self.raw_pinned(&sql)
    }

    pub fn savepoint(
        &self,
        name: impl Into<String>,
    ) -> Fiber<'d, impl Future<Output = Result<SavepointGuard<'d, I, S, E>, Error>>> {
        SavepointGuard::open(self.conn, self.pin, name.into())
    }
}

impl<'d, I, S, E> Drop for SavepointGuard<'d, I, S, E>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    fn drop(&mut self) {
        if !self.finalised {
            let sql = format!(
                "ROLLBACK TO SAVEPOINT \"{}\"",
                self.name.replace('"', "\"\"")
            );
            Disp::rollback_on_drop(self.conn, self.pin, &sql);
        }
    }
}

pub struct ListenGuard<'d, I, S, E>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    conn: PgHolding<'d, I, S, E>,
    pin: Token,
    channel: String,
    finalised: bool,
}

impl<'d, I, S, E> PgOps<'d, I, S, E> for ListenGuard<'d, I, S, E>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    fn holding(&self) -> PgHolding<'d, I, S, E> {
        self.conn
    }

    fn pin(&self) -> Option<Token> {
        Some(self.pin)
    }
}

impl<'d, I, S, E> ListenGuard<'d, I, S, E>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    pub(super) fn from_parts(conn: PgHolding<'d, I, S, E>, pin: Token, channel: String) -> Self {
        Self {
            conn,
            pin,
            channel,
            finalised: false,
        }
    }

    fn raw_pinned(&self, sql: &str) -> Fiber<'d, Dispatched<'d, I, S, E, ExtractUnit>> {
        Fiber::new(Disp::dispatch_raw::<ExtractUnit, I, S, E>(
            self.conn,
            Some(self.pin),
            Request::raw(sql),
        ))
    }

    pub fn channel(&self) -> &str {
        &self.channel
    }

    pub fn unlisten(mut self) -> Fiber<'d, Dispatched<'d, I, S, E, ExtractUnit>> {
        self.finalised = true;
        let sql = format!("UNLISTEN \"{}\"", self.channel.replace('"', "\"\""));
        self.raw_pinned(&sql)
    }
}

impl<'d, I, S, E> Drop for ListenGuard<'d, I, S, E>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    fn drop(&mut self) {
        if !self.finalised {
            let sql = format!("UNLISTEN \"{}\"", self.channel.replace('"', "\"\""));
            Disp::rollback_on_drop(self.conn, self.pin, &sql);
        }
    }
}
