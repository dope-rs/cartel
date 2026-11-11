use crate::{
    api,
    port::replies,
    reply::{Error, group::Ledger},
    schema,
};
use api::task::tx::{Begin, Held};
use api::{
    ops, request,
    results::copy::Chunk,
    sql,
    traits::{self, copy::Transfer, sealed},
};
use dope_fiber::abi;
use finish::{End, Finish};
use std::mem;

pub mod control;
mod finish;

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub enum Isolation {
    #[default]
    Default,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub enum AccessMode {
    #[default]
    Default,
    ReadWrite,
    ReadOnly,
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
#[repr(u8)]
pub(super) enum Deferral {
    #[default]
    Default,
    Deferred,
}

pub struct Builder<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, const ID: u8 = 0>
where
    I: schema::QuerySet + 'd,
{
    pub(super) client: api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
    pub(super) isolation: Isolation,
    pub(super) access_mode: AccessMode,
    pub(super) deferral: Deferral,
    pub(super) timeout: Option<sql::Timeout>,
}

impl<B: ?Sized, I: schema::QuerySet, const ID: u8> sealed::Sealed
    for Builder<'_, '_, '_, '_, '_, '_, '_, B, I, ID>
{
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    traits::Begin<'d, ID> for Builder<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    type Guard = Guard<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>;

    fn isolation(mut self, level: Isolation) -> Self {
        self.isolation = level;
        self
    }

    fn read_only(mut self) -> Self {
        self.access_mode = AccessMode::ReadOnly;
        self
    }

    fn read_write(mut self) -> Self {
        self.access_mode = AccessMode::ReadWrite;
        self
    }

    fn deferrable(mut self) -> Self {
        self.deferral = Deferral::Deferred;
        self
    }

    fn timeout(mut self, time: sql::Timeout) -> Self {
        self.timeout = Some(time);
        self
    }

    fn begin(self) -> impl abi::Fiber<'d, Output = Result<Self::Guard, Error<'d, ID>>> {
        let limit = self.timeout;
        let client = self.client;
        let level = self.isolation;
        let access = self.access_mode;
        let deferral = self.deferral;
        dope_fiber::fiber!('d => async move {
            let lease = Begin::new(client, request::Begin::new(level, access, deferral)).await?;
            let target = lease.target();
            if let Some(time) = limit {
                let setting = ops::Query::from_parts(client, api::Target::from_tx(target)).raw(request::Request::timeout(time));
                setting.await?;
            }
            Ok(Guard { lease })
        })
    }
}

/// An exclusive transaction lease. Every operation view borrows this guard,
/// so a pinned query cannot survive commit, rollback, or guard drop.
pub struct Guard<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, const ID: u8 = 0>
where
    I: schema::QuerySet + 'd,
{
    lease: Held<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
}

/// The operation or transaction finalization failed, retaining both causes
/// when rollback also fails.
#[derive(Debug)]
pub enum FinishError<E, F> {
    Operation(E),
    Commit(F),
    Rollback { operation: E, rollback: F },
}

const _: () = assert!(mem::size_of::<Deferral>() == mem::size_of::<bool>());
const _: () = assert!(mem::align_of::<Deferral>() == mem::align_of::<bool>());

impl<B: ?Sized, I: schema::QuerySet, const ID: u8> sealed::Sealed
    for Guard<'_, '_, '_, '_, '_, '_, '_, B, I, ID>
{
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    traits::Transaction<'d, ID> for Guard<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    type Schema = I;
    type Payload = replies::Payload<'b, 'c, 'd>;
    type Diagnostics = Ledger<'r, 's>;
    type CopyChunk = Chunk<'b, 'c, 'd>;
    type Query<'tx>
        = ops::Query<api::Client<'tx, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>, api::Target<'d, ID>>
    where
        Self: 'tx;
    type Savepoint<'tx>
        = control::Savepoint<'tx, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
    where
        Self: 'tx;

    fn commit(self) -> impl abi::Fiber<'d, Output = Result<(), Error<'d, ID>>> {
        Finish::new(self.lease, End::Commit)
    }

    fn rollback(self) -> impl abi::Fiber<'d, Output = Result<(), Error<'d, ID>>> {
        Finish::new(self.lease, End::Rollback)
    }

    /// Commits success or rolls back failure after the operation releases its guard borrow.
    /// Shares `commit` and `rollback` settlement and cancellation, retaining both errors
    /// when an unsuccessful operation is followed by an unsuccessful rollback.
    fn finish<T, E>(
        self,
        outcome: Result<T, E>,
    ) -> impl abi::Fiber<'d, Output = Result<T, FinishError<E, Error<'d, ID>>>> {
        let end = match &outcome {
            Ok(_) => End::Commit,
            Err(_) => End::Rollback,
        };
        let finish = Finish::new(self.lease, end);
        dope_fiber::fiber!('d => async move {
            match (outcome, finish.await) {
                (Ok(value), Ok(())) => Ok(value),
                (Ok(_), Err(error)) => Err(FinishError::Commit(error)),
                (Err(error), Ok(())) => Err(FinishError::Operation(error)),
                (Err(operation), Err(rollback)) => {
                    Err(FinishError::Rollback { operation, rollback })
                }
            }
        })
    }

    fn cancel(self) {
        drop(self.lease);
    }

    fn query(&self) -> Self::Query<'_> {
        ops::Query::from_parts(
            self.lease.reborrow(),
            api::Target::from_tx(self.lease.target()),
        )
    }

    fn unchecked(&self) -> impl traits::Unchecked<'d, ID> + '_ {
        self.query()
    }

    fn copy(&self) -> impl Transfer<'d, ID, Chunk = Self::CopyChunk> + '_ {
        self.query()
    }

    fn savepoint<'tx>(
        &'tx mut self,
        name: crate::Identifier,
    ) -> impl abi::Fiber<'d, Output = Result<Self::Savepoint<'tx>, Error<'d, ID>>> {
        let client = self.lease.reborrow();
        let target = self.lease.target();
        dope_fiber::fiber!('d => async move {
            let lease = ops::Query::from_parts(client, api::Target::from_tx(target)).stateful(request::Request::savepoint(&name))
            .await?
            .retarget(target)?;
            Ok(control::Savepoint {
                lease,
                name,
            })
        })
    }

    fn cancel_token(&self) -> Option<control::CancelToken<'_, 'd, ID>> {
        let client = self.lease.client();
        let target = self.lease.target().conn;
        let (backend, pid) = client.port.connection().cancel(target)?;
        let wake = client.port.lifecycle.cancel.ready_target()?;
        Some(control::CancelToken {
            lifecycle: &client.port.lifecycle,
            backend,
            pid,
            wake,
        })
    }
}
