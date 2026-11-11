//! Statically dispatched operations retaining their exact client and reply owners.

use crate::{api, protocol, reply, reply::Error, schema};
use api::tx::control;
use api::{results, sql, tx};
use dope_fiber::abi::Fiber;
use results::{group, source};
use schema::result;
use std::ops::Deref;

pub mod copy;
pub(crate) mod sealed;

use copy::Transfer;
use sealed::Sealed;

/// One exact runtime-issued client, with its storage details carried by its type.
pub trait Client<'d, const ID: u8 = 0>: Notify<'d, ID> {
    type Transaction: Transaction<
            'd,
            ID,
            Schema = Self::Schema,
            Payload = Self::Payload,
            Diagnostics = Self::Diagnostics,
            CopyChunk = Self::Chunk,
        >;

    fn transaction(self) -> impl Begin<'d, ID, Guard = Self::Transaction>;
    fn status(self) -> impl Status<'d, ID>;
}

pub trait IntoQuery<'d, const ID: u8 = 0>: Sized {
    type Query;

    fn into_query(self) -> Self::Query;
}

impl<'a, 'd, T: Connection<'d, ID>, const ID: u8> IntoQuery<'d, ID> for &'a T {
    type Query = T::Query<'a>;

    fn into_query(self) -> Self::Query {
        self.query()
    }
}

/// A query view whose result owners retain the admitted storage and route.
pub trait Query<'d, const ID: u8 = 0>:
    Sealed + IntoQuery<'d, ID, Query = Self> + Unchecked<'d, ID> + Transfer<'d, ID>
{
    type Acquired: Query<
            'd,
            ID,
            Schema = Self::Schema,
            Payload = Self::Payload,
            Batch = Self::Batch,
            Diagnostics = Self::Diagnostics,
        >;

    fn acquire(self) -> impl Fiber<'d, Output = Result<Self::Acquired, Error<'d, ID>>>
    where
        Self: Copy;
    type Schema: schema::QuerySet + 'd;
    type Payload: source::Payload;
    type Batch: source::Batch;
    type Diagnostics: group::Diagnostics;
    type Stream<Q>: Stream<'d, Q, ID, Payload = Self::Payload>
    where
        Q: schema::TypedQuery<Result = result::Rows>;

    fn run_one<Q>(
        self,
        params: Q::Params<'_>,
    ) -> impl Fiber<'d, Output = Result<results::Row<Self::Payload, Q>, Error<'d, ID>>>
    + use<'d, Self, Q, ID>
    where
        Q: schema::TypedQuery<Result = result::Rows>,
        Self::Schema: schema::HasGroup<Q::Group>,
        Q::Group: schema::QueryGroup;

    /// Encodes a nonempty group now and runs it with one connection, Sync, and deadline.
    /// Success waits for ReadyForQuery; later commands may be skipped after an error.
    /// Group synchronization does not promise transaction atomicity.
    fn run_group<Q, const N: usize>(
        self,
        params: [Q::Params<'_>; N],
    ) -> impl Fiber<
        'd,
        Output = results::GroupResult<'d, results::Row<Self::Payload, Q>, Self::Diagnostics, N, ID>,
    > + use<'d, Self, Q, ID, N>
    where
        Q: schema::TypedQuery<Result = result::Rows>,
        Self::Schema: schema::HasGroup<Q::Group>,
        Q::Group: schema::QueryGroup;

    fn run_first<Q>(
        self,
        params: Q::Params<'_>,
    ) -> impl Fiber<'d, Output = results::FirstResult<'d, Self::Payload, Q, ID>> + use<'d, Self, Q, ID>
    where
        Q: schema::TypedQuery<Result = result::Rows>,
        Self::Schema: schema::HasGroup<Q::Group>,
        Q::Group: schema::QueryGroup;

    fn run_scalar<Q>(
        self,
        params: Q::Params<'_>,
    ) -> impl Fiber<'d, Output = results::FirstResult<'d, Self::Payload, Q, ID, true>>
    + use<'d, Self, Q, ID>
    where
        Q: schema::TypedQuery<Result = result::Rows<true>>,
        Self::Schema: schema::HasGroup<Q::Group>,
        Q::Group: schema::QueryGroup;

    fn run_all<Q>(
        self,
        params: Q::Params<'_>,
    ) -> impl Fiber<'d, Output = Result<results::Collection<Self::Batch, Q>, Error<'d, ID>>>
    + use<'d, Self, Q, ID>
    where
        Q: schema::TypedQuery<Result = result::Rows>,
        Self::Schema: schema::HasGroup<Q::Group>,
        Q::Group: schema::QueryGroup;

    fn run_no_rows<Q>(
        self,
        params: Q::Params<'_>,
    ) -> impl Fiber<'d, Output = Result<(), Error<'d, ID>>> + use<'d, Self, Q, ID>
    where
        Q: schema::TypedQuery<Row = (), Result = result::Command>,
        Self::Schema: schema::HasGroup<Q::Group>,
        Q::Group: schema::QueryGroup;

    fn run_stream<Q>(self, params: Q::Params<'_>) -> <Self as Query<'d, ID>>::Stream<Q>
    where
        Q: schema::TypedQuery<Result = result::Rows>,
        Self::Schema: schema::HasGroup<Q::Group>,
        Q::Group: schema::QueryGroup;
}

/// Unchecked SQL on the exact connection and owner carried by this view.
pub trait Unchecked<'d, const ID: u8 = 0>: Sealed + Sized {
    /// Encodes the SQL before returning the future.
    fn command(
        self,
        sql: &str,
    ) -> impl Fiber<'d, Output = Result<(), Error<'d, ID>>> + use<'d, Self, ID>;
}

/// Streaming rows retain their payload independently of the next-row borrow.
pub trait Stream<'d, Q: schema::TypedQuery, const ID: u8 = 0>: Sealed {
    type Payload: source::Payload;

    fn next_row(
        &mut self,
    ) -> impl Fiber<'d, Output = Result<Option<results::Row<Self::Payload, Q>>, Error<'d, ID>>> + '_;
}

pub trait Begin<'d, const ID: u8 = 0>: Sealed + Sized {
    type Guard: Transaction<'d, ID>;

    fn isolation(self, level: tx::Isolation) -> Self;
    fn read_only(self) -> Self;
    fn read_write(self) -> Self;
    fn deferrable(self) -> Self;
    fn timeout(self, time: sql::Timeout) -> Self;
    fn begin(self) -> impl Fiber<'d, Output = Result<Self::Guard, Error<'d, ID>>>;
}

pub trait Connection<'d, const ID: u8 = 0>: Sealed {
    type Schema: schema::QuerySet + 'd;
    type Payload: source::Payload;
    type Diagnostics: group::Diagnostics;
    type CopyChunk: AsRef<[u8]> + Deref<Target = [u8]>;
    type Query<'query>: Notify<
            'd,
            ID,
            Schema = Self::Schema,
            Payload = Self::Payload,
            Diagnostics = Self::Diagnostics,
            Chunk = Self::CopyChunk,
        >
    where
        Self: 'query;

    fn query(&self) -> Self::Query<'_>;
}

/// Each query borrows its transaction; completed single rows retain their own owner.
pub trait Transaction<'d, const ID: u8 = 0>: Connection<'d, ID> + Sized {
    type Savepoint<'tx>: Savepoint<
            'd,
            ID,
            Schema = Self::Schema,
            Payload = Self::Payload,
            Diagnostics = Self::Diagnostics,
            CopyChunk = Self::CopyChunk,
        >
    where
        Self: 'tx;

    fn savepoint<'tx>(
        &'tx mut self,
        name: crate::Identifier,
    ) -> impl Fiber<'d, Output = Result<Self::Savepoint<'tx>, Error<'d, ID>>>;
    fn cancel_token(&self) -> Option<control::CancelToken<'_, 'd, ID>>;
    fn commit(self) -> impl Fiber<'d, Output = Result<(), Error<'d, ID>>>;
    fn rollback(self) -> impl Fiber<'d, Output = Result<(), Error<'d, ID>>>;
    fn finish<T, E>(
        self,
        outcome: Result<T, E>,
    ) -> impl Fiber<'d, Output = Result<T, tx::FinishError<E, Error<'d, ID>>>>;
    fn cancel(self);
}

/// A savepoint and every nested operation retain their exact parent lease.
pub trait Savepoint<'d, const ID: u8 = 0>: Connection<'d, ID> + Sized {
    type Savepoint<'parent>: Savepoint<
            'd,
            ID,
            Schema = Self::Schema,
            Payload = Self::Payload,
            Diagnostics = Self::Diagnostics,
            CopyChunk = Self::CopyChunk,
        >
    where
        Self: 'parent;

    fn name(&self) -> &str;
    fn cancel(self);
    fn release(self) -> impl Fiber<'d, Output = Result<(), Error<'d, ID>>>;
    fn rollback(self) -> impl Fiber<'d, Output = Result<(), Error<'d, ID>>>;
    fn savepoint<'parent>(
        &'parent mut self,
        name: crate::Identifier,
    ) -> impl Fiber<'d, Output = Result<Self::Savepoint<'parent>, Error<'d, ID>>>;
}

/// Notifications and listener admission on one exact client or listener view.
pub trait Notify<'d, const ID: u8 = 0>: Query<'d, ID> + Copy {
    type Listener: Listener<
            'd,
            ID,
            Schema = Self::Schema,
            Payload = Self::Payload,
            Diagnostics = Self::Diagnostics,
        >;

    fn dropped(self) -> u64;
    fn next(self) -> impl Fiber<'d, Output = Result<reply::Notice<'d>, reply::Error<'d, ID>>>;
    fn listen(
        self,
        channel: crate::Identifier,
    ) -> impl Fiber<'d, Output = Result<Self::Listener, reply::Error<'d, ID>>>;
}

/// A listener whose operation views keep its connection lease borrowed.
pub trait Listener<'d, const ID: u8 = 0>: Connection<'d, ID> + Sized {
    fn channel(&self) -> &str;
    fn unlisten(self) -> impl Fiber<'d, Output = Result<(), reply::Error<'d, ID>>>;
}

pub trait Status<'d, const ID: u8 = 0>: Sealed + Copy {
    fn is_failed(self) -> bool;
    fn is_ready(self) -> bool;
    fn live_count(self) -> usize;
    fn wait_ready(self) -> impl Fiber<'d, Output = Result<(), Error<'d, ID>>>;
    fn set_policy(self, policy: protocol::PickPolicy);
}
