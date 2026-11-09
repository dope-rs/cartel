//! PostgreSQL client capabilities issued by an actual protocol session.
//!
//! A driver-owned port cannot mint a client directly:
//!

use dope::core::driver::route::KeyTag;
use dope_fiber::reply::{Batch, Batches};

use crate::port::{pool::access, replies, replies::Response};
use crate::protocol::{self, runtime::tx::Access};
use crate::reply::{self, Pressure};
use crate::schema;
use cartel_core::reply::recv::outcome::Terminal;
use cartel_core::reply::{
    Slot,
    recv::{self, attach},
};
use dope::manifold::client::conn;
use dope_fiber::wait;
use o3::mem::credit::fair;
use std::{marker, mem};

use request::Pending;
use task::lease;

/// Exact owner-bound destination of one request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Target<'d, const ID: u8> {
    conn: conn::Id<'d, KeyTag<ID>>,
    generation: Option<access::Generation>,
}

impl<'d, const ID: u8> Target<'d, ID> {
    pub(crate) const fn new(conn: conn::Id<'d, KeyTag<ID>>) -> Self {
        Self {
            conn,
            generation: None,
        }
    }

    pub(crate) const fn from_tx(tx: access::Transaction<'d, ID>) -> Self {
        Self {
            conn: tx.conn,
            generation: Some(tx.generation),
        }
    }

    pub(crate) const fn transaction(self) -> Option<access::Transaction<'d, ID>> {
        match self.generation {
            Some(generation) => Some(access::Transaction {
                conn: self.conn,
                generation,
            }),
            None => None,
        }
    }
}

impl<'d, const ID: u8> lease::Target<'d, ID> for Target<'d, ID> {
    fn connection(self) -> conn::Id<'d, KeyTag<ID>> {
        self.conn
    }

    fn current(self, tx: &Access<'_, 'd, ID>) -> Option<conn::Id<'d, KeyTag<ID>>> {
        match self.transaction() {
            Some(target) => lease::Target::current(target, tx),
            None => Some(self.conn),
        }
    }
}

mod client;
pub mod ops;
pub(super) mod request;
pub mod results;
pub mod sql;
pub mod task;
pub mod traits;
pub mod tx;

pub use client::Client;

fn check_open<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet, const ID: u8>(
    client: Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
) -> Result<(), reply::Error<'d, ID>> {
    client.port.shared.failure().map_or(Ok(()), Err)
}

pub(super) struct ExtractAll<const ID: u8 = 0>;

impl<'app, 'b: 'app, 'c: 'b, 'd: 'c, const ID: u8>
    recv::Extract<'app, 'b, 'c, 'd, replies::Row<'d>, replies::ReplyFailure<'d>>
    for ExtractAll<ID>
{
    type Output = Result<Batch<'app, 'b, 'c, 'd, replies::Row<'d>>, reply::Error<'d, ID>>;

    const ACCEPTANCE: recv::Acceptance = recv::Acceptance::Collected;

    type Context = &'app Batches<'b, 'c, 'd, replies::Row<'d>>;

    fn extract(
        batches: &Self::Context,
        slot: &mut Slot<'_, 'app, 'b, 'c, 'd, replies::Row<'d>, replies::ReplyFailure<'d>>,
    ) -> Option<Self::Output> {
        match slot.terminal()? {
            Terminal::Failed(failure) => Some(Err(failure.into_error().into_error())),
            Terminal::Complete(done) => Some(
                Batch::try_new(done, batches)
                    .map_err(|error| crate::Error::ResponseRetain(error).into()),
            ),
        }
    }
}

pub struct ExtractUnit<const ID: u8 = 0>;

impl<'app, 'b, 'c, 'd, const ID: u8>
    recv::Extract<'app, 'b, 'c, 'd, replies::Row<'d>, replies::ReplyFailure<'d>>
    for ExtractUnit<ID>
{
    type Output = Result<(), reply::Error<'d, ID>>;
    const ACCEPTANCE: recv::Acceptance = recv::Acceptance::Unit;

    type Context = ();

    fn extract(
        _: &Self::Context,
        slot: &mut Slot<'_, 'app, 'b, 'c, 'd, replies::Row<'d>, replies::ReplyFailure<'d>>,
    ) -> Option<Self::Output> {
        match slot.terminal()? {
            Terminal::Failed(failure) => Some(Err(failure.into_error().into_error())),
            Terminal::Complete(mut done) => match done.pop() {
                Some(_) => Some(Err(
                    crate::Error::Protocol(protocol::Reason::CommandData).into()
                )),
                None => Some(Ok(())),
            },
        }
    }
}

pub(super) struct ExtractOne<const ID: u8 = 0>;

impl<'app, 'b, 'c: 'b, 'd: 'c, const ID: u8>
    recv::Extract<'app, 'b, 'c, 'd, replies::Row<'d>, replies::ReplyFailure<'d>>
    for ExtractOne<ID>
{
    type Output = Result<replies::Payload<'b, 'c, 'd>, reply::Error<'d, ID>>;

    type Context = ();

    fn extract(
        _: &Self::Context,
        slot: &mut Slot<'_, 'app, 'b, 'c, 'd, replies::Row<'d>, replies::ReplyFailure<'d>>,
    ) -> Option<Self::Output> {
        match slot.try_node() {
            Ok(Some(item)) => return Some(Ok(replies::Payload::from_node(item))),
            Ok(None) => {}
            Err(error) => return Some(Err(crate::Error::ResponseRetain(error).into())),
        }
        if let Some(failure) = slot.pop_error() {
            return Some(Err(failure.into_error()));
        }
        None
    }
}

pub(super) struct ExtractFirst<M, const ID: u8 = 0>(marker::PhantomData<M>);

impl<'app, 'b, 'c: 'b, 'd: 'c, M, T, const ID: u8>
    recv::Extract<'app, 'b, 'c, 'd, replies::Row<'d>, replies::ReplyFailure<'d>>
    for ExtractFirst<M, ID>
where
    M: Unpin + Fn(Result<Option<replies::Payload<'b, 'c, 'd>>, reply::Error<'d, ID>>) -> T,
{
    type Output = T;

    const ACCEPTANCE: recv::Acceptance = recv::Acceptance::First;

    type Context = M;

    fn extract(
        map: &Self::Context,
        slot: &mut Slot<'_, 'app, 'b, 'c, 'd, replies::Row<'d>, replies::ReplyFailure<'d>>,
    ) -> Option<Self::Output> {
        Some(map(match slot.terminal()? {
            Terminal::Failed(failure) => Err(failure.into_error().into_error()),
            Terminal::Complete(mut done) => done
                .try_node()
                .map(|payload| payload.map(replies::Payload::from_node))
                .map_err(|error| crate::Error::ResponseRetain(error).into()),
        }))
    }
}

pub(in crate::api) trait Sync {
    const BOUNDARY: Boundary = Boundary::Close;
}

impl<const ID: u8> Sync for ExtractAll<ID> {}
impl<const ID: u8> Sync for ExtractUnit<ID> {}
impl<M, const ID: u8> Sync for ExtractFirst<M, ID> {}

impl<const ID: u8> Sync for ExtractOne<ID> {
    const BOUNDARY: Boundary = Boundary::Open;
}

#[repr(u8)]
pub(in crate::api) enum Boundary {
    Close,
    Open,
}

pub(super) struct Throttle<'d, const ID: u8> {
    conn: conn::Id<'d, KeyTag<ID>>,
    cause: Pressure,
}

impl<'d, const ID: u8> Throttle<'d, ID> {
    pub(in crate::api) fn new(
        conn: conn::Id<'d, KeyTag<ID>>,
        error: crate::Error,
    ) -> Result<Self, crate::Error> {
        let cause = match error {
            crate::Error::Reply(attach::Error::Full { len, capacity }) => {
                Pressure::Full { len, capacity }
            }
            crate::Error::Reply(attach::Error::Capacity {
                lane,
                cause:
                    fair::Cause::Exhausted {
                        axis,
                        required,
                        available: avail,
                    },
            }) => Pressure::Capacity {
                lane,
                axis,
                required,
                available: avail,
            },
            error => return Err(error),
        };
        Ok(Self { conn, cause })
    }

    fn fail(self, error: wait::Error<'d>) -> reply::Error<'d, ID> {
        reply::Error::Admission {
            conn: self.conn,
            cause: self.cause,
            error,
        }
    }
}

const _: () = assert!(
    mem::size_of::<Target<'static, 0>>() == mem::size_of::<access::Transaction<'static, 0>>()
);
const _: () = assert!(
    mem::align_of::<Target<'static, 0>>() == mem::align_of::<access::Transaction<'static, 0>>()
);
const _: () =
    assert!(mem::size_of::<Option<Target<'static, 0>>>() == mem::size_of::<Target<'static, 0>>());
const _: () = assert!(mem::size_of::<Boundary>() == mem::size_of::<bool>());
const _: () = assert!(mem::align_of::<Boundary>() == mem::align_of::<bool>());
