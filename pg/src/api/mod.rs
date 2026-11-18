//! PostgreSQL client capabilities issued by an actual protocol session.
//!
//! A driver-owned port cannot mint a client directly:
//!

use dope::core::driver::route::KeyTag;
use dope_fiber::reply::Batch;

use crate::port::{pool::access, replies, replies::Response};
use crate::protocol::{self, runtime::tx::Access};
use crate::reply::{self, Pressure};
use crate::schema;
use cartel_core::reply::recv::outcome::Terminal;
use cartel_core::reply::{Slot, recv};
use dope::manifold::client::conn;
use dope_fiber::wait;
use std::{marker, mem};

use request::Pending;
use task::lease;

#[derive(Clone, Copy)]
pub struct Pooled;

impl<'d, const ID: u8> From<Pooled> for Option<Target<'d, ID>> {
    fn from(_: Pooled) -> Self {
        None
    }
}

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
pub(crate) mod task;
pub mod traits;
pub mod tx;

#[doc(hidden)]
pub use client::Context;
pub use ops::Query as Client;

fn check_open<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet, const ID: u8>(
    client: Context<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
) -> Result<(), reply::Error<'d, ID>> {
    client.port.shared.failure().map_or(Ok(()), Err)
}

pub(super) struct ExtractAll<Q, const ID: u8 = 0>(marker::PhantomData<Q>);

impl<'app, 'b: 'app, 'c: 'b, 'd: 'c, Q: schema::TypedQuery, const ID: u8>
    task::Read<'app, 'b, 'c, 'd> for ExtractAll<Q, ID>
{
    type Context = ();
    type Spec = recv::Collected;
    type Output = Result<
        results::Collection<Batch<'app, 'b, 'c, 'd, replies::Row<'d>>, Q>,
        reply::Error<'d, ID>,
    >;

    fn poll<K, T>(
        reply: std::pin::Pin<&mut task::Receiver<'app, 'b, 'c, 'd, Self::Spec, K>>,
        cx: std::pin::Pin<&mut dope_fiber::context::Context<'_, 'd>>,
        finish: impl FnOnce(Self::Output) -> T,
    ) -> std::task::Poll<T> {
        reply.poll_all(cx, |result| {
            finish(
                result
                    .map(results::Collection::new)
                    .map_err(replies::ReplyFailure::into_error),
            )
        })
    }
}

pub struct ExtractUnit<const ID: u8 = 0, const REQUIRED: bool = false>;

impl<'app, 'b, 'c, 'd, const ID: u8, const REQUIRED: bool>
    recv::Spec<'app, 'b, 'c, 'd, replies::Row<'d>, replies::ReplyFailure<'d>>
    for ExtractUnit<ID, REQUIRED>
{
    type Context = ();
    type Mode = ();
    const ACCEPTANCE: recv::Acceptance = recv::Acceptance::Unit;
}

impl<'app, 'b, 'c, 'd, const ID: u8, const REQUIRED: bool>
    recv::Extract<'app, 'b, 'c, 'd, replies::Row<'d>, replies::ReplyFailure<'d>>
    for ExtractUnit<ID, REQUIRED>
{
    type Output = Result<(), reply::Error<'d, ID>>;

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

impl<'app, 'b, 'c, 'd, const ID: u8>
    recv::Spec<'app, 'b, 'c, 'd, replies::Row<'d>, replies::ReplyFailure<'d>> for ExtractOne<ID>
{
    type Context = ();
    type Mode = ();
}

impl<'app, 'b, 'c: 'b, 'd: 'c, const ID: u8>
    recv::Extract<'app, 'b, 'c, 'd, replies::Row<'d>, replies::ReplyFailure<'d>>
    for ExtractOne<ID>
{
    type Output = Result<replies::Payload<'b, 'c, 'd>, reply::Error<'d, ID>>;

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

impl<'app, 'b, 'c, 'd, M: Unpin, const ID: u8>
    recv::Spec<'app, 'b, 'c, 'd, replies::Row<'d>, replies::ReplyFailure<'d>>
    for ExtractFirst<M, ID>
{
    type Context = M;
    type Mode = ();
    const ACCEPTANCE: recv::Acceptance = recv::Acceptance::First;
}

impl<'app, 'b, 'c: 'b, 'd: 'c, M, T, const ID: u8>
    recv::Extract<'app, 'b, 'c, 'd, replies::Row<'d>, replies::ReplyFailure<'d>>
    for ExtractFirst<M, ID>
where
    M: Unpin + Fn(Result<Option<replies::Payload<'b, 'c, 'd>>, reply::Error<'d, ID>>) -> T,
{
    type Output = T;

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
    const BOUNDARY: crate::port::requests::Boundary = crate::port::requests::Boundary::Close;
}

impl<Q, const ID: u8> Sync for ExtractAll<Q, ID> {}
impl<const ID: u8, const REQUIRED: bool> Sync for ExtractUnit<ID, REQUIRED> {
    const BOUNDARY: crate::port::requests::Boundary = if REQUIRED {
        crate::port::requests::Boundary::Required
    } else {
        crate::port::requests::Boundary::Close
    };
}
impl<M, const ID: u8> Sync for ExtractFirst<M, ID> {}

impl<const ID: u8> Sync for ExtractOne<ID> {
    const BOUNDARY: crate::port::requests::Boundary = crate::port::requests::Boundary::Open;
}

impl<X: Sync> From<marker::PhantomData<X>> for crate::port::requests::Boundary {
    fn from(_: marker::PhantomData<X>) -> Self {
        X::BOUNDARY
    }
}

impl Pressure {
    fn fail<'d, const ID: u8>(
        self,
        conn: conn::Id<'d, KeyTag<ID>>,
        error: wait::Error<'d>,
    ) -> reply::Error<'d, ID> {
        reply::Error::Admission {
            conn,
            cause: self,
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
