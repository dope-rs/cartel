//! PostgreSQL client capabilities issued by an actual protocol session.
//!
//! A driver-owned port cannot mint a client directly:
//!

use dope::core::driver::route::KeyTag;
use dope_fiber::reply::{Batch, Batches};

use crate::port::{pool::access, replies};
use crate::protocol::{self, runtime::tx::Access};
use crate::reply;
use crate::schema;
use cartel_core::reply::recv::outcome::Terminal;
use cartel_core::reply::{
    LaneIndex, Slot, layout,
    recv::{self, attach},
};
use dope::manifold::client::conn;
use dope_fiber::wait;
use o3::{depot::capacity, mem::credit::fair};
use std::mem;

use request::Pending;
use task::lease;

/// Exact owner-bound destination of one request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Target<'d, const ID: u8> {
    identity: Identity<'d, ID>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Identity<'d, const ID: u8> {
    Connection(conn::Id<'d, KeyTag<ID>>),
    Transaction(access::Transaction<'d, ID>),
}

impl<'d, const ID: u8> Target<'d, ID> {
    pub(crate) const fn new(conn: conn::Id<'d, KeyTag<ID>>) -> Self {
        Self {
            identity: Identity::Connection(conn),
        }
    }

    pub(crate) const fn from_tx(tx: access::Transaction<'d, ID>) -> Self {
        Self {
            identity: Identity::Transaction(tx),
        }
    }

    pub(crate) const fn transaction(self) -> Option<access::Transaction<'d, ID>> {
        match self.identity {
            Identity::Connection(_) => None,
            Identity::Transaction(tx) => Some(tx),
        }
    }
}

impl<'d, const ID: u8> lease::Target<'d, ID> for Target<'d, ID> {
    fn connection(self) -> conn::Id<'d, KeyTag<ID>> {
        match self.identity {
            Identity::Connection(conn) => conn,
            Identity::Transaction(tx) => tx.conn,
        }
    }

    fn current(self, tx: &Access<'_, 'd, ID>) -> Option<conn::Id<'d, KeyTag<ID>>> {
        match self.identity {
            Identity::Connection(conn) => Some(conn),
            Identity::Transaction(target) => lease::Target::current(target, tx),
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

use task::tx::Held;

type Response<'app, 'g, 'r, 's, 'b, 'c, 'd, X> = recv::Response<
    'app,
    'b,
    'c,
    'd,
    replies::Row<'d>,
    X,
    replies::ReplyFailure<'d>,
    replies::Tag<'g, 'r, 's, 'b, 'c, 'd>,
>;

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
        match slot.try_lease() {
            Ok(Some(item)) => return Some(Ok(replies::Payload::from_lease(item))),
            Ok(None) => {}
            Err(error) => return Some(Err(crate::Error::ResponseRetain(error).into())),
        }
        if let Some(failure) = slot.pop_error() {
            return Some(Err(failure.into_error()));
        }
        None
    }
}

pub(super) struct ExtractFirst<const ID: u8 = 0>;

impl<'app, 'b, 'c: 'b, 'd: 'c, const ID: u8>
    recv::Extract<'app, 'b, 'c, 'd, replies::Row<'d>, replies::ReplyFailure<'d>>
    for ExtractFirst<ID>
{
    type Output = Result<Option<replies::Payload<'b, 'c, 'd>>, reply::Error<'d, ID>>;

    const ACCEPTANCE: recv::Acceptance = recv::Acceptance::Collected;

    type Context = ();

    fn extract(
        _: &Self::Context,
        slot: &mut Slot<'_, 'app, 'b, 'c, 'd, replies::Row<'d>, replies::ReplyFailure<'d>>,
    ) -> Option<Self::Output> {
        match slot.terminal()? {
            Terminal::Failed(failure) => Some(Err(failure.into_error().into_error())),
            Terminal::Complete(mut done) => Some(
                done.try_lease()
                    .map(|payload| payload.map(replies::Payload::from_lease))
                    .map_err(|error| crate::Error::ResponseRetain(error).into()),
            ),
        }
    }
}

pub(in crate::api) trait Sync {
    const BOUNDARY: Boundary = Boundary::Close;
}

impl<const ID: u8> Sync for ExtractAll<ID> {}
impl<const ID: u8> Sync for ExtractUnit<ID> {}
impl<const ID: u8> Sync for ExtractFirst<ID> {}

impl<const ID: u8> Sync for ExtractOne<ID> {
    const BOUNDARY: Boundary = Boundary::Open;
}

#[repr(u8)]
pub(in crate::api) enum Boundary {
    Close,
    Open,
}

pub(super) struct Throttle<'d, const ID: u8, S = ()> {
    request: Pending<'d, S>,
    conn: conn::Id<'d, KeyTag<ID>>,
    cause: Pressure,
}

pub(super) enum Pressure {
    Full {
        len: capacity::Count,
        capacity: layout::Entries,
    },
    Capacity {
        lane: LaneIndex,
        axis: fair::Axis<2>,
        required: usize,
        available: usize,
    },
}

impl<'d, const ID: u8, S> Throttle<'d, ID, S> {
    pub(in crate::api) fn new(
        conn: conn::Id<'d, KeyTag<ID>>,
        error: crate::Error,
        request: Pending<'d, S>,
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
        Ok(Self {
            request,
            conn,
            cause,
        })
    }

    fn fail(self, error: wait::Error<'d>) -> reply::Error<'d, ID> {
        reply::Error::Admission {
            conn: self.conn,
            cause: self.cause.attach(),
            error,
        }
    }

    fn release(self) -> Throttle<'d, ID> {
        let Self {
            request,
            conn,
            cause,
        } = self;
        Throttle {
            request: Pending {
                frame: request.frame,
                boundary: request.boundary,
                state: (),
            },
            conn,
            cause,
        }
    }
}

impl Pressure {
    fn attach(self) -> attach::Error {
        match self {
            Self::Full { len, capacity } => attach::Error::Full { len, capacity },
            Self::Capacity {
                lane,
                axis,
                required,
                available: avail,
            } => attach::Error::Capacity {
                lane,
                cause: fair::Cause::Exhausted {
                    axis,
                    required,
                    available: avail,
                },
            },
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Pressure>() == 4 * mem::size_of::<usize>());
    assert!(mem::align_of::<Pressure>() == mem::align_of::<usize>());
    assert!(
        mem::size_of::<Pressure>() + mem::size_of::<usize>() == mem::size_of::<attach::Error>()
    );
};

enum TxOutcome<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, const ID: u8>
where
    I: schema::QuerySet + 'd,
{
    Enqueued {
        lease: Held<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
    },
    Throttled {
        throttle: Throttle<'d, ID>,
    },
    NoConn {
        request: Pending<'d>,
    },
    Failed(reply::Error<'d, ID>),
}

enum TxRetry<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, const ID: u8>
where
    I: schema::QuerySet + 'd,
{
    Waiting(Pending<'d>),
    Enqueued {
        lease: Held<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>,
    },
    Failed(reply::Error<'d, ID>),
}

const _: () = assert!(
    mem::size_of::<Target<'static, 0>>() == mem::size_of::<access::Transaction<'static, 0>>()
);
const _: () = assert!(
    mem::align_of::<Target<'static, 0>>() == mem::align_of::<access::Transaction<'static, 0>>()
);
const _: () = assert!(
    mem::size_of::<Option<Target<'static, 0>>>() == mem::size_of::<(Target<'static, 0>, u8)>()
);
const _: () = assert!(mem::size_of::<Boundary>() == mem::size_of::<bool>());
const _: () = assert!(mem::align_of::<Boundary>() == mem::align_of::<bool>());
