use crate::port;
use crate::port::{Storage, replies};
use crate::reply::group::Ledger;
use crate::{api, reply, schema};
use api::task::{
    Call,
    lease::{Lease, Recover},
};
use api::{
    ops,
    request::{self, stateful},
    results::copy::Chunk,
    traits::{self, copy::Transfer, sealed},
};
use dope::core::driver::{route::KeyTag, schedule::ready};
use dope::manifold::client::{conn, side};
use dope_fiber::{abi, context};
use port::pool::{access, backend};
use side::registry;
use std::task;

pub mod error;
use error::Error;
use error::Failure;

/// A backend cancellation capability borrowed from one live transaction.
///
/// The token cannot escape the guard that supplied its connection epoch:
///
pub struct CancelToken<'tx, 'd: 'tx, const ID: u8 = 0> {
    pub(super) port: &'tx registry::Registry<'d, port::Backend<'d, ID>, KeyTag<ID>>,
    pub(super) backend: backend::Bound<'tx, 'd, ID>,
    pub(super) buffer: &'tx Storage,
    pub(super) pid: i32,
    pub(super) wake: ready::Target<'d>,
}

impl<'tx, 'd: 'tx, const ID: u8> CancelToken<'tx, 'd, ID> {
    pub fn target(&self) -> conn::Id<'d, KeyTag<ID>> {
        self.backend.target()
    }

    pub fn pid(&self) -> i32 {
        self.pid
    }

    /// Sends the cancel packet on a fresh connection and settles after write.
    /// PostgreSQL sends no response; observe the query for server-side result.
    pub fn cancel(&self) -> Cancel<'tx, 'd, ID> {
        Cancel {
            port: self.port,
            backend: self.backend,
            buffer: self.buffer,
            wake: self.wake,
            phase: Phase::New,
        }
    }
}

#[derive(Clone, Copy)]
#[repr(u8)]
enum Phase {
    New,
    Pending,
    Done,
}

#[must_use = "a cancellation request does nothing unless it is driven"]
pub struct Cancel<'tx, 'd: 'tx, const ID: u8 = 0> {
    port: &'tx registry::Registry<'d, port::Backend<'d, ID>, KeyTag<ID>>,
    backend: backend::Bound<'tx, 'd, ID>,
    buffer: &'tx Storage,
    wake: ready::Target<'d>,
    phase: Phase,
}

impl<const ID: u8> Unpin for Cancel<'_, '_, ID> {}

impl<'tx, 'd: 'tx, const ID: u8> abi::Fiber<'d> for Cancel<'tx, 'd, ID> {
    type Output = Result<(), Error<'tx, 'd, ID>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, cx) = call.into_parts();
        let this = this.get_mut();
        let waiter = cx.as_ref().completion();
        match this.phase {
            Phase::New => {
                let intent = match this.backend.try_current(this.buffer) {
                    Ok(intent) => intent,
                    Err(error) => {
                        this.phase = Phase::Done;
                        return task::Poll::Ready(Err(Error {
                            failure: Failure::Backend(error),
                        }));
                    }
                };
                if let Err(error) = this.port.try_reserve(intent, waiter) {
                    this.phase = Phase::Done;
                    return task::Poll::Ready(Err(Error {
                        failure: Failure::Reserve {
                            backend: this.backend,
                            cause: error,
                        },
                    }));
                }
                this.phase = Phase::Pending;
                this.wake.wake();
                task::Poll::Pending
            }
            Phase::Pending => match this.port.poll(this.backend.target(), waiter) {
                task::Poll::Ready(result) => {
                    this.phase = Phase::Done;
                    task::Poll::Ready(result.map_err(|cause| Error {
                        failure: Failure::Delivery {
                            backend: this.backend,
                            cause,
                        },
                    }))
                }
                task::Poll::Pending => task::Poll::Pending,
            },
            Phase::Done => task::Poll::Pending,
        }
    }
}

impl<const ID: u8> Drop for Cancel<'_, '_, ID> {
    fn drop(&mut self) {
        match self.phase {
            Phase::Pending => {
                self.port.abandon(self.backend.target());
                self.wake.wake();
            }
            Phase::New | Phase::Done => {}
        }
    }
}

const _: () = assert!(std::mem::size_of::<Phase>() == std::mem::size_of::<bool>());
const _: () = assert!(std::mem::align_of::<Phase>() == std::mem::align_of::<bool>());

/// A savepoint lease whose operation views cannot outlive the savepoint.
/// Call [`Self::release`] or [`Self::rollback`] to preserve the transaction.
/// Dropping an unfinished lease quarantines its connection for full rollback.
///
#[must_use = "release or roll back the savepoint to preserve its transaction"]
pub struct Savepoint<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, const ID: u8 = 0>
where
    I: schema::QuerySet + 'd,
{
    pub(super) lease:
        Lease<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, access::Transaction<'d, ID>, Recover, ID>,
    pub(super) name: crate::Identifier,
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    Savepoint<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    fn dispatch(
        &self,
        request: request::Request<'_>,
    ) -> Call<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, api::ExtractUnit<ID>, ID> {
        self.lease
            .client()
            .raw::<api::ExtractUnit<ID>>(Some(api::Target::from_tx(self.lease.target())), request)
    }
}

impl<B: ?Sized, I: schema::QuerySet, const ID: u8> sealed::Sealed
    for Savepoint<'_, '_, '_, '_, '_, '_, '_, B, I, ID>
{
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    traits::Savepoint<'d, ID> for Savepoint<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    type Schema = I;
    type Payload = replies::Payload<'b, 'c, 'd>;
    type Diagnostics = Ledger<'r, 's>;
    type CopyChunk = Chunk<'b, 'c, 'd>;
    type Query<'save>
        = ops::Query<'save, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
    where
        Self: 'save;
    type Savepoint<'parent>
        = Savepoint<'parent, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
    where
        Self: 'parent;

    fn name(&self) -> &str {
        &self.name
    }

    fn query(&self) -> Self::Query<'_> {
        ops::Query::from_parts(
            self.lease.reborrow(),
            Some(api::Target::from_tx(self.lease.target())),
        )
    }

    fn unchecked(&self) -> impl traits::Unchecked<'d, ID> + '_ {
        ops::Unchecked(self.query())
    }

    fn copy(&self) -> impl Transfer<'d, ID, Chunk = Self::CopyChunk> + '_ {
        ops::Transfer(self.query())
    }

    fn cancel(self) {
        self.lease.quarantine();
    }

    fn release(self) -> impl abi::Fiber<'d, Output = Result<(), reply::Error<'d, ID>>> {
        let closing = self.dispatch(request::Request::release(&self.name));
        dope_fiber::fiber!('d => async move {
            closing.await?;
            self.lease.settle();
            Ok(())
        })
    }

    fn rollback(self) -> impl abi::Fiber<'d, Output = Result<(), reply::Error<'d, ID>>> {
        let rollback = self.dispatch(request::Request::rollback(&self.name));
        dope_fiber::fiber!('d => async move {
            rollback.await?;
            self.dispatch(request::Request::release(&self.name))
                .await?;
            self.lease.settle();
            Ok(())
        })
    }

    fn savepoint<'parent>(
        &'parent mut self,
        name: crate::Identifier,
    ) -> impl abi::Fiber<'d, Output = Result<Self::Savepoint<'parent>, reply::Error<'d, ID>>> {
        let client = self.lease.reborrow();
        let target = self.lease.target();
        let opening = stateful::Stateful::new(
            client,
            Some(api::Target::from_tx(target)),
            request::Request::savepoint(&name),
        );
        dope_fiber::fiber!('d => async move {
            let lease = opening.await?.retarget(target)?;
            Ok(Savepoint {
                lease,
                name,
            })
        })
    }
}

/// A listener lease whose pinned operation views cannot outlive the listener.
///
pub struct ListenGuard<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I, const ID: u8 = 0>
where
    I: schema::QuerySet + 'd,
{
    lease: Lease<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, api::Target<'d, ID>, Recover, ID>,
    channel: crate::Identifier,
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    ListenGuard<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    pub(in crate::api) fn from_parts(
        lease: Lease<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, api::Target<'d, ID>, Recover, ID>,
        channel: crate::Identifier,
    ) -> Self {
        Self { lease, channel }
    }

    fn dispatch(
        &self,
        request: request::Request<'_>,
    ) -> Call<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, api::ExtractUnit<ID>, ID> {
        self.lease
            .client()
            .raw::<api::ExtractUnit<ID>>(Some(self.lease.target()), request)
    }
}

impl<B: ?Sized, I: schema::QuerySet, const ID: u8> sealed::Sealed
    for ListenGuard<'_, '_, '_, '_, '_, '_, '_, B, I, ID>
{
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    traits::Listener<'d, ID> for ListenGuard<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    type Schema = I;
    type Payload = replies::Payload<'b, 'c, 'd>;
    type Diagnostics = Ledger<'r, 's>;
    type Query<'listen>
        = ops::Query<'listen, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
    where
        Self: 'listen;

    fn channel(&self) -> &str {
        &self.channel
    }

    fn query(&self) -> Self::Query<'_> {
        ops::Query::from_parts(self.lease.reborrow(), Some(self.lease.target()))
    }

    fn unchecked(&self) -> impl traits::Unchecked<'d, ID> + '_ {
        ops::Unchecked(self.query())
    }

    fn notify(
        &self,
    ) -> impl traits::Notify<
        'd,
        ID,
        Schema = Self::Schema,
        Payload = Self::Payload,
        Diagnostics = Self::Diagnostics,
    > + '_ {
        ops::Notify(self.query())
    }

    fn unlisten(self) -> impl abi::Fiber<'d, Output = Result<(), reply::Error<'d, ID>>> {
        let closing = self.dispatch(request::Request::unlisten(&self.channel));
        dope_fiber::fiber!('d => async move {
            closing.await?;
            self.lease.settle();
            Ok(())
        })
    }
}
