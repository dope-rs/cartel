use crate::port;
use crate::port::replies;
use crate::reply::group::Ledger;
use crate::{api, reply, schema};
use api::task::lease::{Lease, Recover};
use api::{
    ops, request,
    results::copy::Chunk,
    traits::{self, sealed},
};
use dope::core::driver::{route::KeyTag, schedule::ready};
use dope::manifold::client::conn;
use dope_fiber::{abi, context};
use port::pool::{access, backend};
use std::task;

pub mod error;
use error::Error;
use error::Failure;

/// A backend cancellation capability borrowed from one live transaction.
///
/// The token cannot escape the guard that supplied its connection epoch:
///
pub struct CancelToken<'tx, 'd: 'tx, const ID: u8 = 0> {
    pub(super) lifecycle: &'tx port::Lifecycle<'d, ID>,
    pub(super) backend: backend::Bound<'tx, 'd, ID>,
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
            lifecycle: self.lifecycle,
            backend: self.backend,
            wake: self.wake,
            pending: false,
        }
    }
}

#[must_use = "a cancellation request does nothing unless it is driven"]
pub struct Cancel<'tx, 'd: 'tx, const ID: u8 = 0> {
    lifecycle: &'tx port::Lifecycle<'d, ID>,
    backend: backend::Bound<'tx, 'd, ID>,
    wake: ready::Target<'d>,
    pending: bool,
}

impl<const ID: u8> Unpin for Cancel<'_, '_, ID> {}

impl<'tx, 'd: 'tx, const ID: u8> abi::Fiber<'d> for Cancel<'tx, 'd, ID> {
    type Output = Result<(), Error<'tx, 'd, ID>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, cx) = call.into_parts();
        let this = this.get_mut();
        let waiter = cx.as_ref().completion();
        if !this.pending {
            let intent = match this.backend.try_current(&this.lifecycle.buffer) {
                Ok(intent) => intent,
                Err(error) => {
                    return task::Poll::Ready(Err(Error {
                        failure: Failure::Backend(error),
                    }));
                }
            };
            if let Err(error) = this.lifecycle.cancel.try_reserve(intent, waiter) {
                return task::Poll::Ready(Err(Error {
                    failure: Failure::Reserve {
                        backend: this.backend,
                        cause: error,
                    },
                }));
            }
            this.pending = true;
            this.wake.wake();
            return task::Poll::Pending;
        }
        match this.lifecycle.cancel.poll(this.backend.target(), waiter) {
            task::Poll::Ready(result) => {
                this.pending = false;
                task::Poll::Ready(result.map_err(|cause| Error {
                    failure: Failure::Delivery {
                        backend: this.backend,
                        cause,
                    },
                }))
            }
            task::Poll::Pending => task::Poll::Pending,
        }
    }
}

impl<const ID: u8> Drop for Cancel<'_, '_, ID> {
    fn drop(&mut self) {
        if self.pending {
            self.lifecycle.cancel.abandon(self.backend.target());
            self.wake.wake();
        }
    }
}

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
    ) -> impl abi::Fiber<'d, Output = Result<(), reply::Error<'d, ID>>>
    + use<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID> {
        ops::Query::from_parts(
            self.lease.client(),
            api::Target::from_tx(self.lease.target()),
        )
        .raw(request)
    }
}

impl<B: ?Sized, I: schema::QuerySet, const ID: u8> sealed::Sealed
    for Savepoint<'_, '_, '_, '_, '_, '_, '_, B, I, ID>
{
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    traits::Connection<'d, ID> for Savepoint<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    type Schema = I;
    type Payload = replies::Payload<'b, 'c, 'd>;
    type Diagnostics = Ledger<'r, 's>;
    type CopyChunk = Chunk<'b, 'c, 'd>;
    type Query<'save>
        = ops::Query<api::Context<'save, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>, api::Target<'d, ID>>
    where
        Self: 'save;

    fn query(&self) -> Self::Query<'_> {
        ops::Query::from_parts(
            self.lease.reborrow(),
            api::Target::from_tx(self.lease.target()),
        )
    }
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    traits::Savepoint<'d, ID> for Savepoint<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    type Savepoint<'parent>
        = Savepoint<'parent, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
    where
        Self: 'parent;

    fn name(&self) -> &str {
        &self.name
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
        let opening = ops::Query::from_parts(client, api::Target::from_tx(target))
            .stateful(request::Request::savepoint(&name));
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
    ) -> impl abi::Fiber<'d, Output = Result<(), reply::Error<'d, ID>>>
    + use<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID> {
        ops::Query::from_parts(self.lease.client(), self.lease.target()).raw(request)
    }
}

impl<B: ?Sized, I: schema::QuerySet, const ID: u8> sealed::Sealed
    for ListenGuard<'_, '_, '_, '_, '_, '_, '_, B, I, ID>
{
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    traits::Connection<'d, ID> for ListenGuard<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    type Schema = I;
    type Payload = replies::Payload<'b, 'c, 'd>;
    type Diagnostics = Ledger<'r, 's>;
    type CopyChunk = Chunk<'b, 'c, 'd>;
    type Query<'listen>
        = ops::Query<api::Context<'listen, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>, api::Target<'d, ID>>
    where
        Self: 'listen;

    fn query(&self) -> Self::Query<'_> {
        ops::Query::from_parts(self.lease.reborrow(), self.lease.target())
    }
}

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    traits::Listener<'d, ID> for ListenGuard<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    fn channel(&self) -> &str {
        &self.channel
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
