use crate::port::Disposition;
use crate::{
    port::{
        pool::{access, slot},
        replies,
    },
    protocol::runtime::ready,
};
use dope::core::driver::route;
use dope::manifold::client::conn;
use dope_fiber::context;
use dope_fiber::wait;
use std::{pin, task::Poll};

pub(crate) mod poll;

pub(crate) struct Access<'a, 'd, const ID: u8> {
    pub(super) shared: &'a super::Shared<'d, ID>,
}

impl<'a, 'd, const ID: u8> Access<'a, 'd, ID> {
    pub(crate) fn is_current<const FINAL: bool>(&self, id: access::Transaction<'d, ID>) -> bool {
        let Some(entry) = self.shared.ready.connection(id.conn) else {
            return false;
        };
        access::Access::new(entry).is_current::<FINAL>(id.generation)
    }

    pub(crate) fn current(
        &self,
        slot: conn::Id<'d, route::KeyTag<ID>>,
    ) -> access::Current<'a, 'd, ID> {
        self.shared.ready.current(slot)
    }

    pub(crate) fn reserve(
        &self,
        listed: ready::Listed<'a, 'd, ID>,
        load: impl FnOnce(conn::Id<'d, route::KeyTag<ID>>) -> Option<usize>,
    ) -> Option<ready::Reservation<'a, 'd, ID>> {
        let slot = slot::Ready::listed(&listed).connection();
        let state = listed.reserve();
        if state.is_none() && load(slot) == Some(0) {
            self.shared.wake_close(
                slot,
                replies::Failure::from_error(crate::Error::Closed),
                Disposition::Reconnect,
            );
        }
        state
    }

    pub(crate) fn release(&self, proof: ready::Reservation<'_, 'd, ID>) {
        self.shared.wake_tx(proof.release());
    }

    #[inline]
    pub(crate) fn poll_settled<'target>(
        &self,
        target: access::Transaction<'d, ID>,
        waiter: pin::Pin<&wait::Waiter<'target, 'd>>,
        ctx: pin::Pin<&context::Context<'_, 'd>>,
    ) -> Poll<Result<poll::Disposition, wait::Error<'d>>>
    where
        'a: 'target,
        'd: 'target,
    {
        match self.current(target.conn) {
            access::Current::Reserved(current) if current.id() == target => {}
            access::Current::Held(current) if current == target => {}
            access::Current::Finalizing(current) if current.id() == target => {}
            access::Current::Shared
            | access::Current::Reserved(_)
            | access::Current::Held(_)
            | access::Current::Finalizing(_)
            | access::Current::Draining => {
                return Poll::Ready(Ok(poll::Disposition::Reusable));
            }
            access::Current::Stale | access::Current::Handshaking | access::Current::Closing => {
                return Poll::Ready(Ok(poll::Disposition::Lost));
            }
        }
        let queue: &'a _ = &self.shared.transactions;
        queue.try_register(target, waiter, ctx)?;
        Poll::Pending
    }

    pub(crate) fn begin_finish(
        &self,
        id: access::Transaction<'d, ID>,
    ) -> access::Finalization<'d, ID> {
        self.shared.ready.finalize(id)
    }

    pub(in crate::protocol) fn poll_ready(
        &self,
        slot: conn::Id<'d, route::KeyTag<ID>>,
        status: poll::Status,
        load: usize,
    ) -> poll::Readiness {
        let (state, wake) = self.shared.ready.poll_ready(slot, status, load);
        if let Some(id) = wake {
            self.shared.wake_tx(id);
        }
        state
    }
}
