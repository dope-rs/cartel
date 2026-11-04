use crate::port::Disposition;
use crate::port::{pool::access, replies};
use crate::protocol::runtime::ready;
use dope::core::driver::route;
use dope::manifold::client::conn;
use dope_fiber::context;
use dope_fiber::wait;
use std::pin;

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
        slot: conn::Id<'d, route::KeyTag<ID>>,
        load: usize,
    ) -> access::Reserve<'a, 'd, ID> {
        let state = self.shared.ready.reserve(slot);
        if matches!(state, access::Reserve::Exhausted) && load == 0 {
            self.shared.wake_close(
                slot,
                replies::Failure::from_error(crate::Error::Closed),
                Disposition::Reconnect,
            );
        }
        state
    }

    pub(crate) fn release(&self, proof: access::Reservation<'_, 'd, ID>, load: usize) {
        let id = proof.id();
        match self.shared.ready.release(proof, load) {
            access::Release::Released => {}
            access::Release::Stale => return,
        }
        self.shared.wake_tx(id);
    }

    pub(crate) fn settled(&self, target: access::Transaction<'d, ID>) -> poll::Settlement {
        match self.current(target.conn) {
            access::Current::Reserved(current) if current.id() == target => {
                poll::Settlement::Pending
            }
            access::Current::Held(current) if current == target => poll::Settlement::Pending,
            access::Current::Finalizing(current) if current.id() == target => {
                poll::Settlement::Pending
            }
            access::Current::Shared
            | access::Current::Reserved(_)
            | access::Current::Held(_)
            | access::Current::Finalizing(_)
            | access::Current::Draining => poll::Settlement::Complete(poll::Disposition::Reusable),
            access::Current::Stale | access::Current::Handshaking | access::Current::Closing => {
                poll::Settlement::Complete(poll::Disposition::Lost)
            }
        }
    }

    pub(crate) fn begin_finish(
        &self,
        id: access::Transaction<'d, ID>,
    ) -> access::Finalization<'d, ID> {
        self.shared.ready.finalize(id)
    }

    pub(crate) fn try_register<'target>(
        &self,
        target: access::Transaction<'d, ID>,
        waiter: pin::Pin<&wait::Waiter<'target, 'd>>,
        ctx: pin::Pin<&context::Context<'_, 'd>>,
    ) -> poll::Register<'d>
    where
        'a: 'target,
        'd: 'target,
    {
        match self.current(target.conn) {
            access::Current::Reserved(current) if current.id() == target => {}
            access::Current::Held(current) if current == target => {}
            access::Current::Finalizing(current) if current.id() == target => {}
            access::Current::Stale
            | access::Current::Handshaking
            | access::Current::Shared
            | access::Current::Reserved(_)
            | access::Current::Held(_)
            | access::Current::Finalizing(_)
            | access::Current::Draining
            | access::Current::Closing => return poll::Register::Stale,
        }
        let queue: &'a _ = &self.shared.transactions;
        match queue.try_register(target, waiter, ctx) {
            Ok(()) => poll::Register::Registered,
            Err(error) => poll::Register::Failed(error),
        }
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

    pub(crate) fn blocking(&self) -> ready::Blocking {
        self.shared.ready.blocking()
    }
}
