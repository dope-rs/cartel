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
    fn access(
        &self,
        id: access::Transaction<'d, ID>,
    ) -> Option<(&'a slot::Slot<'d, ID>, access::State)> {
        let slot = self.shared.entry(id.conn.index().into_usize())?.slot();
        Some((slot, slot.access(id.conn)?))
    }

    pub(crate) fn held(&self, id: access::Transaction<'d, ID>) -> Option<&'a slot::Slot<'d, ID>> {
        let (slot, state) = self.access(id)?;
        (state == access::State::Held(id.generation)).then_some(slot)
    }

    pub(crate) fn is_current<const FINAL: bool>(&self, id: access::Transaction<'d, ID>) -> bool {
        match self.access(id) {
            Some((_, access::State::Finalizing(current))) => FINAL && current == id.generation,
            Some((_, access::State::Reserved(current) | access::State::Held(current))) => {
                !FINAL && current == id.generation
            }
            _ => false,
        }
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
        match self.access(target) {
            Some((
                _,
                access::State::Reserved(current)
                | access::State::Held(current)
                | access::State::Finalizing(current),
            )) if current == target.generation => {}
            Some(_) => return Poll::Ready(Ok(poll::Disposition::Reusable)),
            None => return Poll::Ready(Ok(poll::Disposition::Lost)),
        }
        let queue: &'a _ = &self.shared.transactions;
        queue.try_register(target, waiter, ctx)?;
        Poll::Pending
    }

    pub(crate) fn begin_finish(
        &self,
        id: access::Transaction<'d, ID>,
    ) -> access::Finalization<'d, ID> {
        self.shared.finalize(id)
    }
}

impl<'d, I: crate::schema::QuerySet, const ID: u8>
    crate::port::Port<'_, '_, '_, '_, '_, '_, 'd, I, ID>
{
    pub(crate) fn poll_idle<'target>(
        &'target self,
        target: access::Transaction<'d, ID>,
        waiter: pin::Pin<&wait::Waiter<'target, 'd>>,
        mut cx: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> Poll<Result<(), crate::reply::Error<'d, ID>>> {
        let slot = self
            .shared
            .transaction()
            .held(target)
            .ok_or(crate::Error::Closed)?;
        if slot.barriers() == 0
            && self
                .lane(target.conn.lane())
                .depth(cx.as_mut().region_token())
                .get()
                == 0
        {
            return Poll::Ready(Ok(()));
        }
        self.shared
            .transactions
            .try_register(target, waiter, cx.as_ref())
            .map_err(crate::reply::Error::Wait)?;
        Poll::Pending
    }
}
