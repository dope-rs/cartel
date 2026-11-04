use crate::{api, port::pool::access};
use cartel_core::reply;
use dope::core::driver::{self, route::table::conn, schedule::app};
use dope_fiber::wait::Capacity;
use dope_fiber::{context, wait};
use o3::alloc;
use o3::depot::queue::indexed::shared;
use std::{cell, mem, ops, pin};

fn index_error<'d>(cause: shared::Bounds) -> wait::Error<'d> {
    wait::Error::Index {
        index: wait::Index::new(cause.index),
        slots: wait::Slots::new(cause.capacity),
    }
}

pub(in crate::protocol) struct Queue<'d> {
    inner: wait::Queue<'d>,
    remaining: cell::Cell<usize>,
}

impl<'d> Queue<'d> {
    pub(in crate::protocol) fn new(driver: driver::Reference<'d>, capacity: Capacity) -> Self {
        Self {
            inner: wait::Queue::new(driver, capacity),
            remaining: cell::Cell::new(0),
        }
    }

    pub(in crate::protocol) fn wake(&self) {
        self.remaining.set(self.inner.len().get());
    }

    pub(in crate::protocol) fn flush(&self, work: app::Work<'_, 'd>) {
        let remaining = self.remaining.get();
        if remaining == 0 {
            return;
        }
        if self.inner.wake_one(work) == wait::WakeStatus::Complete {
            // Re-registering callers cannot extend this notification round.
            self.remaining
                .set((remaining - 1).min(self.inner.len().get()));
        }
    }

    pub(in crate::protocol) fn is_pending(&self) -> bool {
        self.remaining.get() != 0
    }
}

const _: () = assert!(
    mem::size_of::<Queue<'static>>()
        == mem::size_of::<wait::Queue<'static>>() + mem::size_of::<usize>()
);

impl<'d> ops::Deref for Queue<'d> {
    type Target = wait::Queue<'d>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub(in crate::protocol) struct Table<'d, const ID: u8> {
    slots: shared::Fifo<access::Transaction<'d, ID>, Registration<'d, ID>>,
    driver: driver::Reference<'d>,
}

#[derive(Default)]
struct Registration<'d, const ID: u8> {
    target: cell::Cell<Option<access::Transaction<'d, ID>>>,
    waiter: wait::Slot<'d>,
}

impl<'d, const ID: u8> Table<'d, ID> {
    pub(in crate::protocol) fn try_new(
        driver: driver::Reference<'d>,
        slots: conn::Capacity,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            slots: shared::Fifo::try_new(slots.entries().get())?,
            driver,
        })
    }

    pub(in crate::protocol) fn try_register<'target, 'poll>(
        &'target self,
        target: access::Transaction<'d, ID>,
        waiter: pin::Pin<&wait::Waiter<'target, 'd>>,
        context: pin::Pin<&context::Context<'poll, 'd>>,
    ) -> Result<(), wait::Error<'d>> {
        if !context.waker().belongs_to(self.driver) {
            return Err(wait::Error::Foreign);
        }
        let slot = self
            .slots
            .try_entry(target.conn.index().into_usize())
            .map_err(index_error)?;
        slot.state().waiter.try_register(waiter, context)?;
        slot.state().target.set(Some(target));
        Ok(())
    }

    pub(in crate::protocol) fn wake(
        &self,
        target: access::Transaction<'d, ID>,
    ) -> Result<(), shared::Bounds> {
        let slot = self.slots.try_entry(target.conn.index().into_usize())?;
        if slot.get::<0>() != Some(target) {
            slot.push::<0>(target);
        }
        Ok(())
    }

    pub(in crate::protocol) fn wake_all(&self) {
        for slot in self.slots.iter() {
            if let Some(target) = slot.state().target.get()
                && slot.get::<0>() != Some(target)
            {
                slot.push::<0>(target);
            }
        }
    }

    pub(in crate::protocol) fn flush(&self, work: app::Work<'_, 'd>) {
        let Some((slot, target)) = self.slots.next::<0>() else {
            return;
        };
        let state = slot.state();
        if state.target.get() != Some(target) {
            return;
        }
        match state.waiter.wake_one(work) {
            wait::WakeStatus::Complete => state.target.set(None),
            wait::WakeStatus::Pending => slot.push::<0>(target),
        }
    }

    pub(in crate::protocol) fn is_pending(&self) -> bool {
        !self.slots.is_empty::<0>()
    }
}

pub(crate) struct Egress<'d> {
    full: Queue<'d>,
    lanes: shared::Fifo<(), Queue<'d>>,
}

impl<'d> Egress<'d> {
    pub(in crate::protocol) fn try_new(
        driver: driver::Reference<'d>,
        slots: conn::Capacity,
        capacity: Capacity,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            full: Queue::new(driver, capacity),
            lanes: shared::Fifo::try_with(slots.entries().get(), || Queue::new(driver, capacity))?,
        })
    }

    pub(crate) fn try_register<'target, 'poll>(
        &'target self,
        cause: &api::Pressure,
        waiter: pin::Pin<&wait::Waiter<'target, 'd>>,
        context: pin::Pin<&context::Context<'poll, 'd>>,
    ) -> Result<(), wait::Error<'d>> {
        match cause {
            api::Pressure::Full { .. } => self.full.try_register(waiter, context),
            api::Pressure::Capacity { lane, .. } => {
                let index = lane.get();
                let queue = self.lanes.try_entry(index).map_err(index_error)?;
                queue.state().try_register(waiter, context)
            }
        }
    }

    pub(in crate::protocol) fn release(
        &self,
        released: reply::Released<'_>,
    ) -> Result<(), shared::Bounds> {
        let lane = released.index();
        let slot = self.lanes.try_entry(lane.get())?;
        self.full.wake();
        Self::wake_lane(slot);
        Ok(())
    }

    fn flush(&self, work: app::Work<'_, 'd>) {
        self.full.flush(work);
        if let Some((slot, ())) = self.lanes.next::<0>() {
            let queue = slot.state();
            queue.flush(work);
            if queue.is_pending() {
                slot.push::<0>(());
            }
        }
    }

    fn is_pending(&self) -> bool {
        self.full.is_pending() || !self.lanes.is_empty::<0>()
    }

    fn wake_all(&self) {
        self.full.wake();
        for slot in self.lanes.iter() {
            Self::wake_lane(slot);
        }
    }

    fn wake_lane(slot: shared::Slot<'_, (), Queue<'d>>) {
        let queue = slot.state();
        queue.wake();
        if queue.is_pending() && slot.get::<0>().is_none() {
            slot.push::<0>(());
        }
    }
}

const _: () = assert!(
    mem::size_of::<Egress<'static>>()
        == mem::size_of::<Queue<'static>>() + mem::size_of::<shared::Fifo<(), Queue<'static>>>()
);

pub(crate) struct Set<'a, 'd, const ID: u8> {
    ready: &'a Queue<'d>,
    transactions: &'a Table<'d, ID>,
    notifications: &'a Queue<'d>,
    egress: &'a Egress<'d>,
}

impl<'a, 'd, const ID: u8> Set<'a, 'd, ID> {
    pub(in crate::protocol) const fn new(
        ready: &'a Queue<'d>,
        tx: &'a Table<'d, ID>,
        notices: &'a Queue<'d>,
        egress: &'a Egress<'d>,
    ) -> Self {
        Self {
            ready,
            transactions: tx,
            notifications: notices,
            egress,
        }
    }

    pub(in crate::protocol) fn flush(&self, work: app::Work<'_, 'd>) {
        self.ready.flush(work);
        self.transactions.flush(work);
        self.notifications.flush(work);
        self.egress.flush(work);
    }

    pub(in crate::protocol) fn is_pending(&self) -> bool {
        self.ready.is_pending()
            || self.transactions.is_pending()
            || self.notifications.is_pending()
            || self.egress.is_pending()
    }

    pub(crate) fn wake_all(&self) {
        self.ready.wake();
        self.transactions.wake_all();
        self.notifications.wake();
        self.egress.wake_all();
    }
}
