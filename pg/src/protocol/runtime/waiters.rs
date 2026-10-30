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
    phase: cell::Cell<Phase>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
enum Phase {
    Idle,
    Pending,
}

impl<'d> Queue<'d> {
    pub(in crate::protocol) fn new(driver: driver::Reference<'d>, capacity: Capacity) -> Self {
        Self {
            inner: wait::Queue::new(driver, capacity),
            phase: cell::Cell::new(Phase::Idle),
        }
    }

    pub(in crate::protocol) fn wake(&self) {
        self.phase.set(Phase::Pending);
    }

    pub(in crate::protocol) fn flush(&self, work: app::Work<'_, 'd>) {
        if self.phase.replace(Phase::Idle) == Phase::Idle {
            return;
        }
        match self.inner.wake_one(work) {
            wait::WakeStatus::Complete if self.inner.len() != wait::Length::ZERO => {
                self.phase.set(Phase::Pending);
            }
            wait::WakeStatus::Complete => {}
            wait::WakeStatus::Pending => self.phase.set(Phase::Pending),
        }
    }

    pub(in crate::protocol) fn is_pending(&self) -> bool {
        self.phase.get() == Phase::Pending
    }
}

const _: () = {
    assert!(mem::size_of::<Phase>() == mem::size_of::<bool>());
    assert!(mem::align_of::<Phase>() == mem::align_of::<bool>());
    assert!(
        mem::size_of::<Queue<'static>>()
            == mem::size_of::<wait::Queue<'static>>() + mem::size_of::<usize>()
    );
};

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
    lanes: shared::Fifo<reply::LaneIndex, Queue<'d>>,
    scan: cell::Cell<Phase>,
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
            scan: cell::Cell::new(Phase::Idle),
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
        slot.state().wake();
        if slot.get::<0>().is_none() {
            slot.push::<0>(lane);
        }
        Ok(())
    }

    fn flush(&self, work: app::Work<'_, 'd>) {
        self.full.flush(work);
        if let Some((slot, lane)) = self.lanes.next::<0>() {
            let queue = slot.state();
            queue.flush(work);
            if queue.is_pending() {
                slot.push::<0>(lane);
            }
        }
        if self.scan.replace(Phase::Idle) == Phase::Pending {
            for slot in self.lanes.iter() {
                let queue = slot.state();
                queue.flush(work);
                if queue.is_pending() {
                    self.scan.set(Phase::Pending);
                }
            }
        }
    }

    fn is_pending(&self) -> bool {
        self.full.is_pending() || !self.lanes.is_empty::<0>() || self.scan.get() == Phase::Pending
    }

    fn wake_all(&self) {
        self.full.wake();
        for slot in self.lanes.iter() {
            slot.state().wake();
        }
        self.scan.set(Phase::Pending);
    }
}

const _: () = assert!(
    mem::size_of::<Egress<'static>>()
        == mem::size_of::<Queue<'static>>()
            + mem::size_of::<shared::Fifo<reply::LaneIndex, Queue<'static>>>()
            + mem::size_of::<usize>()
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
