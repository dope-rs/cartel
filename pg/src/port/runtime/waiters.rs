use crate::port::pool::access;
use dope::core::driver::{self, route::table::conn, schedule::app};
use dope_fiber::{context, wait};
use o3::alloc;
use o3::depot::queue::indexed::shared;
use std::{cell, pin};

fn index_error<'d>(cause: shared::Bounds) -> wait::Error<'d> {
    wait::Error::Index {
        index: wait::Index::new(cause.index),
        slots: wait::Slots::new(cause.capacity),
    }
}

pub(super) struct Table<'d, const ID: u8> {
    slots: shared::Fifo<access::Transaction<'d, ID>, Registration<'d, ID>>,
    driver: driver::Reference<'d>,
}

#[derive(Default)]
struct Registration<'d, const ID: u8> {
    target: cell::Cell<Option<access::Transaction<'d, ID>>>,
    waiter: wait::Slot<'d>,
}

impl<'d, const ID: u8> Table<'d, ID> {
    pub(super) fn try_new(
        driver: driver::Reference<'d>,
        slots: conn::Capacity,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            slots: shared::Fifo::try_new(slots.entries().get())?,
            driver,
        })
    }

    pub(super) fn try_register<'target, 'poll>(
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

    pub(super) fn wake(&self, target: access::Transaction<'d, ID>) -> Result<(), shared::Bounds> {
        let slot = self.slots.try_entry(target.conn.index().into_usize())?;
        if slot.get::<0>() != Some(target) {
            slot.push::<0>(target);
        }
        Ok(())
    }

    pub(super) fn wake_all(&self) {
        for slot in self.slots.iter() {
            if let Some(target) = slot.state().target.get()
                && slot.get::<0>() != Some(target)
            {
                slot.push::<0>(target);
            }
        }
    }

    pub(super) fn flush(&self, work: app::Work<'_, 'd>) {
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

    pub(super) fn is_pending(&self) -> bool {
        !self.slots.is_empty::<0>()
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum Step {
    Retire,
    Ready,
    Transaction,
    Notification,
    Admission,
    Frame,
}

impl Step {
    fn next(self) -> Self {
        match self {
            Self::Retire => Self::Ready,
            Self::Ready => Self::Transaction,
            Self::Transaction => Self::Notification,
            Self::Notification => Self::Admission,
            Self::Admission => Self::Frame,
            Self::Frame => Self::Retire,
        }
    }
}

pub(crate) struct Set<'a, 'd, const ID: u8> {
    next: &'a cell::Cell<Step>,
    ready: &'a wait::Notifier<'d>,
    transactions: &'a Table<'d, ID>,
    notifications: &'a wait::Notifier<'d>,
    egress: &'a wait::Notifier<'d>,
    frames: &'a wait::Notifier<'d>,
}

impl<'a, 'd, const ID: u8> Set<'a, 'd, ID> {
    pub(super) const fn new(
        next: &'a cell::Cell<Step>,
        ready: &'a wait::Notifier<'d>,
        tx: &'a Table<'d, ID>,
        notices: &'a wait::Notifier<'d>,
        egress: &'a wait::Notifier<'d>,
        frames: &'a wait::Notifier<'d>,
    ) -> Self {
        Self {
            next,
            ready,
            transactions: tx,
            notifications: notices,
            egress,
            frames,
        }
    }

    pub(crate) fn flush(&self, work: app::Work<'_, 'd>, mut retire: impl FnMut()) {
        // Each source consumes at most one credit. Rotate only a short turn.
        if work.remaining() >= 6 {
            retire();
            self.ready.flush(work);
            self.transactions.flush(work);
            self.notifications.flush(work);
            self.egress.flush(work);
            self.frames.flush(work);
            return;
        }
        let first = match work.remaining() {
            1..6 => {
                let first = self.next.get();
                self.next.set(first.next());
                first
            }
            _ => Step::Retire,
        };
        let mut step = first;
        loop {
            match step {
                Step::Retire => retire(),
                Step::Ready => self.ready.flush(work),
                Step::Transaction => self.transactions.flush(work),
                Step::Notification => self.notifications.flush(work),
                Step::Admission => self.egress.flush(work),
                Step::Frame => self.frames.flush(work),
            }
            step = step.next();
            if step == first {
                break;
            }
        }
    }

    pub(crate) fn is_pending(&self) -> bool {
        self.ready.is_pending()
            || self.transactions.is_pending()
            || self.notifications.is_pending()
            || self.egress.is_pending()
            || self.frames.is_pending()
    }

    pub(crate) fn wake_all(&self) {
        self.ready.notify();
        self.transactions.wake_all();
        self.notifications.notify();
        self.egress.notify();
        self.frames.notify();
    }
}
