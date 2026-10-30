use dope::{
    core::driver::{route, schedule::ready},
    manifold::client::conn,
};
use o3::{alloc, depot::queue::indexed::shared};
use std::{cell, marker, mem};

use crate::protocol::parser::storage::Storage;

struct Waiting<'a, 'd> {
    slot: shared::Slot<'a, ready::Target<'d>, cell::Cell<Option<ready::Target<'d>>>>,
    target: ready::Target<'d>,
}

enum Phase<'a, 'd> {
    Idle,
    Held,
    Waiting(Waiting<'a, 'd>),
}

pub(super) struct Held<'a, 'd>(marker::PhantomData<fn(&'a mut Storage<'d>) -> &'a mut Storage<'d>>);

/// One live charge of this exact gate, transferred from held access or a grant.
/// Clearing the previous holder before constructing this owner prevents double refunds.
#[doc(hidden)]
pub struct Lane<'a, 'd>(&'a Gate<'d>);

pub(super) enum Acquire<'a, 'd> {
    Acquired(Held<'a, 'd>),
    Blocked,
}

pub(crate) struct Access<'a, 'd> {
    storage: &'a Storage<'d>,
    phase: Phase<'a, 'd>,
}

pub(super) struct Gate<'d> {
    active: cell::Cell<usize>,
    lanes: usize,
    waiters: shared::Fifo<ready::Target<'d>, cell::Cell<Option<ready::Target<'d>>>>,
}

impl<'a, 'd> Access<'a, 'd> {
    pub(super) const fn new(storage: &'a Storage<'d>) -> Self {
        Self {
            storage,
            phase: Phase::Idle,
        }
    }

    pub(super) const fn storage(&self) -> &'a Storage<'d> {
        self.storage
    }

    pub(super) fn acquire(&mut self) -> Acquire<'a, 'd> {
        if let Phase::Idle = &self.phase {
            let gate = &self.storage.gate;
            let active = gate.active.get();
            if active >= gate.lanes {
                return Acquire::Blocked;
            }
            gate.active.set(active + 1);
        } else if let Phase::Waiting(waiting) = &self.phase
            && !self.storage.gate.resume(waiting)
        {
            return Acquire::Blocked;
        }
        self.phase = Phase::Held;
        Acquire::Acquired(Held(marker::PhantomData))
    }

    pub(crate) fn wait<const ID: u8>(
        &mut self,
        conn: conn::Id<'d, route::KeyTag<ID>>,
        target: ready::Target<'d>,
    ) -> Result<(), shared::Bounds> {
        let index = conn.index().into_usize();
        match &self.phase {
            Phase::Held => return Ok(()),
            Phase::Waiting(waiting)
                if waiting.slot.index() == index && waiting.target == target =>
            {
                return Ok(());
            }
            Phase::Idle | Phase::Waiting(_) => {}
        }
        let slot = self.storage.gate.waiters.try_entry(index)?;
        if slot.get::<0>() != Some(target) {
            slot.push::<0>(target);
        }
        let next = Waiting { slot, target };
        if let Phase::Waiting(waiting) = mem::replace(&mut self.phase, Phase::Waiting(next)) {
            self.storage.gate.cancel(waiting);
        }
        Ok(())
    }

    pub(super) fn release(&mut self) {
        let phase = mem::replace(&mut self.phase, Phase::Idle);
        match phase {
            Phase::Idle => {}
            Phase::Held => drop(Lane(&self.storage.gate)),
            Phase::Waiting(waiting) => self.storage.gate.cancel(waiting),
        }
    }

    pub(super) fn finish(&mut self, held: Held<'a, 'd>) -> Lane<'a, 'd> {
        let Held(marker::PhantomData) = held;
        self.phase = Phase::Idle;
        Lane(&self.storage.gate)
    }
}

impl<'d> Gate<'d> {
    pub(super) fn try_new(conns: usize, lanes: usize) -> Result<Self, alloc::Error> {
        Ok(Self {
            active: cell::Cell::new(0),
            lanes,
            waiters: shared::Fifo::try_new(conns)?,
        })
    }

    fn resume(&self, waiting: &Waiting<'_, 'd>) -> bool {
        let granted = waiting.slot.state();
        if granted.get() == Some(waiting.target) {
            granted.set(None);
            return true;
        }
        let active = self.active.get();
        if active >= self.lanes {
            return false;
        }
        if waiting.slot.get::<0>() == Some(waiting.target) {
            waiting.slot.clear::<0>();
        }
        self.active.set(active + 1);
        true
    }

    fn cancel(&self, waiting: Waiting<'_, 'd>) {
        if waiting.slot.get::<0>() == Some(waiting.target) {
            waiting.slot.clear::<0>();
        }
        let granted = waiting.slot.state();
        if granted.get() == Some(waiting.target) {
            granted.set(None);
            drop(Lane(self));
        }
    }
}

impl Drop for Lane<'_, '_> {
    fn drop(&mut self) {
        let gate = self.0;
        let Some((slot, target)) = gate.waiters.next::<0>() else {
            gate.active.set(gate.active.get().wrapping_sub(1));
            return;
        };
        slot.state().set(Some(target));
        target.wake();
    }
}
