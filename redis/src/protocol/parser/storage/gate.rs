use dope::{
    core::driver::{route, schedule::ready},
    manifold::client::conn,
};
use o3::{alloc, depot::queue::indexed::shared};
use std::{cell, mem, num};

use super::{Error, Finished, Storage, Tape};
use crate::protocol::parser::{self, limit};

struct Waiting<'a, 'd> {
    slot: shared::Slot<'a, ready::Target<'d>, cell::Cell<Option<ready::Target<'d>>>>,
    target: ready::Target<'d>,
}

enum Phase<'a, 'd> {
    Idle,
    Held,
    Waiting(Waiting<'a, 'd>),
}

/// One live charge of this exact gate, transferred from held access or a grant.
/// Clearing the previous holder before constructing this owner prevents double refunds.
#[doc(hidden)]
pub struct Lane<'a, 'd>(&'a Gate<'d>);

/// An in-progress tape whose nodes are already in their final retained storage.
pub(crate) struct Builder<'a, 'd> {
    storage: &'a Storage<'d>,
    phase: Phase<'a, 'd>,
    tail: Option<parser::Key<parser::Occupied>>,
    len: u32,
}

pub(in crate::protocol::parser) struct Stored<'a, 'b, 'd> {
    builder: &'a mut Builder<'b, 'd>,
    tail: parser::Key<parser::Occupied>,
    len: num::NonZero<u32>,
}

pub(super) struct Gate<'d> {
    active: cell::Cell<usize>,
    lanes: usize,
    waiters: shared::Fifo<ready::Target<'d>, cell::Cell<Option<ready::Target<'d>>>>,
}

impl<'d> Storage<'d> {
    pub(in crate::protocol::parser) fn builder(&self) -> Builder<'_, 'd> {
        Builder {
            storage: self,
            phase: Phase::Idle,
            tail: None,
            len: 0,
        }
    }
}

impl<'a, 'd> Builder<'a, 'd> {
    pub(in crate::protocol::parser) fn len(&self) -> u32 {
        self.len
    }

    pub(in crate::protocol::parser) fn try_push(
        &mut self,
        slot: limit::Slot,
        value: parser::Kind,
    ) -> Result<Stored<'_, 'a, 'd>, Error> {
        let storage = self.storage;
        let tail = match self.tail {
            Some(tail) => storage.tapes.append(tail, &value),
            None => {
                if !self.acquire() {
                    return Err(Error::Blocked(slot));
                }
                storage.tapes.first(&value)
            }
        };
        let Some(tail) = tail else {
            return Err(Error::Full(slot));
        };
        let len = slot.commit();
        self.tail = Some(tail);
        self.len = len.get();
        Ok(Stored {
            builder: self,
            tail,
            len,
        })
    }

    pub(in crate::protocol::parser) fn reset(&mut self) {
        *self = self.storage.builder();
    }

    fn acquire(&mut self) -> bool {
        match &self.phase {
            Phase::Idle => {
                let gate = &self.storage.gate;
                let active = gate.active.get();
                if active >= gate.lanes {
                    return false;
                }
                gate.active.set(active + 1);
            }
            Phase::Waiting(waiting) if !self.storage.gate.resume(waiting) => return false,
            Phase::Waiting(_) => {}
            Phase::Held => return true,
        }
        self.phase = Phase::Held;
        true
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
}

impl Drop for Builder<'_, '_> {
    fn drop(&mut self) {
        self.storage.tapes.release(self.tail);
        let phase = mem::replace(&mut self.phase, Phase::Idle);
        match phase {
            Phase::Idle => {}
            Phase::Held => drop(Lane(&self.storage.gate)),
            Phase::Waiting(waiting) => self.storage.gate.cancel(waiting),
        }
    }
}

impl<'a, 'g> Stored<'_, 'a, 'g> {
    pub(in crate::protocol::parser) fn finish(self) -> Finished<'a, 'g> {
        self.builder.tail = None;
        self.builder.len = 0;
        self.builder.phase = Phase::Idle;
        let storage = self.builder.storage;
        Finished {
            nodes: Tape {
                pool: &storage.tapes,
                tail: self.tail,
                len: self.len,
            },
            lane: Lane(&storage.gate),
        }
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
