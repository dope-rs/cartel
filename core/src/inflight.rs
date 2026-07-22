use std::cell::Cell;

use crate::error::Error;

pub struct Inflight {
    capacity: Cell<usize>,
    len: Cell<usize>,
}

impl Inflight {
    pub const fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity: Cell::new(capacity),
            len: Cell::new(0),
        }
    }
    pub fn len(&self) -> usize {
        self.len.get()
    }

    pub fn is_empty(&self) -> bool {
        self.len.get() == 0
    }

    pub fn capacity(&self) -> usize {
        self.capacity.get()
    }

    pub fn check(&self) -> Result<(), Error> {
        if self.len.get() >= self.capacity.get() {
            return Err(Error::Backpressure {
                inflight: self.len.get(),
                queued: 0,
                cap: self.capacity.get(),
            });
        }
        Ok(())
    }

    pub fn inc(&self) {
        self.len.set(self.len.get().saturating_add(1));
    }

    pub fn dec(&self) {
        self.len.set(self.len.get().saturating_sub(1));
    }

    pub fn dec_n(&self, n: usize) {
        self.len.set(self.len.get().saturating_sub(n));
    }
}
