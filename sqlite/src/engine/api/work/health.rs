use crate::sys;
use std::{cell, mem};

/// Retains independently owned native and terminal cleanup failures in their delivery order.
pub(super) struct Health {
    mode: cell::Cell<Mode>,
    native: cell::Cell<Option<crate::Error>>,
    cleanup: cell::Cell<Option<crate::Error>>,
}

#[derive(Clone, Copy)]
enum Mode {
    Healthy,
    Recoverable,
    NativeFirst,
    CleanupFirst,
}

impl Health {
    pub(super) const fn new() -> Self {
        Self {
            mode: cell::Cell::new(Mode::Healthy),
            native: cell::Cell::new(None),
            cleanup: cell::Cell::new(None),
        }
    }

    /// A new owner can reopen an empty poisoned state; retained errors remain closed.
    pub(super) fn reset(&self) {
        let native = self.native.take();
        let cleanup = self.cleanup.take();
        if native.is_none() && cleanup.is_none() {
            self.mode.set(Mode::Healthy);
        }
        self.native.set(native);
        self.cleanup.set(cleanup);
    }

    pub(super) fn check(&self) -> Result<(), crate::Error> {
        match self.mode.get() {
            Mode::Healthy => Ok(()),
            Mode::Recoverable => {
                self.mode.set(Mode::Healthy);
                match self.native.take() {
                    Some(error) => Err(error),
                    None => Ok(()),
                }
            }
            Mode::NativeFirst => self.failed(&self.native, &self.cleanup),
            Mode::CleanupFirst => self.failed(&self.cleanup, &self.native),
        }
    }

    /// The consumed call guard owns this vacant slot until its reset has completed.
    pub(super) fn defer(&self, error: crate::Error) {
        self.native.set(Some(error));
        self.mode.set(Mode::Recoverable);
    }

    /// A consumed native error receipt belongs to the sole unre-wound statement.
    /// Fully reset statements cannot issue another receipt for a deferred reset.
    pub(super) fn finalized(&self, error: sys::Finalized) {
        self.native.set(Some(error.into_error()));
        match self.mode.get() {
            Mode::Healthy | Mode::Recoverable => self.mode.set(Mode::NativeFirst),
            Mode::NativeFirst | Mode::CleanupFirst => {}
        }
    }

    pub(super) fn fail(&self) {
        match self.mode.get() {
            Mode::Healthy => self.mode.set(Mode::CleanupFirst),
            Mode::Recoverable => self.mode.set(Mode::NativeFirst),
            Mode::NativeFirst | Mode::CleanupFirst => {}
        }
    }

    /// Only the consumed terminal cleanup receipt can publish this failure.
    pub(super) fn cleanup(&self, error: crate::Error) {
        self.cleanup.set(Some(error));
    }

    pub(super) fn take(&self) -> Option<(crate::Error, Option<crate::Error>)> {
        let order = self.mode.replace(Mode::NativeFirst);
        let native = self.native.take();
        let cleanup = self.cleanup.take();
        let (first, second) = match order {
            Mode::Healthy | Mode::Recoverable | Mode::NativeFirst => (native, cleanup),
            Mode::CleanupFirst => (cleanup, native),
        };
        match first {
            Some(first) => Some((first, second)),
            None => second.map(|second| (second, None)),
        }
    }

    fn failed(
        &self,
        first: &cell::Cell<Option<crate::Error>>,
        second: &cell::Cell<Option<crate::Error>>,
    ) -> Result<(), crate::Error> {
        Err(first
            .take()
            .or_else(|| second.take())
            .unwrap_or(crate::Error::ConnectionPoisoned))
    }
}

const _: () = assert!(mem::size_of::<Health>() == 7 * mem::size_of::<usize>());
