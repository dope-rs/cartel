use crate::limits;
use std::{cell, mem};

pub(crate) struct Meter {
    primary: Counter,
    mode: cell::Cell<Mode>,
    permission: cell::Cell<Permission>,
}

#[derive(Clone, Copy)]
pub(super) enum Permission {
    Denied,
    Granted,
}

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
enum Mode {
    Idle = 0,
    Primary = 2,
    Cleanup = 3,
}

pub(super) enum Tick {
    Continue,
    Stop,
}

pub(crate) enum Quota {
    Available,
    Exhausted,
}

#[repr(transparent)]
struct Counter(cell::Cell<u64>);

impl Meter {
    pub(super) const fn new() -> Self {
        Self {
            primary: Counter::new(),
            mode: cell::Cell::new(Mode::Idle),
            permission: cell::Cell::new(Permission::Denied),
        }
    }

    pub(super) fn reset(&self) {
        self.mode.set(Mode::Idle);
        self.permission.set(Permission::Denied);
    }

    pub(super) fn begin_call(&self, primary: limits::Progress) -> Result<(), crate::Error> {
        self.begin(Mode::Primary, primary)
    }

    pub(super) fn cleanup(&self, quota: limits::Progress) -> Result<(), crate::Error> {
        match self.mode.get() {
            Mode::Cleanup => Ok(()),
            Mode::Idle => self.begin(Mode::Cleanup, quota),
            Mode::Primary => Err(crate::Error::ReentrantQuery),
        }
    }

    pub(super) fn tick(&self) -> Tick {
        match self.mode.get() {
            Mode::Idle => Tick::Continue,
            Mode::Primary | Mode::Cleanup => self.primary.tick(),
        }
    }

    pub(super) fn call_quota(&self) -> Quota {
        self.primary.quota()
    }

    pub(super) fn finish(&self) {
        self.mode.set(Mode::Idle);
    }

    pub(super) fn stop(&self) {
        if matches!(self.mode.get(), Mode::Cleanup) {
            self.mode.set(Mode::Idle);
        }
    }

    pub(crate) fn run<T>(
        &self,
        op: impl FnOnce() -> Result<T, crate::Error>,
    ) -> Result<T, crate::Error> {
        match self.permission.replace(Permission::Granted) {
            Permission::Denied => {}
            Permission::Granted => return Err(crate::Error::ReentrantQuery),
        }
        let result = op();
        self.permission.set(Permission::Denied);
        result
    }

    pub(super) fn permission(&self) -> Permission {
        self.permission.get()
    }

    fn begin(&self, mode: Mode, primary: limits::Progress) -> Result<(), crate::Error> {
        if self.mode.get() != Mode::Idle {
            return Err(crate::Error::ReentrantQuery);
        }
        self.primary.start(primary);
        self.mode.set(mode);
        Ok(())
    }
}

impl Counter {
    const fn new() -> Self {
        Self(cell::Cell::new(0))
    }

    fn start(&self, quota: limits::Progress) {
        self.0.set(quota.get());
    }

    fn tick(&self) -> Tick {
        let left = self.0.get();
        if left == 0 {
            return Tick::Stop;
        }
        let left = left - 1;
        self.0.set(left);
        match left {
            0 => Tick::Stop,
            _ => Tick::Continue,
        }
    }

    fn quota(&self) -> Quota {
        match self.0.get() {
            0 => Quota::Exhausted,
            _ => Quota::Available,
        }
    }
}

const _: () = assert!(mem::size_of::<Meter>() <= 3 * mem::size_of::<u64>());
