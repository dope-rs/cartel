use crate::{api::work, sys};
use std::mem;
use work::guard::{self, call};

#[derive(Clone, Copy)]
pub(crate) enum Authority {
    Plain,
    Control,
}

#[derive(Clone, Copy)]
pub(crate) enum Retry {
    Denied,
    Allowed,
}

pub(crate) trait Access {
    type Output<T>;

    const AUTHORIZATION: Authority;

    fn retry(&self) -> Retry;

    fn complete<T>(self, guard: &guard::Guard<'_, call::Call>, value: T) -> Self::Output<T>;

    fn busy(&self, fault: sys::Fault, tx: sys::Tx) -> crate::Error {
        match (tx, self.retry()) {
            (sys::Tx::Idle, _) | (sys::Tx::Active, Retry::Allowed) => fault.busy(),
            (sys::Tx::Active, Retry::Denied) => fault.transaction(),
        }
    }
}

pub(crate) struct Plain;

pub(crate) struct Control {
    retry: Retry,
}

impl Control {
    pub(super) const fn new(retry: Retry) -> Self {
        Self { retry }
    }
}

impl Access for Plain {
    type Output<T> = T;

    const AUTHORIZATION: Authority = Authority::Plain;

    fn retry(&self) -> Retry {
        Retry::Denied
    }

    fn complete<T>(self, _: &guard::Guard<'_, call::Call>, value: T) -> T {
        value
    }
}

impl Access for Control {
    type Output<T> = T;

    const AUTHORIZATION: Authority = Authority::Control;

    fn retry(&self) -> Retry {
        self.retry
    }

    fn complete<T>(self, _: &guard::Guard<'_, call::Call>, value: T) -> T {
        value
    }
}

impl Access for work::Permit {
    type Output<T> = work::Count;

    const AUTHORIZATION: Authority = Authority::Control;

    fn retry(&self) -> Retry {
        Retry::Denied
    }

    fn complete<T>(self, guard: &guard::Guard<'_, call::Call>, _: T) -> work::Count {
        guard.work().depth.open(self)
    }
}

const _: () = {
    assert!(mem::size_of::<Plain>() == 0);
    assert!(mem::size_of::<Control>() == mem::size_of::<Retry>());
    assert!(mem::size_of::<Retry>() == mem::size_of::<bool>());
};
