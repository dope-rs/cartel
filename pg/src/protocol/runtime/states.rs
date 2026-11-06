use crate::{
    protocol::runtime::waiters,
    reply::{self, server},
};
use dope::core::driver::route;
use dope::manifold::client::attempt;
use dope::net::link::pool::change::opening;
use std::cell;

pub(super) enum Failure<'d, const ID: u8> {
    Driver(crate::Error),
    Open(reply::Open<'d, ID>),
    Server(server::Diagnostic<'d>),
}

pub(crate) struct Signals<'d, const ID: u8> {
    pub(super) cause: cell::Cell<Option<Failure<'d, ID>>>,
    pub(super) failed: cell::Cell<bool>,
    pub(crate) egress: waiters::Egress<'d>,
}

impl<'d, const ID: u8> Signals<'d, ID> {
    pub(crate) fn is_failed(&self) -> bool {
        self.failed.get()
    }

    /// Taking the cause only copies a driver value or retains local diagnostic
    /// storage. Neither operation calls application code, wakes a waiter, or
    /// destroys the original diagnostic before it is restored.
    pub(crate) fn failure(&self) -> Option<reply::Error<'d, ID>> {
        if !self.is_failed() {
            return None;
        }
        let cause = self.cause.take();
        let failure = match cause.as_ref() {
            Some(Failure::Driver(error)) => Some(reply::Error::Driver(*error)),
            Some(Failure::Open(cause)) => Some(reply::Error::Open(*cause)),
            Some(Failure::Server(error)) => Some(match error.try_clone() {
                Ok(error) => reply::Error::Server(error),
                Err(error) => reply::Error::Driver(crate::Error::ResponseRetain(error)),
            }),
            None => None,
        };
        self.cause.set(cause);
        failure
    }

    pub(crate) fn clear_open(&self) {
        let cause = self.cause.take();
        if matches!(cause, Some(Failure::Open(_))) {
            self.failed.set(false);
        } else {
            self.cause.set(cause);
        }
    }

    pub(crate) fn record_fatal(&self, error: crate::Error) {
        self.record_cause(Failure::Driver(error));
    }

    pub(in crate::protocol) fn record_reply(&self, error: server::Diagnostic<'d>) {
        self.record_cause(Failure::Server(error));
    }

    /// Restore the retained cause before destroying a rejected failure, so a
    /// diagnostic release cannot observe a transient empty cause.
    fn record_cause(&self, failure: Failure<'d, ID>) {
        let cause = self.cause.take();
        if cause.is_none() || matches!(cause.as_ref(), Some(Failure::Open(_))) {
            self.failed.set(true);
            self.cause.set(Some(failure));
        } else {
            self.cause.set(cause);
        }
    }

    pub(in crate::protocol) fn exhaust(
        &self,
        key: attempt::Id<'d, route::KeyTag<ID>>,
        cause: opening::Exhaustion,
    ) {
        self.record_cause(Failure::Open(reply::Open::Exhaustion { key, cause }));
    }

    pub(in crate::protocol) fn failed(
        &self,
        key: attempt::Id<'d, route::KeyTag<ID>>,
        cause: attempt::ConnectFault,
    ) {
        self.record_cause(Failure::Open(reply::Open::Transport { key, cause }));
    }
}
