use crate::reply::{self, server};
use dope::core::driver::route;
use dope::manifold::client::attempt;
use dope::net::link::pool::change::opening;
use dope_fiber::wait;
use std::cell;

pub(super) enum Failure<'d, const ID: u8> {
    Driver(crate::Error),
    Open(reply::Open<'d, ID>),
    Server(server::Diagnostic<'d>),
}

pub(crate) struct Signals<'d, const ID: u8> {
    pub(super) cause: cell::Cell<Option<Failure<'d, ID>>>,
    pub(crate) egress: wait::Notifier<'d>,
}

impl<'d, const ID: u8> Signals<'d, ID> {
    pub(crate) fn is_failed(&self) -> bool {
        // SAFETY: this observation neither mutates the cell nor calls user code.
        unsafe { &*self.cause.as_ptr() }.is_some()
    }

    pub(crate) fn failure(&self) -> Option<reply::Error<'d, ID>> {
        // SAFETY: copying errors and retaining the local diagnostic neither mutate
        // this cell nor call user code. No borrowed cause escapes this match.
        match unsafe { &*self.cause.as_ptr() } {
            Some(Failure::Driver(error)) => Some(reply::Error::Driver(*error)),
            Some(Failure::Open(cause)) => Some(reply::Error::Open(*cause)),
            Some(Failure::Server(error)) => Some(match error.try_clone() {
                Ok(error) => reply::Error::Server(error),
                Err(error) => reply::Error::Driver(crate::Error::ResponseRetain(error)),
            }),
            None => None,
        }
    }

    pub(crate) fn clear_open(&self) {
        // SAFETY: this observation ends before replacement and calls no user code.
        let clear = matches!(unsafe { &*self.cause.as_ptr() }, Some(Failure::Open(_)));
        if clear {
            // Open is Copy, so the replaced value needs no destruction.
            std::mem::forget(self.cause.replace(None));
        }
    }

    pub(crate) fn record_fatal(&self, error: crate::Error) {
        self.record_cause(Failure::Driver(error));
    }

    pub(crate) fn record_reply(&self, error: server::Diagnostic<'d>) {
        self.record_cause(Failure::Server(error));
    }

    fn record_cause(&self, failure: Failure<'d, ID>) {
        // SAFETY: this observation ends before replacement or diagnostic destruction.
        let replace = matches!(
            unsafe { &*self.cause.as_ptr() },
            None | Some(Failure::Open(_))
        );
        if replace {
            // Only None or the Copy Open value can be replaced here.
            std::mem::forget(self.cause.replace(Some(failure)));
        }
    }

    pub(crate) fn exhaust(
        &self,
        key: attempt::Id<'d, route::KeyTag<ID>>,
        cause: opening::Exhaustion,
    ) {
        self.record_cause(Failure::Open(reply::Open::Exhaustion { key, cause }));
    }

    pub(crate) fn failed(
        &self,
        key: attempt::Id<'d, route::KeyTag<ID>>,
        cause: attempt::ConnectFault,
    ) {
        self.record_cause(Failure::Open(reply::Open::Transport { key, cause }));
    }
}
