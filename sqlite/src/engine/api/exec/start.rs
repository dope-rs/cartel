use crate::{api, catalogs, retry};
use api::{exec, stmt, work};
use dope_fiber::context;
use exec::machine::{self, call};
use retry::lease;
use std::{mem, pin, result, task};
use stmt::target::Target;
use work::cleanup;

pub(crate) struct Start<'query, 'connection, 'd, I: catalogs::Catalog> {
    engine: &'query mut stmt::Engine<'connection, I>,
    work: &'query work::Work,
    retry: lease::Lease<'query, 'd, lease::Cleanup>,
    txn: exec::Tx<'query>,
}

impl<'query, 'connection, 'd, I: catalogs::Catalog> Start<'query, 'connection, 'd, I> {
    pub(crate) const fn new(
        engine: &'query mut stmt::Engine<'connection, I>,
        work: &'query work::Work,
        retry: lease::Lease<'query, 'd, lease::Cleanup>,
        txn: exec::Tx<'query>,
    ) -> Self {
        Self {
            engine,
            work,
            retry,
            txn,
        }
    }

    pub(crate) fn run<P: api::Bind, T: Target<I>>(
        self,
        target: T,
        params: P,
    ) -> exec::Machine<'query, 'connection, 'd, I, P, T> {
        exec::Machine::new(self, target, params)
    }

    pub(super) fn poll(
        mut self,
        context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> machine::Next<Self, machine::Ready<'query, 'connection, 'd, I>> {
        if matches!(self.work.pending.status(), cleanup::Status::Pending) {
            let mut run = cleanup::Run::new(self.engine, self.work, self.retry);
            let polled = run.poll(context);
            // Progress stays in Work and the original retry slot. Moving this
            // lease back preserves its budget and alarm without detaching it.
            (self.engine, self.work, self.retry) = run.into_parts();
            match polled {
                task::Poll::Pending => return machine::Next::Pending(self),
                task::Poll::Ready(Err(error)) => return machine::Next::Ready(Err(error)),
                task::Poll::Ready(Ok(())) => {}
            }
        }
        machine::Next::Ready(self.prepare())
    }

    fn prepare(self) -> result::Result<machine::Ready<'query, 'connection, 'd, I>, crate::Error> {
        self.txn.check()?;
        let retry = self.retry.into_call();
        let guard = self.work.begin(retry.config().work.call.progress)?;
        let state = call::State::new(guard, retry, self.txn);
        Ok(machine::Ready::new(self.engine, state))
    }
}

const _: () = {
    let word = mem::size_of::<usize>();
    assert!(mem::size_of::<Start<'static, 'static, 'static, catalogs::Empty>>() == 4 * word);
};
