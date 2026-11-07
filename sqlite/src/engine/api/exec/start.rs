use crate::{api, catalogs, retry};
use api::{exec, stmt, work};
use dope_fiber::context;
use exec::machine::{self, call};
use std::{mem, pin, result, task};
use stmt::target::Target;
use work::cleanup;

pub(crate) struct Start<'query, 'connection, 'd, I: catalogs::Catalog> {
    engine: &'query mut stmt::Engine<'connection, I>,
    work: &'query work::Work,
    retry: retry::raw::Loan<'query, 'd>,
    txn: exec::Tx<'query>,
}

impl<'query, 'connection, 'd, I: catalogs::Catalog> Start<'query, 'connection, 'd, I> {
    pub(crate) const fn new(
        engine: &'query mut stmt::Engine<'connection, I>,
        work: &'query work::Work,
        retry: retry::raw::Loan<'query, 'd>,
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
        &mut self,
        context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> task::Poll<result::Result<(), crate::Error>> {
        if matches!(self.work.pending.status(), cleanup::Status::Pending) {
            cleanup::Run::new(self.engine, self.work, self.retry.config().work.cleanup)
                .poll(context)
        } else {
            task::Poll::Ready(Ok(()))
        }
    }

    pub(super) fn prepare<P: api::Bind, T: Target<I>>(
        self,
        params: P,
        target: T,
    ) -> result::Result<machine::Active<'query, 'connection, 'd, P, T::Access>, crate::Error> {
        self.txn.check()?;
        let retry = self.retry.call();
        let guard = self.work.begin(retry.config().work.call.progress)?;
        let (bound, access) = target.bind(self.engine, params, retry.config(), &guard)?;
        Ok(machine::Active::new(
            bound,
            call::State::new(guard, retry, self.txn),
            access,
        ))
    }
}

const _: () = {
    let word = mem::size_of::<usize>();
    assert!(mem::size_of::<Start<'static, 'static, 'static, catalogs::Empty>>() == 4 * word);
};
