use crate::{api, catalogs, retry};
use api::{exec, stmt, work};
use dope_fiber::context;
use exec::machine::{self, call};
use retry::lease;
use std::{mem, pin, result, task};
use stmt::target::Target;
use work::cleanup;

struct Parts<'query, 'connection, 'd, I: catalogs::Catalog> {
    engine: &'query mut stmt::Engine<'connection, I>,
    work: &'query work::Work,
    retry: lease::Lease<'query, 'd, lease::Cleanup>,
    txn: exec::Tx<'query>,
}

impl<'query, 'connection, 'd, I: catalogs::Catalog> Parts<'query, 'connection, 'd, I> {
    fn prepare(self) -> result::Result<machine::Ready<'query, 'connection, 'd, I>, crate::Error> {
        self.txn.check()?;
        let retry = self.retry.into_call();
        let guard = self.work.begin(retry.config().work.call.progress)?;
        let state = call::State::new(guard, retry, self.txn);
        Ok(machine::Ready::new(self.engine, state))
    }

    fn cleanup(self) -> (cleanup::Run<'query, 'connection, 'd, I>, exec::Tx<'query>) {
        let run = cleanup::Run::new(self.engine, self.work, self.retry);
        (run, self.txn)
    }
}

enum Phase<'query, 'connection, 'd, I: catalogs::Catalog> {
    New(Parts<'query, 'connection, 'd, I>),
    Cleanup(cleanup::Run<'query, 'connection, 'd, I>, exec::Tx<'query>),
}

pub(crate) struct Start<'query, 'connection, 'd, I: catalogs::Catalog> {
    phase: Phase<'query, 'connection, 'd, I>,
}

impl<'query, 'connection, 'd, I: catalogs::Catalog> Start<'query, 'connection, 'd, I> {
    pub(crate) const fn new(
        engine: &'query mut stmt::Engine<'connection, I>,
        work: &'query work::Work,
        retry: lease::Lease<'query, 'd, lease::Cleanup>,
        txn: exec::Tx<'query>,
    ) -> Self {
        Self {
            phase: Phase::New(Parts {
                engine,
                work,
                retry,
                txn,
            }),
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
        self,
        mut context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> machine::Next<Self, machine::Ready<'query, 'connection, 'd, I>> {
        let mut phase = self.phase;
        loop {
            let parts = match phase {
                Phase::New(parts) => match parts.work.pending.status() {
                    cleanup::Status::Idle => parts,
                    cleanup::Status::Pending => {
                        let (run, txn) = parts.cleanup();
                        phase = Phase::Cleanup(run, txn);
                        continue;
                    }
                },
                Phase::Cleanup(mut run, txn) => {
                    match run.poll(context.as_mut()) {
                        task::Poll::Pending => {
                            return machine::Next::Pending(Self {
                                phase: Phase::Cleanup(run, txn),
                            });
                        }
                        task::Poll::Ready(Err(error)) => {
                            return machine::Next::Ready(Err(error));
                        }
                        task::Poll::Ready(Ok(())) => {}
                    }
                    let (engine, work, retry) = run.into_parts();
                    Parts {
                        engine,
                        work,
                        retry,
                        txn,
                    }
                }
            };
            return machine::Next::Ready(parts.prepare());
        }
    }
}

const _: () = {
    let word = mem::size_of::<usize>();
    assert!(mem::size_of::<Parts<'static, 'static, 'static, catalogs::Empty>>() == 4 * word);
    assert!(mem::size_of::<Start<'static, 'static, 'static, catalogs::Empty>>() <= 5 * word);
};
