use crate::{api, config, retry, sys};
use api::{exec, stmt, work};
use dope_core::driver::{schedule::clock, schedule::ready::wake};
use dope_fiber::context;
use retry::lease;
use std::{mem, pin, result, task};
use stmt::access::{Access, Authority};
use work::guard::{self, call};

pub(crate) struct State<'query, 'd> {
    pub(in crate::engine::api::exec::machine) guard: guard::Guard<'query, call::Call>,
    retry: lease::Lease<'query, 'd, lease::Call>,
    transaction: exec::Tx<'query>,
}

impl<'query, 'd> State<'query, 'd> {
    pub(crate) const fn new(
        guard: guard::Guard<'query, call::Call>,
        retry: lease::Lease<'query, 'd, lease::Call>,
        txn: exec::Tx<'query>,
    ) -> Self {
        Self {
            guard,
            retry,
            transaction: txn,
        }
    }

    pub(crate) const fn config(&self) -> &config::Config {
        self.retry.config()
    }

    pub(crate) fn poll<A: Access>(
        &mut self,
        prepared: &mut stmt::Prepared<'_>,
        access: &A,
        mut context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> task::Poll<result::Result<stmt::Step, crate::Error>> {
        loop {
            match self.retry.wait() {
                retry::Wait::Ready => {}
                retry::Wait::Pending => match self.poll_retry(context.as_mut()) {
                    task::Poll::Pending => return task::Poll::Pending,
                    task::Poll::Ready(Err(error)) => return task::Poll::Ready(Err(error)),
                    task::Poll::Ready(Ok(())) => {}
                },
            }
            match self.step(prepared, access) {
                Err(crate::Error::Busy { action, code }) => {
                    self.transaction.check()?;
                    let fault = sys::Fault::new(action, code);
                    match self.wait(context.as_mut(), fault) {
                        task::Poll::Pending => return task::Poll::Pending,
                        task::Poll::Ready(Err(error)) => return task::Poll::Ready(Err(error)),
                        task::Poll::Ready(Ok(())) => {}
                    }
                }
                result => return task::Poll::Ready(result),
            }
        }
    }

    fn step<A: Access>(
        &self,
        prepared: &mut stmt::Prepared<'_>,
        access: &A,
    ) -> result::Result<stmt::Step, crate::Error> {
        let mut op = || self.guard.check(prepared.step(access, self.transaction));
        match A::AUTHORIZATION {
            Authority::Plain => op(),
            Authority::Control => self.guard.work().control().run(op),
        }
    }

    pub(crate) fn reset<P, A: Access>(
        &self,
        bound: &mut stmt::Bound<'_, '_, P>,
        access: &A,
    ) -> result::Result<(), crate::Error> {
        // Fixed controls never yield rows; step halts or resets them while authorized.
        // Plain reset retains its denied authority.
        match bound.finish(access, self.transaction) {
            Ok(()) => Ok(()),
            Err(error) => self.guard.check(Err(error)),
        }
    }

    pub(crate) fn defer(self, error: crate::Error) -> impl Sized + use<'query, 'd> {
        (self.guard.defer(error), self.retry)
    }

    fn wait(
        &mut self,
        context: pin::Pin<&mut context::Context<'_, 'd>>,
        fault: sys::Fault,
    ) -> task::Poll<result::Result<(), crate::Error>> {
        let (now, wake) = Self::sample(context);
        self.retry.defer(now, fault)?;
        self.retry.poll(now, wake)
    }

    fn poll_retry(
        &mut self,
        context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> task::Poll<result::Result<(), crate::Error>> {
        let (now, wake) = Self::sample(context);
        self.retry.poll(now, wake)
    }

    fn sample(
        mut context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> (clock::Moment<'d>, wake::Waker<'d>) {
        let wake = context.as_ref().completion();
        let now = context.as_mut().driver().driver_ref().clock().now();
        (now, wake)
    }
}

const _: () = {
    let word = mem::size_of::<usize>();
    assert!(mem::size_of::<State<'static, 'static>>() == 3 * word);
};
