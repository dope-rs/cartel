mod open;

pub(crate) use open::Open;

use crate::api::tx;
use crate::{api, catalogs, retry, runtime, sys};
use api::{exec, stmt, work};
use dope_core::driver::{schedule, schedule::clock};
use dope_fiber::abi;
use runtime::slot;
use std::{result, task};
use stmt::{access::Access, target::Target};
use work::cleanup::{Activity, Status};

pub(crate) enum Plan {
    Idle,
    Delayed,
    Ready,
}

#[repr(transparent)]
pub(crate) struct State<'local, 'd: 'local, I: catalogs::Catalog = catalogs::Empty> {
    ready: slot::Ready<'local, 'd, I>,
}

impl<'local, 'd: 'local, I: catalogs::Catalog> State<'local, 'd, I> {
    pub(super) fn new(ready: slot::Ready<'local, 'd, I>) -> Self {
        Self { ready }
    }

    pub(crate) fn query(&mut self) -> exec::Query<'_, 'local, 'd, I> {
        self.query_in(exec::Tx::NONE)
    }

    pub(crate) fn query_in<'query>(
        &'query mut self,
        txn: exec::Tx<'query>,
    ) -> exec::Query<'query, 'local, 'd, I> {
        self.ready.query(txn)
    }

    pub(crate) fn control<'query, T: Target<I>>(
        &'query mut self,
        control: T,
        txn: exec::Tx<'query>,
    ) -> impl abi::Fiber<
        'd,
        Output = result::Result<<T::Access as Access>::Output<()>, crate::Error>,
    > + use<'query, 'local, 'd, I, T> {
        self.ready.control(control, txn)
    }

    pub(crate) fn work(&self) -> &'local work::Work {
        self.ready.work()
    }

    pub(crate) fn progress(&self) -> schedule::Progress<'d> {
        match self.ready.pending() {
            Status::Idle => schedule::Progress::Quiescent,
            Status::Pending => self.ready.retry_ref().progress(),
        }
    }

    pub(crate) fn stopping(&self) -> schedule::Progress<'d> {
        match self.progress() {
            schedule::Progress::Quiescent => schedule::Progress::Runnable,
            progress => progress,
        }
    }

    pub(crate) fn maintenance(&self, now: clock::Moment<'d>) -> Plan {
        match self.ready.pending() {
            Status::Idle => Plan::Idle,
            Status::Pending => match self.ready.retry_ref().park(now) {
                retry::Park::Ready => Plan::Ready,
                retry::Park::Delayed => Plan::Delayed,
            },
        }
    }

    pub(crate) fn maintain(&mut self, now: clock::Moment<'d>) -> task::Poll<()> {
        match self.ready.poll_cleanup(now) {
            task::Poll::Pending | task::Poll::Ready(Activity::Idle) => task::Poll::Pending,
            task::Poll::Ready(Activity::Advanced) => task::Poll::Ready(()),
        }
    }

    pub(crate) fn retire(self) -> slot::Retirement<'local, 'd, I> {
        self.ready.retire()
    }

    pub(crate) fn shutdown(&self) {
        match self.ready.pending() {
            Status::Idle => {}
            Status::Pending => self.ready.rollback(),
        }
    }

    pub(crate) fn tx(&self) -> sys::Tx {
        self.ready.transaction()
    }

    pub(crate) fn transaction<'database>(
        &'database mut self,
    ) -> impl abi::Fiber<
        'd,
        Output = result::Result<tx::Transaction<'database, 'local, 'd, I>, crate::Error>,
    > + use<'database, 'local, 'd, I> {
        dope_fiber::fiber!('d => async move {
            self.control(
                stmt::Control::Begin,
                exec::Tx::NONE,
            )
            .await?;
            Ok(tx::Transaction::new(self))
        })
    }
}
