mod open;

pub(crate) use open::Open;

use crate::api::tx;
use crate::{api, catalogs, retry, runtime};
use api::{exec, stmt, work};
use dope_core::driver::{schedule, schedule::clock};
use dope_fiber::abi;
pub(crate) use runtime::slot::Ready as State;
use std::{result, task};
use work::cleanup::{Activity, Status};

pub(crate) enum Plan {
    Idle,
    Delayed,
    Ready,
}

impl<'local, 'd: 'local, I: catalogs::Catalog> State<'local, 'd, I> {
    pub(crate) fn query(&mut self) -> exec::Query<'_, 'local, 'd, I> {
        self.query_in(exec::Tx::NONE)
    }

    pub(crate) fn progress(&self) -> schedule::Progress<'d> {
        match self.pending() {
            Status::Idle => schedule::Progress::Quiescent,
            Status::Pending => self.retry_ref().progress(),
        }
    }

    pub(crate) fn stopping(&self) -> schedule::Progress<'d> {
        match self.progress() {
            schedule::Progress::Quiescent => schedule::Progress::Runnable,
            progress => progress,
        }
    }

    pub(crate) fn maintenance(&self, now: clock::Moment<'d>) -> Plan {
        match self.pending() {
            Status::Idle => Plan::Idle,
            Status::Pending => match self.retry_ref().park(now) {
                retry::Park::Ready => Plan::Ready,
                retry::Park::Delayed => Plan::Delayed,
            },
        }
    }

    pub(crate) fn maintain(&mut self, now: clock::Moment<'d>) -> task::Poll<()> {
        match self.poll_cleanup(now) {
            task::Poll::Pending | task::Poll::Ready(Activity::Idle) => task::Poll::Pending,
            task::Poll::Ready(Activity::Advanced) => task::Poll::Ready(()),
        }
    }

    pub(crate) fn shutdown(&self) {
        match self.pending() {
            Status::Idle => {}
            Status::Pending => self.rollback(),
        }
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
