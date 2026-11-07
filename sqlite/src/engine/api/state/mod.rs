mod open;

pub(crate) use open::Open;

use crate::api::tx;
use crate::{api, catalogs, runtime};
use api::{exec, stmt, work};
use dope_core::driver::schedule;
use dope_fiber::abi;
pub(crate) use runtime::slot::Ready as State;
use std::result;
use work::cleanup::Status;

impl<'local, 'd: 'local, I: catalogs::Catalog> State<'local, 'd, I> {
    pub(crate) fn query(&mut self) -> exec::Query<'_, 'local, 'd, I> {
        self.query_in(exec::Tx::NONE)
    }

    pub(crate) fn progress(&self) -> schedule::Progress<'d> {
        match self.pending() {
            Status::Idle => schedule::Progress::Quiescent,
            Status::Pending => schedule::Progress::Runnable,
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
