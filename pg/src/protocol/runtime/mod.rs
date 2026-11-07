use crate::port::Disposition;
use crate::port::{
    config,
    pool::{access, slot},
    replies,
};
use crate::protocol::scram;
use cartel_core::reply::order;
use dope::core::driver::{self, route};
use dope::manifold::client::{close, conn};
use dope_fiber::wait;
use o3::alloc;
use o3::base::cell::region;
use std::cell;

pub(crate) mod notify;
pub(crate) mod ready;
mod states;
pub(crate) mod tx;
mod waiters;

pub(crate) struct Shared<'d, const ID: u8> {
    pub(super) database: crate::Config,
    pub(crate) config: config::Config,
    pub(crate) ready: ready::Set<'d, ID>,
    transactions: waiters::Table<'d, ID>,
    pub(crate) notifications: notify::Queue<'d>,
    pub(crate) signals: states::Signals<'d, ID>,
    pub(crate) scram: scram::Runtime<'d, ID>,
}

impl<'d, const ID: u8> Shared<'d, ID> {
    pub(crate) fn try_new(
        db: crate::Config,
        config: config::Config,
        driver: driver::Reference<'d>,
        proof: region::Identity<'d>,
    ) -> Result<Self, alloc::Error> {
        let caps = config.capacities();
        let cap = config.side().primary();
        let wait = wait::Capacity::new(caps.admission_waiters);
        let ready = config.ready_layout();
        let items = config.notice_limit();
        let bytes = caps.notifications.bytes;
        Ok(Self {
            database: db,
            config,
            ready: ready::Set::try_new(ready, driver, wait)?,
            transactions: waiters::Table::try_new(driver, cap)?,
            notifications: notify::Queue::try_new(driver, items, bytes, wait, proof)?,
            signals: states::Signals {
                cause: cell::Cell::new(None),
                egress: waiters::Egress::try_new(driver, cap, wait)?,
            },
            scram: scram::Runtime::try_new(cap, proof)?,
        })
    }

    pub(crate) fn transaction(&self) -> tx::Access<'_, 'd, ID> {
        tx::Access { shared: self }
    }

    pub(crate) fn waiters(&self) -> waiters::Set<'_, 'd, ID> {
        waiters::Set::new(
            &self.ready.waiters,
            &self.transactions,
            &self.notifications.waiters,
            &self.signals.egress,
        )
    }
}

impl<'d, const ID: u8> Shared<'d, ID> {
    /// Operations can use prepared peers while another connection is opening.
    pub(crate) fn failure(&self) -> Option<crate::reply::Error<'d, ID>> {
        match self.signals.failure() {
            Some(crate::reply::Error::Open(_)) if self.ready.count() != 0 => None,
            failure => failure,
        }
    }

    pub(crate) fn reject(&self, reason: close::Reason) {
        self.fail(crate::Error::Connection(reason));
    }

    pub(crate) fn fail(&self, error: crate::Error) {
        self.signals.record_fatal(error);
        self.waiters().wake_all();
    }

    pub(crate) fn wake_ready(&self) {
        self.ready.waiters.notify();
    }

    pub(crate) fn reindex(&self, slot: conn::Id<'d, route::KeyTag<ID>>, depth: order::Depth) {
        if let Some(slot) = self.ready.reindex(slot, depth) {
            self.wake_close(
                slot,
                replies::Failure::from_error(crate::Error::Closed),
                Disposition::Reconnect,
            );
        }
    }

    pub(crate) fn try_close(
        &self,
        conn: conn::Id<'d, route::KeyTag<ID>>,
        cause: replies::Failure,
        mode: Disposition,
    ) -> Option<replies::Failure> {
        let close = self.ready.try_close(conn, cause, mode)?;
        Some(self.apply_close(close))
    }

    pub(crate) fn wake_close(
        &self,
        conn: conn::Id<'d, route::KeyTag<ID>>,
        cause: replies::Failure,
        mode: Disposition,
    ) -> replies::Failure {
        let Some(entry) = self.ready.connection(conn) else {
            return cause;
        };
        let Some(close) = self.ready.try_close(conn, cause, mode) else {
            return cause;
        };
        let cause = self.apply_close(close);
        entry.wake();
        cause
    }

    fn apply_close(&self, close: slot::Close<'d, ID>) -> replies::Failure {
        match close.state {
            slot::Closed::Reserved(tx) | slot::Closed::Held(tx) | slot::Closed::Finalizing(tx) => {
                self.wake_tx(tx);
            }
            slot::Closed::Handshaking | slot::Closed::Shared | slot::Closed::Draining => {
                self.ready.waiters.notify();
            }
            slot::Closed::Closing => return close.failure,
        }
        close.failure
    }

    fn wake_tx(&self, target: access::Transaction<'d, ID>) {
        if let Err(cause) = self.transactions.wake(target) {
            self.signals
                .record_fatal(crate::Error::TransactionSlot(cause));
        }
        self.ready.waiters.notify();
    }
}
