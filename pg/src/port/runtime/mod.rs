use crate::port::Disposition;
use crate::port::{
    config,
    pool::{access, slot},
    replies,
};
use crate::protocol::{self, scram};
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
    pub(crate) database: crate::Config,
    pub(crate) config: config::Config,
    index: ready::Index<'d, ID>,
    policy: cell::Cell<protocol::PickPolicy>,
    next: cell::Cell<waiters::Step>,
    ready_waiters: wait::Notifier<'d>,
    pub(crate) frame_waiters: wait::Notifier<'d>,
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
            index: ready::Index::try_new(ready)?,
            policy: cell::Cell::new(protocol::PickPolicy::default()),
            next: cell::Cell::new(waiters::Step::Retire),
            ready_waiters: wait::Notifier::new(driver, wait),
            frame_waiters: wait::Notifier::new(driver, wait),
            transactions: waiters::Table::try_new(driver, cap)?,
            notifications: notify::Queue::try_new(driver, items, bytes, wait, proof)?,
            signals: states::Signals {
                cause: cell::Cell::new(None),
                // Each admission waiter retains one distinct request-pool frame.
                egress: wait::Notifier::new(driver, wait::Capacity::new(caps.requests.frames)),
            },
            scram: scram::Runtime::try_new(cap, proof)?,
        })
    }

    pub(crate) fn transaction(&self) -> tx::Access<'_, 'd, ID> {
        tx::Access { shared: self }
    }

    pub(crate) fn waiters(&self) -> waiters::Set<'_, 'd, ID> {
        waiters::Set::new(
            &self.next,
            &self.ready_waiters,
            &self.transactions,
            &self.notifications.waiters,
            &self.signals.egress,
            &self.frame_waiters,
        )
    }
}

impl<'d, const ID: u8> Shared<'d, ID> {
    /// Operations can use prepared peers while another connection is opening.
    pub(crate) fn failure(&self) -> Option<crate::reply::Error<'d, ID>> {
        match self.signals.failure() {
            Some(crate::reply::Error::Open(_)) if self.count() != 0 => None,
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
        self.ready_waiters.notify();
        self.signals.egress.notify();
    }

    pub(crate) fn try_close(
        &self,
        conn: conn::Id<'d, route::KeyTag<ID>>,
        mut cause: replies::Failure,
        mode: Disposition,
    ) -> Option<replies::Failure> {
        self.entry(conn.index().into_usize())?
            .close(conn, &mut cause, mode)?;
        Some(cause)
    }

    pub(crate) fn wake_close(
        &self,
        conn: conn::Id<'d, route::KeyTag<ID>>,
        mut cause: replies::Failure,
        mode: Disposition,
    ) -> replies::Failure {
        let Some(entry) = self.entry(conn.index().into_usize()) else {
            return cause;
        };
        let slot = entry.slot();
        if entry.close(conn, &mut cause, mode).is_some() {
            slot.wake();
        }
        cause
    }

    fn apply_close(&self, state: slot::Closed<'d, ID>) {
        match state {
            slot::Closed::Reserved(tx) | slot::Closed::Held(tx) | slot::Closed::Finalizing(tx) => {
                self.wake_tx(tx);
            }
            slot::Closed::Handshaking | slot::Closed::Shared | slot::Closed::Draining => {
                self.wake_ready();
            }
            slot::Closed::Closing => {}
        }
    }

    fn wake_tx(&self, target: access::Transaction<'d, ID>) {
        if let Err(cause) = self.transactions.wake(target) {
            self.signals
                .record_fatal(crate::Error::TransactionSlot(cause));
        }
        self.wake_ready();
    }
}
