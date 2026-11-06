use std::{marker, num};

use cartel_core::{deadline, queue, reply};
use dope::{
    core::driver::{route::KeyTag, schedule::app::Work},
    manifold::{
        client::{conn, side::registry},
        service::bindings::endpoint,
    },
    net::link::egress,
    runtime::client,
};
use o3::{
    alloc,
    base::cell::region,
    buffer::pool::{CreateError, Pool},
};
use reply::{layout, order};

use crate::{
    api,
    port::{
        pool::{control, slot},
        replies::groups,
    },
    protocol::runtime,
    schema,
};

mod backend;
pub mod config;
mod node;
mod plan;
pub(crate) mod pool;
pub mod ready;
pub mod replies;
#[doc(hidden)]
pub mod requests;

#[doc(hidden)]
pub use backend::Backend;
pub(crate) use backend::Storage;
pub use node::Node;
pub use plan::Plan;

pub(super) const CONNECTOR_CONNECTION_LIMIT: usize = u32::MAX as usize;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Build {
    Runtime(alloc::Error),
    Requests(alloc::Exhausted),
    Cancel(alloc::Error),
    CancelBuffer(CreateError),
    Queue(alloc::Error),
    QueueDomain(queue::Mismatch),
    Replies(alloc::Error),
    GroupRecords(alloc::Exhausted),
    GroupRecordPool(alloc::Error),
    GroupOwners(alloc::Error),
    GroupBytes(alloc::Exhausted),
}

pub(super) struct Lifecycle<'d, const ID: u8> {
    pub(super) deadline: deadline::Source<'d>,
    pub(super) cancel: registry::Registry<'d, Backend<'d, ID>, KeyTag<ID>>,
    pub(super) buffer: backend::Storage,
}

pub struct Port<'e, 'g, 'r, 's, 'b, 'c, 'd, I: schema::QuerySet, const ID: u8 = 0> {
    pub(super) shared: runtime::Shared<'d, ID>,
    pub(super) lifecycle: Lifecycle<'d, ID>,
    request_queue: queue::Scoped<'b, 'd, egress::NonEmpty<requests::Frame<'d>>>,
    requests: Pool,
    controls: Pool,
    responses: &'e reply::Arena<
        'b,
        'c,
        'd,
        replies::Row<'d>,
        replies::ReplyFailure<'d>,
        replies::Tag<'g, 'r, 's, 'b, 'c, 'd>,
    >,
    pub(crate) diagnostics: Option<groups::Access<'g, 'r, 's, 'b, 'c, 'd>>,
    pub(crate) batches: &'e reply::Batches<'b, 'c, 'd, replies::Row<'d>>,
    queries: marker::PhantomData<fn() -> I>,
}

impl<
    'life,
    'e: 'life,
    'g: 'life,
    'r: 'life,
    's: 'life,
    'b: 'life,
    'c,
    'd: 'life,
    I: schema::QuerySet + 'life,
    const ID: u8,
> endpoint::Provider<'life, 'd, KeyTag<ID>, { CONNECTOR_CONNECTION_LIMIT }>
    for Port<'e, 'g, 'r, 's, 'b, 'c, 'd, I, ID>
{
    type Client<'app, B: ?Sized + 'app>
        = api::Client<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
    where
        'life: 'app,
        'd: 'app;

    fn provide<'app, B: ?Sized + 'app>(
        &'life self,
        bound: client::Bound<'app, 'd, B>,
    ) -> Self::Client<'app, B>
    where
        'life: 'app,
        'd: 'app,
    {
        api::Client { port: self, bound }
    }
}

impl<'e, 'g, 'r, 's, 'b, 'c, 'd, I: schema::QuerySet, const ID: u8>
    Port<'e, 'g, 'r, 's, 'b, 'c, 'd, I, ID>
{
    pub fn plan(database: crate::Config, config: config::Config) -> Plan<I, ID> {
        Plan::new(database, config)
    }

    pub fn capacity(&self) -> usize {
        self.shared.config.capacities().connections
    }

    pub fn config(&self) -> &config::Config {
        &self.shared.config
    }

    pub(super) fn backpressure(&self, held: num::NonZero<usize>) -> crate::Error {
        crate::Error::Backpressure {
            inflight: self.responses.inflight(),
            transactions: held,
            cap: layout::Entries::new(self.shared.config.capacities().inflight),
        }
    }

    fn conn(&self, token: conn::Id<'d, KeyTag<ID>>) -> Option<&slot::Slot<'d, ID>> {
        self.shared.ready.connection(token)
    }

    pub(super) fn connection(&self) -> control::Control<'_, 'g, 'r, 's, 'b, 'c, 'd, I, ID> {
        control::Control::new(self)
    }

    pub(super) fn request(&self) -> requests::Access<'_, 'g, 'r, 's, 'b, 'c, 'd, I, ID> {
        requests::Access::new(self)
    }

    pub(super) fn responses(
        &self,
        token: conn::Id<'d, KeyTag<ID>>,
    ) -> Option<replies::Lane<'_, 'g, 'r, 's, 'b, 'c, 'd, I, ID>> {
        self.conn(token)?;
        let lane = self.lane(token.lane());
        Some(replies::Lane::new(self, token, lane))
    }

    pub(super) fn depth(
        &self,
        token: conn::Id<'d, KeyTag<ID>>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Option<order::Depth> {
        self.responses(token).map(|lane| lane.depth(region))
    }

    /// Both counts retain u32 domains; o3 admits only usize widths of at least
    /// 64 bits, so their sum plus the open barrier always fits.
    pub(super) fn load(
        &self,
        token: conn::Id<'d, KeyTag<ID>>,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Option<usize> {
        let conn = self.conn(token)?;
        let depth = self.lane(token.lane()).depth(region).get();
        Some(depth + conn.barriers())
    }

    pub(crate) fn maintain(
        &self,
        work: Work<'_, 'd>,
        region: &mut region::Token<region::Identity<'d>>,
    ) {
        if let Some(released) = self.responses.maintain(work, region) {
            let signals = &self.shared.signals;
            if let Err(cause) = signals.egress.release(released) {
                signals.record_fatal(crate::Error::EgressSlot(cause));
            }
        }
    }

    pub(crate) fn maint(&self) -> reply::Work {
        self.responses.work()
    }

    fn lane(
        &self,
        lane: conn::Lane<'d, KeyTag<ID>>,
    ) -> reply::Lane<
        '_,
        'b,
        'c,
        'd,
        replies::Row<'d>,
        replies::ReplyFailure<'d>,
        replies::Tag<'g, 'r, 's, 'b, 'c, 'd>,
    > {
        // SAFETY: the port constructs connection and reply storage from one lane domain.
        unsafe { self.responses.raw().lane(lane) }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Disposition {
    Reconnect,
    Permanent,
}

/// A matching connection whose application state has not entered retirement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Retirement {
    Handshaking,
    Shared,
    Reserved,
    Held,
    Finalizing,
    Draining,
}

type Snapshot<E> = service::Snapshot<
    service::Endpoint<
        <<E as env::Spec>::Transport as net::Transport>::Addr,
        <<E as env::Spec>::Transport as net::Transport>::Addr,
    >,
>;

use dope::{
    manifold::{app::env, service},
    net,
};
