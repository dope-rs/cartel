use cartel_core::queue;
use dope::core::driver::route;
use dope::core::driver::settings::errors;
use o3::alloc;
use std::time;

use crate::client;
use crate::client::settings::{Config, init};
use dope::manifold::app::env;
use dope::manifold::network::client::topology::plan;
use dope::manifold::service::health;
use dope::net;
use dope::net::wire::Identity;
use dope::runtime::topology;

#[derive(Debug)]
pub enum Error {
    Allocation(alloc::Error),
    Queue(queue::Mismatch),
    Retry(health::Error),
    Connector(errors::Claim),
}

impl From<alloc::Error> for Error {
    fn from(err: alloc::Error) -> Self {
        Self::Allocation(err)
    }
}

pub struct Plan<const ID: u8 = 0> {
    pub(in crate::client) config: Config,
}

impl<const ID: u8> Plan<ID> {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    pub fn plan(
        &self,
        base: time::Duration,
        max: time::Duration,
    ) -> Result<plan::Plan<route::KeyTag<ID>, { client::CONNECTOR_CONNECTION_LIMIT }>, Error> {
        plan::Plan::new(
            self.config.connections(),
            plan::Spec::new(base, max)
                .map_err(Error::Retry)?
                .with_egress(self.config.egress()),
        )
        .map_err(Error::Connector)
    }

    pub fn assemble<E>(
        self,
        set: client::Snapshot<E>,
        base: time::Duration,
        max: time::Duration,
        init: init::Plan,
    ) -> Result<topology::Plan<(), client::Node<ID, E>>, Error>
    where
        E: env::Spec<Wire = Identity>,
        E::Transport: net::Transport,
    {
        let plan = self.plan(base, max)?;
        let node = client::Node::new(self.config, set, plan, init);
        Ok(topology::Plan::new((), node))
    }
}
