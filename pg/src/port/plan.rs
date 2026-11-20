use std::time;

use dope::{
    manifold::{app::env, network::client::topology::plan, service::health},
    net::wire::Identity,
    runtime::topology,
};

use crate::port::{self, Node, config};

pub struct Plan<I: port::Instance, const ID: u8 = 0> {
    pub(super) database: crate::Config,
    pub(super) config: config::Configuration<I::Policy>,
}

impl<I: port::Instance, const ID: u8> Plan<I, ID> {
    pub(super) fn new(database: crate::Config, config: config::Configuration<I::Policy>) -> Self {
        Self { database, config }
    }

    pub fn config(&self) -> &config::Configuration<I::Policy> {
        &self.config
    }

    pub fn assemble<E>(
        self,
        set: port::Snapshot<E>,
        base: time::Duration,
        max: time::Duration,
    ) -> Result<topology::Plan<(), Node<I, ID, E>>, health::Error>
    where
        E: env::Spec<Wire = Identity>,
    {
        let plan =
            plan::Layout::from_side(self.config.side()).into_plan(plan::Spec::new(base, max)?);
        Ok(topology::Plan::new((), Node::new(self, set, plan)))
    }
}
