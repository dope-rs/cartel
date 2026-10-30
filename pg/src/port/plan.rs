use std::{marker, time};

use dope::{
    manifold::{app::env, network::client::topology::plan, service::health},
    net::wire::Identity,
    runtime::topology,
};

use crate::{
    port::{self, Node, config},
    schema,
};

pub struct Plan<I, const ID: u8 = 0> {
    pub(super) database: crate::Config,
    pub(super) config: config::Config,
    pub(super) queries: marker::PhantomData<fn() -> I>,
}

impl<I, const ID: u8> Plan<I, ID> {
    pub(super) fn new(database: crate::Config, config: config::Config) -> Self {
        Self {
            database,
            config,
            queries: marker::PhantomData,
        }
    }

    pub fn config(&self) -> &config::Config {
        &self.config
    }
}

impl<I: schema::QuerySet, const ID: u8> Plan<I, ID> {
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
