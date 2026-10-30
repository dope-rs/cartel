use cartel_pg::{port, schema};
use dope::{
    core::driver::settings::errors::Claim,
    manifold::app::env,
    manifold::service::{self, health},
    manifold::timing::Throughput,
    net::Transport,
    net::wire::Identity,
    runtime::topology::Prepared,
};
use std::time::Duration;

#[derive(Debug)]
pub enum Error {
    Health(health::Error),
    Claim(Claim),
}

pub trait Prepare<I: schema::QuerySet> {
    fn try_new<E>(
        self,
        address: <E::Transport as Transport>::Addr,
        base: Duration,
        maximum: Duration,
    ) -> Result<Prepared<(), port::Node<I, 0, E>>, Error>
    where
        E: env::Spec<Wire = Identity>,
        E::Transport: Transport<Addr: Clone>;
}

impl<I: schema::QuerySet> Prepare<I> for port::Plan<I> {
    fn try_new<E>(
        self,
        address: <E::Transport as Transport>::Addr,
        base: Duration,
        maximum: Duration,
    ) -> Result<Prepared<(), port::Node<I, 0, E>>, Error>
    where
        E: env::Spec<Wire = Identity>,
        E::Transport: Transport<Addr: Clone>,
    {
        let snapshot = service::Snapshot::from_one(
            service::Revision::new(1),
            service::Endpoint::new(address.clone(), address),
        );
        self.assemble::<E>(snapshot, base, maximum)
            .map_err(Error::Health)?
            .for_install::<Throughput>()
            .map_err(Error::Claim)
    }
}
