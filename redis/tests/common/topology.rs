use cartel_redis::{
    client, client::plan, client::settings, client::settings::Config, client::settings::init,
};
use dope::{
    manifold::app::env, manifold::service, manifold::timing::Throughput, net::Transport,
    net::wire::Identity, runtime::topology::Prepared,
};

pub fn try_new<
    E,
    I: client::ops::scripts::Invocation + Unpin,
    const EXPECTED: i64,
    const COUNT: usize,
>(
    config: Config,
    address: <E::Transport as Transport>::Addr,
    init: init::Plan<I, EXPECTED, COUNT>,
) -> Result<Prepared<(), client::Node<0, E, I, EXPECTED, COUNT>>, plan::Error>
where
    E: env::Spec<Wire = Identity>,
    E::Transport: Transport<Addr: Clone>,
{
    let snapshot = service::Snapshot::from_one(
        service::Revision::new(1),
        service::Endpoint::new(address.clone(), address),
    );
    plan::Plan::<0>::new(config)
        .assemble::<E, _, _, _>(
            snapshot,
            settings::DEFAULT_BACKOFF,
            settings::DEFAULT_BACKOFF_MAXIMUM,
            init,
        )?
        .for_install::<Throughput>()
        .map_err(plan::Error::Connector)
}
