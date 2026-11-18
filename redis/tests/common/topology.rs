use std::net::SocketAddr;

use cartel_redis::{
    client, client::plan, client::settings, client::settings::Config, client::settings::init,
};
use dope::{
    core::driver::{
        life::recovery,
        route::{Id, KeyTag},
    },
    manifold::{
        app::{self, env},
        network::client::Pool,
        service::{self, bindings, sync},
        timing::Throughput,
    },
    net::{Transport, tcp::Tcp, wire::Identity},
    runtime::{executor::session, topology::Prepared},
};
use dope_test::{harness::GUARD, reclaim::Reports};

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

type Env = env::Bundle<Tcp, Identity, Throughput>;
const LIMIT: usize = u32::MAX as usize;

type Host<'q, 'h, 'r, 's, 'storage, 'b, 'c, 'n, 'd> = app::Host<
    Pool<
        'q,
        'h,
        'n,
        'd,
        'd,
        KeyTag<0>,
        LIMIT,
        bindings::Bound<
            'r,
            'd,
            cartel_redis::Port<'s, 'storage, 'b, 'c, 'd>,
            client::Logic<'s, 'd>,
            KeyTag<0>,
            LIMIT,
        >,
        sync::Replace,
        SocketAddr,
        Env,
    >,
>;

pub fn run(
    config: Config,
    address: SocketAddr,
    test: impl for<'app, 'life, 'q, 'h, 'r, 's, 'storage, 'b, 'c, 'n, 'd> FnOnce(
        session::App<'app, 'life, 'd, Host<'q, 'h, 'r, 's, 'storage, 'b, 'c, 'n, 'd>>,
    ),
) -> Reports<1> {
    let prepared =
        try_new::<Env, _, _, _>(config, address, init::Plan::new()).expect("Redis topology");
    let mut reports = Reports::new(prepared.layout(), [Id::fixed::<0>()]);
    let mut completion = session::Completion::new();
    prepared.with_local(
        |worker| {
            worker.lend(|node, stage| {
                node.install(stage, |host, stage| {
                    stage.with_quiesce(&mut completion, host, GUARD, test)
                })
            })
        },
        recovery::Policy::new(|event| reports.push(event), crate::common::recover),
        |result| assert!(matches!(result, Ok(Ok(Ok(())))), "Redis scope"),
    );
    reports
}
