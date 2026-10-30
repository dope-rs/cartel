use cartel_gen::pg_instance;
use cartel_pg::api::traits::{Client as _, Status as _};
use cartel_pg::{api, port, port::config};
use common::{
    env::{self, Env},
    topology,
};
use dope::core::driver::life::recovery;
use dope::runtime::executor::session::Completion;
use dope::{
    core::driver::route::Id, manifold::app::env::Bundle, manifold::timing::Throughput,
    net::tcp::Tcp, net::wire::Identity,
};
use dope_fiber::BlockOn as _;
use dope_test::{harness::GUARD, reclaim::Reports};
use std::env::var_os;
use std::time::Duration;

pub mod common;

pg_instance! { Probe: }

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn probe_step_by_step() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    let settings = config::Config::new(config::Capacities {
        connections: 1,
        inflight: 16,
        admission_waiters: 16,
        requests: config::Request {
            frames: 16,
            frame_bytes: 4 * 1024,
        },
        responses: config::Response {
            items: 65_536,
            bytes: 256 * 1024 * 1024,
            frame_bytes: 16 * 1024 * 1024,
        },
        notifications: config::Notification {
            items: 1024,
            bytes: 4 * 1024 * 1024,
        },
    });
    let settings = settings.expect("port config");

    let ((), events) = {
        let prepared = topology::Prepare::try_new::<Bundle<Tcp, Identity, Throughput>>(
            port::Port::<Probe>::plan(
                input.try_config().expect("PostgreSQL credentials"),
                settings,
            ),
            input.address(),
            Duration::from_millis(500),
            Duration::from_secs(512),
        )
        .expect("topology fixture completed");
        let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
        let mut report = Completion::new();
        let scoped = prepared.with_local(
            |worker| {
                worker.lend(|node, stage| {
                    node.install(stage, |host, stage| {
                        stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                            let client: api::Client<'_, '_, '_, '_, '_, '_, '_, _, _> =
                                runtime.root_client();

                            assert!(
                                matches!(
                                    runtime.block_on(client.status().wait_ready()),
                                    Ok(Ok(()))
                                ),
                                "PostgreSQL connection ready"
                            );
                        })
                    })
                })
            },
            recovery::Policy::new(|event| reports.push(event), common::recover),
            |outcome| {
                assert!(
                    outcome.is_ok(),
                    "{:?}",
                    outcome.as_ref().err().map(|error| error.cause())
                );
                outcome.ok().expect("application entry")
            },
        );

        let result = scoped.expect("application installation completed");
        (result.expect("application session completed"), reports)
    };
    assert!(
        events.events().expect("native reclamation").is_empty(),
        "unexpected events PostgreSQL operations: {events:?}"
    );
}
