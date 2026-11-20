use cartel_gen::pg_instance;
use cartel_pg::api::traits::{Client as _, Status as _};
use cartel_pg::{api, port, port::config};
use common::{
    env::{self, Env},
    topology,
};
use dope::manifold::timing::Throughput;
use dope_fiber::BlockOn as _;
use std::env::var_os;

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

    {
        let plan = port::Port::<Probe>::plan(
            input.try_config().expect("PostgreSQL credentials"),
            settings,
        );
        topology::run(plan, input.address(), (500, 512_000), Throughput, |mut runtime| {
            let client: api::Client<
                api::Context<'_, '_, '_, '_, '_, '_, '_, _, _>,
            > = runtime.root_client();

            assert!(
                matches!(
                    runtime.block_on(client.status().wait_ready()),
                    Ok(Ok(()))
                ),
                "PostgreSQL connection ready"
            );
        });
    }
}
