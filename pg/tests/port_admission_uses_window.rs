use cartel_pg::api::traits::{Client as _, Unchecked as _};
use dope::core::driver::life::recovery;
use std::{
    net::{SocketAddr, TcpListener},
    time::Duration,
};

use cartel_pg::{api, port, port::config, reply};
use common::{port::*, topology};
use dope::{
    core::driver::route::Id,
    manifold::{app::env::Bundle, timing::Throughput},
    net::{tcp::Tcp, wire::Identity},
    runtime::executor::session::Completion,
};
use dope_fiber::BlockOn as _;
use dope_test::{harness::GUARD, reclaim::Reports};
use o3::base::time::Positive;

type Env = Bundle<Tcp, Identity, Throughput>;

pub mod common;

#[test]
fn admission_uses_window() {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).expect("listener");
    let addr = listener.local_addr().expect("listener address");
    let capacity = config::Capacities {
        connections: 1,
        inflight: 1,
        admission_waiters: 2,
        requests: config::Request {
            frames: 1,
            frame_bytes: 128,
        },
        responses: config::Response {
            items: 1,
            bytes: 128,
            frame_bytes: 128,
        },
        notifications: config::Notification {
            items: 1,
            bytes: 128,
        },
    };
    let config = config::Config::with_window(capacity, Positive::from_millis::<5>());
    let config = config.expect("valid pg configuration");
    let ((), events) = {
        let prepared = topology::try_new::<_, Env>(
            port::Port::<Queries>::plan(Queries::try_config().expect("database config"), config),
            addr,
            Duration::from_millis(1),
            Duration::from_millis(1_024),
        )
        .expect("topology fixture completed");
        let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
        let mut report = Completion::new();
        let scoped = prepared.with_local(
            |worker| {
                worker.lend(|node, stage| {
                        node.install(stage, |host, stage| {

                            stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                                let client: api::Client<'_, '_, '_, '_, '_, '_, '_, _, _> = runtime.root_client();

                assert!(
                    matches!(
                        runtime.block_on(client.unchecked().command("SELECT 1")),
                        Ok(Err(reply::Error::Driver(cartel_pg::Error::RequestTimeout(timeout))))
                            if timeout.phase() == cartel_pg::RequestPhase::Admission
                    ),
                    "request admission must time out"
                );

                            })
                        })
                    })
            },
            recovery::Policy::new(|event| reports.push(event), common::recover),
|outcome| {
    assert!(outcome.is_ok(), "{:?}", outcome.as_ref().err().map(|error| error.cause()));
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
