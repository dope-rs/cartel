use cartel_pg::api::traits::{Client as _, Status as _};
use cartel_pg::{port, reply};
use common::{port::Queries, topology, wire::auth};
use dope::{
    core::driver::{life::recovery, route::Id},
    manifold::{app::env::Bundle, timing::Throughput},
    net::{tcp::Tcp, wire::Identity},
    runtime::executor::session::Completion,
};
use dope_fiber::BlockOn as _;
use dope_test::{
    harness::{GUARD, peer::TcpScript},
    reclaim::Reports,
};
use std::time::Duration;

pub mod common;

#[test]
fn rejects_wire() {
    for (bytes, expected) in auth::REJECTED {
        TcpScript::with(
            |socket| auth::reject(socket, bytes),
            |server| {
                let prepared = topology::try_new::<_, Bundle<Tcp, Identity, Throughput>>(
                    port::Port::<Queries>::plan(
                        Queries::try_config().expect("database configuration"),
                        Queries::try_cancel().expect("port configuration"),
                    ),
                    server.addr(),
                    Duration::from_millis(1),
                    Duration::from_secs(1),
                )
                .expect("SCRAM topology");
                let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
                let mut report = Completion::new();
                prepared
                    .with_local(
                        |worker| {
                            worker.lend(|node, stage| {
                                node.install(stage, |host, stage| {
                                    stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                                        let client = runtime.root_client();
                                        let failed = runtime
                                            .block_on(client.status().wait_ready())
                                            .expect("SCRAM driver")
                                            .expect_err("SCRAM rejection");
                                        let reply::Error::Driver(reason) = failed else {
                                            panic!("SCRAM driver failure");
                                        };
                                        assert_eq!(format!("{reason:?}"), expected);
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
                            outcome.ok().expect("SCRAM scope")
                        },
                    )
                    .expect("SCRAM installation")
                    .expect("SCRAM session");
                assert!(reports.events().expect("native reclamation").is_empty());
                server
                    .finish()
                    .expect("SCRAM peer execution")
                    .expect("SCRAM peer protocol");
            },
        )
        .expect("SCRAM peer");
    }
}
