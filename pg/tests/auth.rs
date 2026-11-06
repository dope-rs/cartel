use cartel_pg::api::traits::{Client as _, Status as _};
use cartel_pg::{Error::ServerFinal, protocol::Reason::SignatureMismatch, reply::Error::Driver};
use cartel_pg::{api, port, port::config};
use common::{port::Queries, topology, wire::auth};
use dope::core::driver::life::recovery;
use dope::{
    core::driver::route::Id, manifold::app::env::Bundle, manifold::timing::Throughput,
    net::tcp::Tcp, net::wire::Identity, runtime::executor::session::Completion,
};
use dope_fiber::BlockOn as _;
use dope_test::harness::peer;
use dope_test::{harness::GUARD, reclaim::Reports};
use std::time::Duration;

pub mod common;

#[test]
fn authenticates_peers() {
    for (index, (password, prepared, salt, rounds, valid)) in auth::passwords().enumerate() {
        let connections = if index == 0 { auth::CONNECTIONS } else { 1 };
        peer::AcceptScript::with(
            move |listener| auth::serve(listener, &prepared, salt, rounds, connections, valid),
            |server| {
                let limits = config::Config::new(config::Capacities {
                    connections,
                    inflight: connections,
                    admission_waiters: connections,
                    requests: config::Request {
                        frames: connections,
                        frame_bytes: 512,
                    },
                    responses: config::Response {
                        items: connections,
                        bytes: connections * 512,
                        frame_bytes: 512,
                    },
                    notifications: config::Notification {
                        items: 1,
                        bytes: 128,
                    },
                });
                let limits = limits.expect("valid pg configuration");
                let login = cartel_pg::Config::new("user".into(), password, "database".into())
                    .expect("valid PostgreSQL credentials");
                let ((), events) = {
                    let prepared = topology::try_new::<_, Bundle<Tcp, Identity, Throughput>>(
                        port::Port::<Queries>::plan(login, limits),
                        server.addr(),
                        Duration::from_millis(1),
                        Duration::from_millis(1024),
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

                                        let result = runtime
                                            .block_on(client.status().wait_ready())
                                            .expect("SCRAM driver");
                                        assert!(matches!(
                                            (valid, result),
                                            (true, Ok(()))
                                                | (
                                                    false,
                                                    Err(Driver(ServerFinal(SignatureMismatch)))
                                                )
                                        ));
                                        assert!(valid || !client.status().is_ready());
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
                server.finish().expect("SCRAM peer server");
            },
        )
        .expect("SCRAM peer worker");
    }
}
