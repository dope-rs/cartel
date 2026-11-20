use cartel_pg::api::traits::{Client as _, Status as _};
use cartel_pg::{Error::ServerFinal, protocol::Reason::SignatureMismatch, reply::Error::Driver};
use cartel_pg::{api, port, port::config};
use common::{port::Queries, topology, wire::auth};
use dope::manifold::timing::Throughput;
use dope_fiber::BlockOn as _;
use dope_test::harness::peer;

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
                {
                    let plan = port::Port::<Queries>::plan(login, limits);
                    topology::run(plan, server.addr(), (1, 1_024), Throughput, |mut runtime| {
                        let client: api::Client<
                            api::Context<'_, '_, '_, '_, '_, '_, '_, _, _>,
                        > = runtime.root_client();

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
                    });
                }
                server.finish().expect("SCRAM peer server");
            },
        )
        .expect("SCRAM peer worker");
    }
}
