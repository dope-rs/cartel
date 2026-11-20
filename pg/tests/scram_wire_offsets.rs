use cartel_pg::api::traits::{Client as _, Status as _};
use cartel_pg::{port, reply};
use common::{port::Queries, topology, wire::auth};
use dope::manifold::timing::Throughput;
use dope_fiber::BlockOn as _;
use dope_test::harness::peer::TcpScript;

pub mod common;

#[test]
fn rejects_wire() {
    for (code, bytes, expected) in auth::REJECTED {
        TcpScript::with(
            |socket| auth::reject(socket, code, bytes),
            |server| {
                let plan = port::Port::<Queries>::plan(
                    Queries::try_config().expect("database configuration"),
                    Queries::try_cancel().expect("port configuration"),
                );
                topology::run(plan, server.addr(), (1, 1_000), Throughput, |mut runtime| {
                    let client = runtime.root_client();
                    let failed = runtime
                        .block_on(client.status().wait_ready())
                        .expect("SCRAM driver")
                        .expect_err("SCRAM rejection");
                    let reply::Error::Driver(reason) = failed else {
                        panic!("SCRAM driver failure");
                    };
                    assert_eq!(format!("{reason:?}"), expected);
                });
                server
                    .finish()
                    .expect("SCRAM peer execution")
                    .expect("SCRAM peer protocol");
            },
        )
        .expect("SCRAM peer");
    }
}
