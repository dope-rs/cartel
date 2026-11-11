use std::{num::NonZero, time::Duration};

use cartel_pg::{
    api::{
        results::group::{Diagnostics as _, Outcome, Sync},
        traits::{Client as _, Query as _, Status as _},
    },
    port, reply,
};
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

use common::{
    port::Queries,
    postgres::group,
    topology,
    wire::checked::{Database, TextQuery},
};

pub mod common;

#[test]
fn missing_keeps_rows() {
    TcpScript::with(group::missing, |server| {
        let config = Queries::try_groups(NonZero::<usize>::MIN, NonZero::<usize>::MIN)
            .expect("group config");
        let prepared = topology::try_new::<_, Bundle<Tcp, Identity, Throughput>>(
            port::Port::<Database>::plan(Queries::try_config().expect("credentials"), config),
            server.addr(),
            Duration::from_millis(1),
            Duration::from_secs(1),
        )
        .expect("group topology");
        let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
        let mut report = Completion::new();
        prepared
            .with_local(
                |worker| {
                    worker.lend(|node, stage| {
                        node.install(stage, |host, stage| {
                            stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                                let client = runtime.root_client();
                                runtime
                                    .block_on(client.status().wait_ready())
                                    .expect("ready driver")
                                    .ok()
                                    .expect("ready PostgreSQL");
                                let failure = runtime
                                    .block_on(client.run_group::<TextQuery, 3>([(); 3]))
                                    .expect("group driver")
                                    .err()
                                    .expect("missing row");
                                let [next] = runtime
                                    .block_on(client.run_group::<TextQuery, 1>([()]))
                                    .expect("next group driver")
                                    .unwrap_or_else(|failure| {
                                        panic!("next group: {:?}", failure.cause().map(common::port::Reply::from))
                                    });
                                assert_eq!(next.decode().expect("next row").lease, 11);
                                assert_eq!(failure.sync(), Sync::Complete);
                                assert!(failure.diagnostics().is_empty());
                                assert!(failure.cause().is_none());
                                assert!(failure.sync_error().is_none());
                                assert!(matches!(failure.outcomes(), [
                                    Outcome::Completed(first),
                                    Outcome::Failed(error),
                                    Outcome::Completed(last),
                                ] if matches!(error.kind(), reply::Kind::Driver(cartel_pg::Error::NotFound))
                                    && first.decode().expect("first row").lease == 3
                                    && last.decode().expect("last row").lease == 7));
                            })
                        })
                    })
                },
                recovery::Policy::new(|event| reports.push(event), common::recover),
                |outcome| outcome.ok().expect("application entry"),
            )
            .expect("group installation")
            .expect("group session");
        assert!(reports.events().expect("native reclamation").is_empty());
        server.finish().expect("peer execution").expect("peer protocol");
    })
    .expect("group peer");
}
