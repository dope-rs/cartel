use std::num::NonZero;

use cartel_pg::{
    api::{
        results::group::{Diagnostics as _, Outcome, Sync},
        traits::{Client as _, Query as _, Status as _},
    },
    port, reply,
};
use dope::manifold::timing::Throughput;
use dope_fiber::BlockOn as _;
use dope_test::harness::peer::TcpScript;

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
        let plan = port::Port::<Database>::plan(Queries::try_config().expect("credentials"), config);
        topology::run(plan, server.addr(), (1, 1_000), Throughput, |mut runtime| {
            let client = runtime.root_client();
            runtime
                .block_on(client.status().wait_ready())
                .expect("ready driver")
                .ok()
                .expect("ready PostgreSQL");
            let pending = client.run_group::<TextQuery, 1>([()]);
            let refused = runtime.block_on(client.run_group::<TextQuery, 1>([()]));
            assert!(matches!(refused, Ok(Err(group)) if matches!(
                group.cause(), Some(reply::Error::Driver(cartel_pg::Error::GroupOwnerCapacity))
            )));
            drop(pending);
            let failure = runtime
                .block_on(client.run_group::<TextQuery, 3>([(); 3]))
                .expect("group driver")
                .err()
                .expect("missing row");
            let [next] = runtime
                .block_on(client.run_group::<TextQuery, 1>([()]))
                .expect("next group driver")
                .map_err(|failure| {
                    let [outcome] = failure.outcomes();
                    let outcome = match outcome {
                        Outcome::Completed(_) => Outcome::Completed(()),
                        Outcome::Failed(error) => {
                            Outcome::Failed(error.kind())
                        }
                        Outcome::Interrupted(error) => {
                            Outcome::Interrupted(error.kind())
                        }
                        Outcome::Server => Outcome::Server,
                        Outcome::Skipped => Outcome::Skipped,
                        Outcome::Unobserved => Outcome::Unobserved,
                    };
                    format!(
                        "{:?}",
                        (
                            outcome,
                            failure.sync(),
                            failure.cause().map(common::port::Reply::from),
                            failure.sync_error().map(common::port::Reply::from),
                            failure.diagnostics().iter().collect::<Vec<_>>(),
                        )
                    )
                })
                .expect("next group");
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
        });
        server.finish().expect("peer execution").expect("peer protocol");
    })
    .expect("group peer");
}
