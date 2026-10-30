use cartel_pg::api::traits::{
    Begin as _, Client as _, Notify as _, Query as _, Status as _, Transaction as _,
};
use dope::core::driver::life::recovery;
use std::{io, num::NonZero, pin::pin, sync::mpsc, time::Duration};

use cartel_pg::{
    api::{
        results::group::{Diagnostics as _, Outcome, Position, Sync},
        tx::FinishError,
    },
    port,
    port::config,
    reply,
};
use common::{
    port::{Queries, Reply},
    postgres::group::{self, COMMAND_ERROR},
    topology,
    wire::checked::{Database, TextQuery},
};
use dope::{
    core::driver::route::Id,
    manifold::{app::env::Bundle, timing::Throughput},
    net::{tcp::Tcp, wire::Identity},
    runtime::executor::session::Completion,
};
use dope_fiber::{BlockOn as _, abi::Task};
use dope_test::{
    fibers,
    harness::{GUARD, peer::TcpScript},
    reclaim::Reports,
};
use o3::base::time::Positive;

pub mod common;

#[test]
fn deadline_keeps_results() {
    TcpScript::with(group::settled, |server| {
        let config = Queries::try_groups(
            NonZero::new(3).expect("records"),
            NonZero::new(256).expect("bytes"),
        )
        .expect("bounded group config");
        let config =
            config::Config::with_window(config.capacities(), Positive::from_millis::<50>())
                .and_then(|timed| timed.with_groups(config.groups().expect("group limits")))
                .expect("bounded group deadline");
        let prepared = topology::Prepare::try_new::<Bundle<Tcp, Identity, Throughput>>(
            port::Port::<Database>::plan(Queries::try_config().expect("credentials"), config),
            server.addr(),
            Duration::from_millis(1),
            Duration::from_secs(1),
        )
        .expect("group topology");
        let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
        let mut report = Completion::new();
        prepared.with_local( |worker| worker.lend(|node, stage| {
            node.install(stage, |host, stage| {
                stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                    let client = runtime.root_client();
                    runtime.block_on(client.status().wait_ready()).expect("ready driver").ok().expect("ready PostgreSQL");
                    let mut group = pin!(Task::new(client.query().run_group::<TextQuery, 3>([(); 3])));
                    assert!(runtime.block_on(fibers::TEST.poll_once(group.as_mut())).expect("admission driver").is_none());
                    let marker = runtime.block_on(client.notify().next()).expect("notification driver").ok().expect("Sync marker");
                    assert_eq!(marker.payload(), "settled");
                    fibers::TEST.try_pause(&mut runtime, Duration::from_millis(50)).expect("group deadline driver");
                    let failure = runtime.block_on(group.as_mut()).expect("group driver").err().expect("expired group");
                    assert_eq!(failure.sync(), Sync::Complete);
                    assert!(matches!(failure.outcomes(), [Outcome::Completed(row), Outcome::Server, Outcome::Skipped] if row.decode().expect("completed row").lease == 3));
                    assert_eq!(failure.diagnostic(1).expect("command error").code().as_str(), "23505");
                    assert!(matches!(failure.cause(), Some(reply::Error::Driver(cartel_pg::Error::RequestTimeout(elapsed))) if elapsed.phase() == cartel_pg::RequestPhase::Response));
                })
            })
        }), recovery::Policy::new(|event| reports.push(event), common::recover),
|outcome| {
    assert!(outcome.is_ok(), "{:?}", outcome.as_ref().err().map(|error| error.cause()));
    outcome.ok().expect("application entry")
}).expect("group installation").expect("group session");
        assert!(reports.events().expect("native reclamation").is_empty());
        server
            .finish()
            .expect("group peer execution")
            .expect("group peer protocol");
    })
    .expect("group peer");
}

#[test]
fn rows_outlive_group() {
    let (release, ready) = mpsc::sync_channel(1);
    TcpScript::with(
        move |socket| {
            group::delay_ready(socket)?;
            ready
                .recv_timeout(Duration::from_secs(1))
                .map_err(io::Error::other)?;
            group::resume_ready(socket)
        },
        |server| {
            let prepared = topology::Prepare::try_new::<Bundle<Tcp, Identity, Throughput>>(
                port::Port::<Database>::plan(
                    Queries::try_config().expect("credentials"),
                    Queries::try_groups(
                        NonZero::new(3).expect("records"),
                        NonZero::new(256).expect("bytes"),
                    )
                    .expect("bounded group config"),
                ),
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
                                    let mut group = pin!(Task::new(
                                        client.query().run_group::<TextQuery, 2>([(); 2])
                                    ));
                                    assert!(
                                        runtime
                                            .block_on(fibers::TEST.poll_once(group.as_mut()))
                                            .expect("admission driver")
                                            .is_none()
                                    );
                                    let marker = runtime
                                        .block_on(client.notify().next())
                                        .expect("notification driver")
                                        .ok()
                                        .expect("completion marker");
                                    assert_eq!(marker.channel(), "group");
                                    assert!(
                                        runtime
                                            .block_on(fibers::TEST.poll_once(group.as_mut()))
                                            .expect("pending driver")
                                            .is_none()
                                    );
                                    release.send(()).expect("release Sync");
                                    let rows = runtime
                                        .block_on(group.as_mut())
                                        .expect("group driver")
                                        .ok()
                                        .expect("completed group");
                                    let next = runtime
                                        .block_on(client.query().run_group::<TextQuery, 1>([()]))
                                        .expect("next group driver")
                                        .ok()
                                        .expect("active group owner released");
                                    assert_eq!(
                                        rows[0].decode().expect("retained first").name,
                                        "first"
                                    );
                                    assert_eq!(rows[1].decode().expect("retained second").lease, 7);
                                    assert_eq!(next[0].decode().expect("next row").lease, 11);
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
                )
                .expect("group installation")
                .expect("group session");
            assert!(reports.events().expect("native reclamation").is_empty());
            server
                .finish()
                .expect("group peer execution")
                .expect("group peer protocol");
        },
    )
    .expect("group peer");
}

#[test]
fn errors_survive_finish() {
    TcpScript::with(group::transaction, |server| {
        let prepared = topology::Prepare::try_new::<Bundle<Tcp, Identity, Throughput>>(
            port::Port::<Database>::plan(
                Queries::try_config().expect("credentials"),
                Queries::try_groups(
                    NonZero::new(3).expect("records"),
                    NonZero::new(256).expect("bytes"),
                )
                .expect("bounded group config"),
            ),
            server.addr(),
            Duration::from_millis(1),
            Duration::from_secs(1),
        )
        .expect("group topology");
        let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
        let mut report = Completion::new();
        prepared.with_local( |worker| worker.lend(|node, stage| {
            node.install(stage, |host, stage| {
                stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                    let client = runtime.root_client();
                    runtime.block_on(client.status().wait_ready()).expect("ready driver").ok().expect("ready PostgreSQL");
                    let tx = runtime.block_on(client.transaction().begin()).expect("begin driver").ok().expect("transaction");
                    let outcome = runtime.block_on(tx.query().run_group::<TextQuery, 3>([(); 3])).expect("group driver");
                    let result = runtime.block_on(tx.finish(outcome)).expect("finish driver");
                    let error = result.err().expect("group failure");
                    let failure = match error {
                        FinishError::Operation(failure) => Ok(failure),
                        FinishError::Commit(error) | FinishError::Rollback { rollback: error, .. } => Err(error),
                    };
                    assert!(failure.is_ok(), "transaction completion error: {:?}", failure.as_ref().err().map(Reply::from));
                    let failure = failure.ok().expect("rolled back group failure");
                    assert_eq!(failure.sync(), Sync::Complete);
                    assert!(matches!(failure.outcomes(), [Outcome::Completed(row), Outcome::Server, Outcome::Skipped] if row.decode().expect("retained row").name == "retained"));
                    let details: Vec<_> = failure.diagnostics().iter().map(|entry| (entry.position, entry.fields.code().as_str(), entry.fields.message())).collect();
                    assert_eq!(details, [(Position::Command(1), "23505", "duplicate"), (Position::Recovery, "XX000", "recovery")]);
                })
            })
        }), recovery::Policy::new(|event| reports.push(event), common::recover),
|outcome| {
    assert!(outcome.is_ok(), "{:?}", outcome.as_ref().err().map(|error| error.cause()));
    outcome.ok().expect("application entry")
}).expect("group installation").expect("group session");
        assert!(reports.events().expect("native reclamation").is_empty());
        server
            .finish()
            .expect("group peer execution")
            .expect("group peer protocol");
    })
    .expect("group peer");
}

#[test]
fn sync_keeps_commands() {
    TcpScript::with(group::late_error, |server| {
        let prepared = topology::Prepare::try_new::<Bundle<Tcp, Identity, Throughput>>(
            port::Port::<Database>::plan(
                Queries::try_config().expect("credentials"),
                Queries::try_groups(
                    NonZero::new(3).expect("records"),
                    NonZero::new(256).expect("bytes"),
                )
                .expect("bounded group config"),
            ),
            server.addr(),
            Duration::from_millis(1),
            Duration::from_secs(1),
        )
        .expect("group topology");
        let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
        let mut report = Completion::new();
        prepared.with_local( |worker| worker.lend(|node, stage| {
            node.install(stage, |host, stage| {
                stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                    let client = runtime.root_client();
                    runtime.block_on(client.status().wait_ready()).expect("ready driver").ok().expect("ready PostgreSQL");
                    let failure = runtime.block_on(client.query().run_group::<TextQuery, 2>([(); 2])).expect("group driver").err().expect("late Sync failure");
                    assert_eq!(failure.sync(), Sync::Complete);
                    assert!(matches!(failure.outcomes(), [Outcome::Completed(first), Outcome::Completed(second)] if first.decode().expect("first row").lease == 3 && second.decode().expect("second row").lease == 7));
                    let detail = failure.diagnostics().iter().next().expect("Sync diagnostic");
                    assert_eq!((detail.position, detail.fields.code().as_str()), (Position::Sync, "23505"));
                })
            })
        }), recovery::Policy::new(|event| reports.push(event), common::recover),
|outcome| {
    assert!(outcome.is_ok(), "{:?}", outcome.as_ref().err().map(|error| error.cause()));
    outcome.ok().expect("application entry")
}).expect("group installation").expect("group session");
        assert!(reports.events().expect("native reclamation").is_empty());
        server
            .finish()
            .expect("group peer execution")
            .expect("group peer protocol");
    })
    .expect("group peer");
}

#[test]
fn classify_error_scope() {
    for (detail, ready, position) in group::SCOPES {
        TcpScript::with(
            |socket| group::classified(socket, detail, ready),
            |server| {
                let prepared = topology::Prepare::try_new::<Bundle<Tcp, Identity, Throughput>>(
                    port::Port::<Database>::plan(
                        Queries::try_config().expect("credentials"),
                        Queries::try_groups(
                            NonZero::new(3).expect("records"),
                            NonZero::new(256).expect("bytes"),
                        )
                        .expect("bounded group config"),
                    ),
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
                                            .block_on(
                                                client.query().run_group::<TextQuery, 2>([(); 2]),
                                            )
                                            .expect("group driver")
                                            .err()
                                            .expect("server failure");
                                        let detail = failure
                                            .diagnostics()
                                            .iter()
                                            .next()
                                            .expect("retained diagnostic");
                                        assert_eq!(detail.position, position);
                                        if ready {
                                            assert_eq!(failure.sync(), Sync::Complete);
                                            assert!(matches!(
                                                failure.outcomes(),
                                                [Outcome::Server, Outcome::Skipped]
                                            ));
                                        } else {
                                            assert_eq!(failure.sync(), Sync::Unobserved);
                                            assert!(matches!(
                                                failure.outcomes(),
                                                [Outcome::Interrupted(_), Outcome::Interrupted(_)]
                                            ));
                                            assert!(failure.sync_error().is_some());
                                        }
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
                    )
                    .expect("group installation")
                    .expect("group session");
                assert!(reports.events().expect("native reclamation").is_empty());
                server
                    .finish()
                    .expect("group peer execution")
                    .expect("group peer protocol");
            },
        )
        .expect("group peer");
    }
}

#[test]
fn capacity_keeps_error() {
    for records in [true, false] {
        TcpScript::with(group::overflow, |server| {
            let (count, bytes) = if records {
                (1, 256)
            } else {
                (3, COMMAND_ERROR.len())
            };
            let prepared = topology::Prepare::try_new::<Bundle<Tcp, Identity, Throughput>>(
                port::Port::<Database>::plan(
                    Queries::try_config().expect("credentials"),
                    Queries::try_groups(
                        NonZero::new(count).expect("records"),
                        NonZero::new(bytes).expect("bytes"),
                    )
                    .expect("bounded group config"),
                ),
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
                                        .block_on(client.query().run_group::<TextQuery, 2>([(); 2]))
                                        .expect("group driver")
                                        .err()
                                        .expect("diagnostic capacity failure");
                                    assert_eq!(failure.diagnostics().len(), 1);
                                    let first = failure.diagnostic(0).expect("first diagnostic");
                                    assert_eq!(
                                        (first.code().as_str(), first.message()),
                                        ("23505", "duplicate")
                                    );
                                    assert_eq!(failure.sync(), Sync::Unobserved);
                                    assert!(matches!(
                                        failure.outcomes(),
                                        [Outcome::Server, Outcome::Interrupted(_)]
                                    ));
                                    assert!(match failure.sync_error() {
                                        Some(reply::Error::Driver(
                                            cartel_pg::Error::GroupDiagnosticRecords { capacity },
                                        )) if records => capacity.get() == 1,
                                        Some(reply::Error::Driver(
                                            cartel_pg::Error::GroupDiagnosticBytes(_),
                                        )) if !records => true,
                                        _ => false,
                                    });
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
                )
                .expect("group installation")
                .expect("group session");
            assert!(reports.events().expect("native reclamation").is_empty());
            server
                .finish()
                .expect("group peer execution")
                .expect("group peer protocol");
        })
        .expect("group peer");
    }
}
