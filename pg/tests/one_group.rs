use cartel_pg::api::traits::{
    Begin as _, Client as _, Notify as _, Query as _, Status as _, Stream as _, Transaction as _,
};
use dope::core::driver::life::recovery;
use std::{num::NonZero, pin::pin, time::Duration};

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
        let prepared = topology::try_new::<_, Bundle<Tcp, Identity, Throughput>>(
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
    use common::postgres::startup;
    use dope::{
        core::{
            driver::route::{KeyTag, table::conn::Capacity},
            io::socket,
        },
        manifold::{app::receive, network::listener, service},
        net::wire::domain,
        runtime::topology::lending::Stage,
    };
    use dope_fiber::{abi::Join, net::server};

    let bound = socket::Listener::bind(([127, 0, 0, 1], 0).into()).expect("peer listener");
    let addr = bound.local_addr();
    let snapshot = service::Snapshot::from_one(
        service::Revision::new(1),
        service::Endpoint::new(addr, addr),
    );
    let (_, node) = port::Port::<Database>::plan(
        Queries::try_config().expect("credentials"),
        Queries::try_groups(
            NonZero::new(3).expect("records"),
            NonZero::new(256).expect("bytes"),
        )
        .expect("bounded group config"),
    )
    .assemble::<Bundle<Tcp, Identity, Throughput>>(
        snapshot,
        Duration::from_millis(1),
        Duration::from_secs(1),
    )
    .expect("group plan")
    .into_parts();
    let peer = server::Plan::<Identity, KeyTag<1>>::new(Capacity::fixed::<1>(), Identity);
    let layout =
        listener::topology::Layout::<KeyTag<1>, receive::Owned>::new(Capacity::fixed::<1>());
    let prepared = dope::runtime::topology::Plan::new((), (node, layout))
        .for_install::<Throughput>()
        .expect("group topology");
    let mut reports = Reports::new(prepared.layout(), [Id::fixed::<0>(), Id::fixed::<1>()]);
    let mut report = Completion::new();
    prepared
        .with_local(
            |worker| {
                worker.lend(|(node, _), stage| {
                    node.install(stage, |host, stage| {
                        let hash = stage.hash_state(listener::Domain::DEFAULT);
                        let port = peer
                            .try_build(stage.driver().storage)
                            .expect("peer storage");
                        server::Listener::<KeyTag<1>, Tcp, Identity, _>::adopt(
                            &port,
                            server::Config {
                                direct_flights: 0.into(),
                                bind: addr,
                                backlog: socket::Backlog::fixed::<1>(),
                                stream: Default::default(),
                                transport: Default::default(),
                                egress: Default::default(),
                                bound,
                                wire: domain::Local::new(Identity),
                            },
                            |cause| cause.eliminate(),
                            hash,
                            stage,
                            Stage::driver,
                            |listener, stage| {
                                let peer = listener.handle();
                                stage.with_quiesce(
                                    &mut report,
                                    topology::App::new(host, listener),
                                    GUARD,
                                    |mut runtime| {
                                        let client = runtime.client(topology::App::connector);
                                        let mut io = fibers::TEST
                                            .try_run(&mut runtime, dope_fiber::fiber!('_ => async move {
                                                let mut io = peer.accept().await.ok().expect("peer connection");
                                                startup::request(&mut io, true).await;
                                                assert!(io.write_all(startup::AUTHENTICATED).await.is_ok());
                                                startup::request(&mut io, false).await;
                                                assert!(io.write_all(group::CATALOG_READY).await.is_ok());
                                                client.status().wait_ready().await.ok().expect("ready PostgreSQL");
                                                io
                                            }))
                                            .expect("startup driver");
                                        let mut stream = client.query().run_stream::<TextQuery>(());
                                        let mut group = pin!(Task::new(
                                            client.query().run_group::<TextQuery, 2>([(); 2])
                                        ));
                                        let (rows, mut io) = fibers::TEST
                                            .try_run(&mut runtime, dope_fiber::fiber!('_ => async move {
                                                assert!(fibers::TEST.poll_once(stream.next_row()).await.is_none());
                                                assert!(fibers::TEST.poll_once(group.as_mut()).await.is_none());
                                                startup::request(&mut io, false).await;
                                                assert!(io.write_all(common::transaction::DETACHED_RESPONSES[0]).await.is_ok());
                                                drop(stream);
                                                assert!(fibers::TEST.poll_once(group.as_mut()).await.is_none());
                                                startup::request(&mut io, false).await;
                                                assert!(io.write_all(group::ROWS_COMPLETED).await.is_ok());
                                                let marker = client.notify().next().await.ok().expect("completion marker");
                                                assert_eq!(marker.channel(), "group");
                                                assert!(fibers::TEST.poll_once(group.as_mut()).await.is_none());
                                                assert!(io.write_all(startup::IDLE).await.is_ok());
                                                let rows = group.as_mut().await.ok().expect("completed group");
                                                (rows, io)
                                            }))
                                            .expect("group driver");
                                        let peer = &mut io;
                                        let exchange = dope_fiber::fiber!('_ => async move {
                                            startup::request(peer, false).await;
                                            assert!(peer.write_all(group::NEXT_ROW).await.is_ok());
                                        });
                                        let (next, ()) = fibers::TEST
                                            .try_run(&mut runtime, Join::new(
                                                client.query().run_group::<TextQuery, 1>([()]),
                                                exchange,
                                            ))
                                            .expect("next group driver");
                                        let next = next.ok().expect("active group owner released");
                                        assert_eq!(rows[0].decode().expect("retained first").name, "first");
                                        assert_eq!(rows[1].decode().expect("retained second").lease, 7);
                                        assert_eq!(next[0].decode().expect("next row").lease, 11);
                                    },
                                )
                            },
                        )
                        .expect("install peer")
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
}

#[test]
fn errors_survive_finish() {
    TcpScript::with(group::transaction, |server| {
        let prepared = topology::try_new::<_, Bundle<Tcp, Identity, Throughput>>(
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
        let prepared = topology::try_new::<_, Bundle<Tcp, Identity, Throughput>>(
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
                let prepared = topology::try_new::<_, Bundle<Tcp, Identity, Throughput>>(
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
            let prepared = topology::try_new::<_, Bundle<Tcp, Identity, Throughput>>(
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
