use cartel_pg::api::traits::{
    Begin as _, Client as _, Notify as _, Query as _, Status as _, Stream as _, Transaction as _,
    Unchecked as _,
};
use dope::core::driver::life::recovery;
use dope_test::harness::peer;
use std::{pin::pin, time::Duration};

use cartel_pg::{api::tx::FinishError, port, port::config, protocol::PickPolicy, reply};
use common::{
    port::*,
    postgres::{accept::Listener, retire, startup::Stream},
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
use dope_test::{fibers, harness::GUARD, reclaim::Reports};
use o3::base::time::Positive;

type Env = Bundle<Tcp, Identity, Throughput>;

pub mod common;

#[test]
fn transaction_does_not_join_open_stream() {
    for policy in [PickPolicy::RoundRobin, PickPolicy::LeastInflight] {
        peer::AcceptScript::with(common::transaction::streamed, |server| {
            let prepared = topology::try_new::<_, Env>(
                port::Port::<Database>::plan(
                    Queries::try_config().expect("database config"),
                    Queries::try_cancel().expect("two in-flight replies"),
                ),
                server.addr(),
                Duration::from_millis(1),
                Duration::from_secs(1),
            )
            .expect("transaction topology");
            let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
            let mut report = Completion::new();
            prepared.with_local(
                |worker| {
                    worker.lend(|node, stage| {
                        node.install(stage, |host, stage| {
                            stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                                let client = runtime.root_client();
                                client.status().set_policy(policy);
                                assert!(matches!(
                                    runtime.block_on(client.status().wait_ready()),
                                    Ok(Ok(()))
                                ));
                                runtime
                                    .block_on(dope_fiber::fiber!('_ => async move {
                                        let mut stream = client.query().run_stream::<TextQuery>(());
                                        ::core::assert!(
                                            fibers::TEST.poll_once(stream.next_row()).await.is_none()
                                        );
                                        let tx = client.transaction().begin().await.ok().expect("independent transaction");
                                        let error = stream.next_row().await.err().expect("stream error");
                                        ::core::assert!(tx.rollback().await.is_ok());
                                        let fields = error.as_server().expect("retained stream error").fields();
                                        ::core::assert_eq!((fields.code().as_str(), fields.message()), ("23505", "duplicate"));
                                    }))
                                    .expect("stream and transaction driver");
                            })
                        })
                    })
                },
                recovery::Policy::new(|event| reports.push(event), common::recover),
                |result| assert!(matches!(result, Ok(Ok(Ok(())))), "transaction scope"),
            );
            assert!(reports.events().expect("reclamation").is_empty());
            assert!(matches!(server.finish(), Ok(Ok(()))), "stream transaction peer");
        })
        .expect("spawn transaction peer");
    }
}

#[test]
fn throttled_query_cannot_enter_later_transaction() {
    peer::AcceptScript::with(
        common::transaction::throttled,
        |server| {
            let mut capacity = Queries::try_cancel().expect("PG capacity").capacities();
            capacity.inflight = 1;
            let prepared = topology::try_new::<_, Env>(
                port::Port::<Queries>::plan(
                    Queries::try_config().expect("database config"),
                    config::Config::new(capacity).expect("single reply"),
                ),
                server.addr(),
                Duration::from_millis(1),
                Duration::from_secs(1),
            )
            .expect("transaction topology");
            let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
            let mut report = Completion::new();
            prepared.with_local(
                |worker| {
                    worker.lend(|node, stage| {
                        node.install(stage, |host, stage| {
                            stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                                let client = runtime.root_client();
                                assert!(matches!(
                                    runtime.block_on(client.status().wait_ready()),
                                    Ok(Ok(()))
                                ));
                                let mut first =
                                    pin!(Task::new(client.unchecked().command("SELECT 1")));
                                let mut waiting =
                                    pin!(Task::new(client.unchecked().command("SELECT 2")));
                                runtime
                                    .block_on(dope_fiber::fiber!('_ => async move {
                                        ::core::assert!(fibers::TEST.poll_once(first.as_mut()).await.is_none());
                                        ::core::assert!(fibers::TEST.poll_once(waiting.as_mut()).await.is_none());
                                        ::core::assert!(matches!(first.as_mut().await, Ok(())));
                                        let tx = client.transaction().begin().await
                                            .ok().expect("exclusive transaction");
                                        ::core::assert!(matches!(
                                            waiting.as_mut().await,
                                            Err(reply::Error::Driver(cartel_pg::Error::Backpressure { .. }))
                                        ));
                                        ::core::assert!(matches!(tx.commit().await, Ok(())));
                                        ::core::assert!(matches!(client.unchecked().command("SELECT 1").await, Ok(())));
                                    }))
                                    .expect("throttled transaction driver");
                            })
                        })
                    })
                },
                recovery::Policy::new(|event| reports.push(event), common::recover),
                |result| assert!(matches!(result, Ok(Ok(Ok(())))), "transaction scope"),
            );
            assert!(reports.events().expect("reclamation").is_empty());
            assert!(matches!(server.finish(), Ok(Ok(()))), "transaction peer");
        },
    )
    .expect("spawn transaction peer");
}

#[test]
fn transaction_status_rejects_stale_guard() {
    for (invalid_begin, unmanaged_begin) in [(false, false), (true, false), (false, true)] {
        peer::AcceptScript::with(
            move |listener| common::transaction::ended(listener, invalid_begin, unmanaged_begin),
            |server| {
                let prepared = topology::try_new::<_, Env>(
                    port::Port::<Queries>::plan(
                        Queries::try_config().expect("database config"),
                        Queries::try_cancel().expect("two in-flight replies"),
                    ),
                    server.addr(),
                    Duration::from_millis(1),
                    Duration::from_secs(1),
                )
                .expect("transaction topology");
                let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
                let mut report = Completion::new();
                prepared.with_local(
                    |worker| {
                        worker.lend(|node, stage| {
                            node.install(stage, |host, stage| {
                                stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                                    let client = runtime.root_client();
                                    assert!(matches!(
                                        runtime.block_on(client.status().wait_ready()),
                                        Ok(Ok(()))
                                    ));
                                    assert!(matches!(
                                        runtime.block_on(fibers::TEST.poll_once(
                                            client.unchecked().command(if unmanaged_begin {
                                                "BEGIN"
                                            } else {
                                                "SELECT 1"
                                            })
                                        )),
                                        Ok(None)
                                    ));
                                    let started = runtime
                                        .block_on(client.transaction().begin())
                                        .expect("begin driver");
                                    let stale = if invalid_begin || unmanaged_begin {
                                        assert!(matches!(
                                            started,
                                            Err(reply::Error::Driver(cartel_pg::Error::Protocol(
                                                cartel_pg::protocol::Reason::TransactionLease
                                            )))
                                        ));
                                        None
                                    } else {
                                        let tx = started.ok().expect("pipelined transaction");
                                        assert!(matches!(
                                            runtime.block_on(tx.unchecked().command("ROLLBACK")),
                                            Ok(Err(reply::Error::Driver(
                                                cartel_pg::Error::Protocol(
                                                    cartel_pg::protocol::Reason::TransactionLease
                                                )
                                            )))
                                        ));
                                        assert!(matches!(
                                            runtime.block_on(tx.unchecked().command("SELECT 1")),
                                            Ok(Err(reply::Error::Pinned(_)))
                                        ));
                                        Some(tx)
                                    };
                                    assert!(matches!(
                                        runtime.block_on(client.unchecked().command("SELECT 1")),
                                        Ok(Ok(()))
                                    ));
                                    drop(stale);
                                    assert!(matches!(
                                        runtime.block_on(client.unchecked().command("SELECT 1")),
                                        Ok(Ok(()))
                                    ));
                                })
                            })
                        })
                    },
                    recovery::Policy::new(|event| reports.push(event), common::recover),
                    |result| assert!(matches!(result, Ok(Ok(Ok(())))), "transaction scope"),
                );
                assert!(reports.events().expect("reclamation").is_empty());
                assert!(matches!(server.finish(), Ok(Ok(()))), "transaction peer");
            },
        )
        .expect("spawn transaction peer");
    }
}

#[test]
fn finalizer_preserves_status_error() {
    for rejected in [false, true] {
        peer::AcceptScript::with(
            move |listener| common::transaction::unfinished(listener, rejected),
            |server| {
                let prepared = topology::try_new::<_, Env>(
                    port::Port::<Queries>::plan(
                        Queries::try_config().expect("database config"),
                        Queries::try_cancel().expect("transaction capacity"),
                    ),
                    server.addr(),
                    Duration::from_millis(1),
                    Duration::from_secs(1),
                )
                .expect("transaction topology");
                let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
                let mut report = Completion::new();
                prepared.with_local(
                    |worker| {
                        worker.lend(|node, stage| {
                            node.install(stage, |host, stage| {
                                stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                                    let client = runtime.root_client();
                                    let tx = runtime
                                        .block_on(client.transaction().begin())
                                        .expect("begin driver")
                                        .ok()
                                        .expect("transaction");
                                    let error = runtime
                                        .block_on(tx.commit())
                                        .expect("commit driver")
                                        .expect_err("transaction remained open");
                                    if rejected {
                                        let fields = error
                                            .as_server()
                                            .expect("original server error")
                                            .fields();
                                        assert_eq!(
                                            (fields.code().as_str(), fields.message()),
                                            ("23505", "duplicate")
                                        );
                                    } else {
                                        assert!(matches!(
                                            error,
                                            reply::Error::Driver(cartel_pg::Error::Protocol(
                                                cartel_pg::protocol::Reason::TransactionOpen
                                            ))
                                        ));
                                    }
                                    assert!(matches!(
                                        runtime.block_on(client.unchecked().command("SELECT 1")),
                                        Ok(Ok(()))
                                    ));
                                })
                            })
                        })
                    },
                    recovery::Policy::new(|event| reports.push(event), common::recover),
                    |result| assert!(matches!(result, Ok(Ok(Ok(())))), "transaction scope"),
                );
                assert!(reports.events().expect("reclamation").is_empty());
                assert!(matches!(server.finish(), Ok(Ok(()))), "transaction peer");
            },
        )
        .expect("spawn transaction peer");
    }
}

#[test]
fn finalizer_keeps_error() {
    for reject in [false, true] {
        peer::AcceptScript::with(
            |listener| listener.finalizer(),
            |server| {
                let mut capacity = Queries::try_cancel()
                    .expect("bounded PostgreSQL config")
                    .capacities();
                capacity.inflight = 1;
                let config = config::Config::with_window(capacity, Positive::from_millis::<100>())
                    .expect("finalizer deadline");
                let prepared = topology::try_new::<_, Env>(
                    port::Port::<Queries>::plan(
                        Queries::try_config().expect("database config"),
                        config,
                    ),
                    server.addr(),
                    Duration::from_millis(1),
                    Duration::from_secs(1),
                )
                .expect("finalizer topology");
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
                                    .expect("PostgreSQL ready");
                                let tx = runtime
                                    .block_on(client.transaction().begin())
                                    .expect("begin driver")
                                    .ok()
                                    .expect("transaction");
                                let cause = String::from("operation rejected");
                                let outcome = if reject { Err(cause.as_str()) } else { Ok(()) };
                                let mut finish = pin!(Task::new(tx.finish(outcome)));
                                assert!(
                                    runtime
                                        .block_on(fibers::TEST.poll_once(finish.as_mut()))
                                        .expect("finalizer admission driver")
                                        .is_none()
                                );
                                let marker = runtime
                                    .block_on(client.notify().next())
                                    .expect("notification driver")
                                    .ok()
                                    .expect("finalizer error marker");
                                assert_eq!(marker.channel(), "finalizer");
                                assert!(
                                    runtime
                                        .block_on(fibers::TEST.poll_once(finish.as_mut()))
                                        .expect("settlement driver")
                                        .is_none(),
                                    "the received finalizer error still awaits ReadyForQuery"
                                );
                                let failure = runtime
                                    .block_on(finish.as_mut())
                                    .expect("finalizer driver")
                                    .expect_err("finalizer failure");
                                let error = match (reject, failure) {
                                    (false, FinishError::Commit(error)) => Some(error),
                                    (
                                        true,
                                        FinishError::Rollback {
                                            operation: prior,
                                            rollback,
                                        },
                                    ) => {
                                        assert_eq!(prior, cause);
                                        Some(rollback)
                                    }
                                    _ => None,
                                }
                                .expect("matching finalizer failure");
                                let fields =
                                    error.as_server().expect("retained server error").fields();
                                assert_eq!(
                                    (fields.code().as_str(), fields.message()),
                                    ("23505", "finish rejected")
                                );
                                assert!(matches!(
                                    runtime.block_on(client.unchecked().command("SELECT 1")),
                                    Ok(Ok(()))
                                ));
                                let fields = error
                                    .as_server()
                                    .expect("error survives reconnect")
                                    .fields();
                                assert_eq!(
                                    (fields.code().as_str(), fields.message()),
                                    ("23505", "finish rejected")
                                );
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
                    .expect("finalizer installation")
                    .expect("finalizer session");
                assert!(reports.events().expect("native reclamation").is_empty());
                server
                    .finish()
                    .expect("finalizer peer")
                    .expect("finalizer protocol");
            },
        )
        .expect("finalizer listener");
    }
}

#[test]
fn retains_next_owner() {
    peer::AcceptScript::with(
        |listener| {
            let mut socket = listener.accept_within(Duration::from_secs(1))?;
            socket.try_start(None)?;
            retire::finalize(&mut socket)
        },
        |server| {
            let prepared = topology::try_new::<_, Env>(
                port::Port::<Queries>::plan(
                    Queries::try_config().expect("database config"),
                    Queries::try_cancel().expect("two in-flight replies on one connection"),
                ),
                server.addr(),
                Duration::from_millis(1),
                Duration::from_secs(1),
            )
            .expect("finalizer topology");
            let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
            let mut report = Completion::new();
            prepared
                .with_local(
                    |worker| {
                        worker.lend(|node, stage| {
                            node.install(stage, |host, stage| {
                                stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                                    let client = runtime.root_client();
                                    assert!(matches!(
                                        runtime.block_on(client.status().wait_ready()),
                                        Ok(Ok(()))
                                    ));
                                    let next = {
                                        let tx = runtime
                                            .block_on(client.transaction().begin())
                                            .expect("begin driver")
                                            .ok()
                                            .expect("first transaction");
                                        let mut old = pin!(Task::new(tx.commit()));
                                        assert!(
                                            runtime
                                                .block_on(fibers::TEST.poll_once(old.as_mut()))
                                                .expect("finalizer admission driver")
                                                .is_none()
                                        );
                                        runtime
                                            .block_on(client.transaction().begin())
                                            .expect("next begin driver")
                                            .ok()
                                            .expect("next transaction after ReadyForQuery")
                                    };
                                    let result = runtime
                                        .block_on(next.unchecked().command("SELECT 1"))
                                        .expect("query driver");
                                    assert!(
                                        result.is_ok(),
                                        "late finalizer closed the new transaction: {:?}",
                                        result.as_ref().err().map(Reply::from)
                                    );
                                    let result =
                                        runtime.block_on(next.commit()).expect("commit driver");
                                    assert!(
                                        result.is_ok(),
                                        "new transaction commit: {:?}",
                                        result.as_ref().err().map(Reply::from)
                                    );
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
                .expect("finalizer installation")
                .expect("finalizer session");
            assert!(reports.events().expect("native reclamation").is_empty());
            server
                .finish()
                .expect("finalizer peer")
                .expect("finalizer protocol");
        },
    )
    .expect("finalizer listener");
}

#[test]
fn transaction_frees_slot() {
    use common::{postgres::startup, transaction};
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

    for failed in [false, true] {
        let bound = socket::Listener::bind(([127, 0, 0, 1], 0).into()).expect("peer listener");
        let addr = bound.local_addr();
        let config = config::Config::with_window(
            config::Capacities {
                connections: 1,
                inflight: 1,
                admission_waiters: 2,
                requests: config::Request {
                    frames: 2,
                    frame_bytes: 512,
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
            },
            Positive::from_millis::<20>(),
        )
        .expect("valid pg configuration");
        let snapshot = service::Snapshot::from_one(
            service::Revision::new(1),
            service::Endpoint::new(addr, addr),
        );
        let (_, node) =
            port::Port::<Queries>::plan(Queries::try_config().expect("database config"), config)
                .assemble::<Env>(
                    snapshot,
                    Duration::from_millis(1),
                    Duration::from_millis(1_024),
                )
                .expect("PostgreSQL plan")
                .into_parts();
        let peer = server::Plan::<Identity, KeyTag<1>>::new(Capacity::fixed::<1>(), Identity);
        let layout =
            listener::topology::Layout::<KeyTag<1>, receive::Owned>::new(Capacity::fixed::<1>());
        let prepared = dope::runtime::topology::Plan::new((), (node, layout))
            .for_install::<Throughput>()
            .expect("PostgreSQL topology");
        let mut reports = Reports::new(prepared.layout(), [Id::fixed::<0>(), Id::fixed::<1>()]);
        let mut report = Completion::new();
        prepared.with_local(
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
                                                assert!(io.write_all(startup::IDLE).await.is_ok());
                                                io
                                            }))
                                            .expect("startup driver");
                                        assert!(
                                            matches!(runtime.block_on(client.status().wait_ready()), Ok(Ok(()))),
                                            "PostgreSQL connection ready"
                                        );
                                        assert!(
                                            matches!(
                                                runtime.block_on(fibers::TEST.poll_once(
                                                    client.unchecked().command("SELECT 1"),
                                                )),
                                                Ok(None)
                                            ),
                                            "request completed before it could be detached"
                                        );
                                        assert!(
                                            matches!(
                                                runtime.block_on(client.transaction().begin()),
                                                Ok(Err(reply::Error::Driver(cartel_pg::Error::RequestTimeout(timeout))))
                                                    if timeout.phase() == cartel_pg::RequestPhase::Admission
                                            ),
                                            "tx admission must time out"
                                        );
                                        fibers::TEST
                                            .try_run(&mut runtime, startup::request(&mut io, false))
                                            .expect("detached request");
                                        assert!(
                                            matches!(
                                                fibers::TEST.try_run(
                                                    &mut runtime,
                                                    io.write_all(transaction::DETACHED_RESPONSES[usize::from(failed)])
                                                ),
                                                Ok(Ok(()))
                                            ),
                                            "server must send detached response"
                                        );
                                        let peer = &mut io;
                                        let exchange = dope_fiber::fiber!('_ => async move {
                                            startup::request(peer, false).await;
                                            assert!(peer.write_all(transaction::DETACHED_RESPONSES[0]).await.is_ok());
                                        });
                                        let (result, ()) = fibers::TEST
                                            .try_run(
                                                &mut runtime,
                                                Join::new(client.unchecked().command("SELECT 1"), exchange),
                                            )
                                            .expect("reused SELECT driver");
                                        assert!(
                                            matches!(result, Ok(())),
                                            "same connection generation remained reusable"
                                        );
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
                assert!(outcome.is_ok(), "{:?}", outcome.as_ref().err().map(|error| error.cause()));
                outcome.ok().expect("application entry")
            },
        )
        .expect("application installation completed")
        .expect("application session completed");
        assert!(reports.events().expect("native reclamation").is_empty());
    }
}
