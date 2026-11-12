use cartel_pg::api::traits::Connection as _;
use cartel_pg::api::traits::{
    Begin as _, Client as _, Notify as _, Query as _, Status as _, Stream as _, Transaction as _,
    Unchecked as _,
};
use dope::core::driver::life::recovery;
use dope_test::harness::peer;
use std::{pin::pin, task::Poll, time::Duration};

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
                                        let mut stream = client.run_stream::<TextQuery>(());
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
            capacity.requests.frames = 3;
            capacity.admission_waiters = 1;
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
                                    pin!(Task::new(client.command("SELECT 1")));
                                let mut waiting =
                                    pin!(Task::new(client.command("SELECT 2")));
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
                                        ::core::assert!(matches!(client.command("SELECT 1").await, Ok(())));
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
fn pinned_queries_wait_for_request_capacity() {
    use common::postgres::{Frame, read_empty};
    use dope::core::dope_sys::io::poll::{Entry, Poll as _};
    use dope_fiber::abi::Join;
    use std::os::fd::AsFd as _;

    peer::AcceptScript::with(
        |listener| {
            let sockets = [(); 2].map(|()| {
                let mut socket = listener.accept().expect("connection");
                socket.try_start(None).expect("startup");
                socket
            });
            let mut entries = sockets
                .each_ref()
                .map(|socket| Entry::readable(socket.as_fd()));
            let events = dope::core::Backend::wait::<_, dope::core::Errno>(&mut entries, 1_000)
                .expect("transaction readiness");
            let lane = events
                .entries()
                .iter()
                .position(|entry| entry.is_readable())
                .expect("transaction request");
            let socket = &sockets[lane];
            read_empty(socket, b'S').expect("begin");
            socket
                .write_all(
                    &[
                        Frame::new(b'C', b"BEGIN\0").encode(),
                        Frame::new(b'Z', b"T").encode(),
                    ]
                    .concat(),
                )
                .expect("begin response");
            for _ in 0..2 {
                read_empty(socket, b'S').expect("pipelined request");
                socket
                    .write_all(
                        &[
                            Frame::new(b'C', b"SELECT 1\0").encode(),
                            Frame::new(b'Z', b"T").encode(),
                        ]
                        .concat(),
                    )
                    .expect("query response");
            }
            read_empty(socket, b'S').expect("commit");
            socket
                .write_all(
                    &[
                        Frame::new(b'C', b"COMMIT\0").encode(),
                        Frame::new(b'Z', b"I").encode(),
                    ]
                    .concat(),
                )
                .expect("commit response");
            for socket in &sockets {
                socket.read_closed().expect("closed");
            }
        },
        |server| {
            let mut capacity = Queries::try_cancel().expect("PG capacity").capacities();
            capacity.connections = 2;
            let prepared = topology::try_new::<_, Env>(
                port::Port::<Queries>::plan(
                    Queries::try_config().expect("database config"),
                    config::Config::new(capacity).expect("one request slot per lane"),
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
                                runtime.block_on(dope_fiber::fiber!('_ => async move {
                            let tx = client.transaction().begin().await.ok().expect("transaction");
                            let (first, second) = Join::new(
                                tx.query().command("SELECT 1"),
                                tx.query().command("SELECT 1"),
                            ).await;
                            assert!(first.is_ok() && second.is_ok());
                            assert!(tx.commit().await.is_ok());
                        })).expect("pipelined transaction driver");
                            })
                        })
                    })
                },
                recovery::Policy::new(|event| reports.push(event), common::recover),
                |result| assert!(matches!(result, Ok(Ok(Ok(())))), "transaction scope"),
            );
            assert!(reports.events().expect("reclamation").is_empty());
            server.finish().expect("transaction peer");
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
                                        runtime.block_on(fibers::TEST.poll_once(client.command(
                                            if unmanaged_begin { "BEGIN" } else { "SELECT 1" }
                                        ))),
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
                                            runtime.block_on(tx.query().command("ROLLBACK")),
                                            Ok(Err(reply::Error::Driver(
                                                cartel_pg::Error::Protocol(
                                                    cartel_pg::protocol::Reason::TransactionLease
                                                )
                                            )))
                                        ));
                                        assert!(matches!(
                                            runtime.block_on(tx.query().command("SELECT 1")),
                                            Ok(Err(reply::Error::Pinned(_)))
                                        ));
                                        Some(tx)
                                    };
                                    assert!(matches!(
                                        runtime.block_on(client.command("SELECT 1")),
                                        Ok(Ok(()))
                                    ));
                                    drop(stale);
                                    assert!(matches!(
                                        runtime.block_on(client.command("SELECT 1")),
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
                                        runtime.block_on(client.command("SELECT 1")),
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
                                    .block_on(client.next())
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
                                    runtime.block_on(client.command("SELECT 1")),
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
                        |outcome| outcome.map_err(common::entry_error).expect("application entry"),
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
fn completed_query_deadline_keeps_next_transaction() {
    peer::AcceptScript::with(common::transaction::throttled, |server| {
        let config = config::Config::with_window(
            Queries::try_cancel().expect("PG capacity").capacities(),
            Positive::from_millis::<100>(),
        )
        .expect("response deadline");
        let prepared = topology::try_new::<_, Env>(
            port::Port::<Queries>::plan(
                Queries::try_config().expect("database config"),
                config,
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
                            let mut old =
                                pin!(Task::new(client.command("SELECT 1")));
                            assert!(
                                runtime
                                    .block_on(fibers::TEST.poll_once(old.as_mut()))
                                    .expect("query admission driver")
                                    .is_none()
                            );
                            let next = runtime
                                .block_on(client.transaction().begin())
                                .expect("begin driver")
                                .ok()
                                .expect("transaction after old ReadyForQuery");
                            fibers::TEST
                                .try_pause(&mut runtime, Duration::from_millis(150))
                                .expect("old response deadline");
                            assert!(matches!(
                                runtime.block_on(old.as_mut()),
                                Ok(Err(reply::Error::Driver(cartel_pg::Error::RequestTimeout(timeout))))
                                    if timeout.phase() == cartel_pg::RequestPhase::Response
                            ));
                            assert!(
                                matches!(runtime.block_on(next.commit()), Ok(Ok(()))),
                                "the completed old query must not close the next transaction"
                            );
                            assert!(matches!(
                                runtime.block_on(client.command("SELECT 1")),
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
    })
    .expect("spawn transaction peer");
}

#[test]
fn retains_next_owner() {
    peer::AcceptScript::with(
        |listener| {
            let mut socket = listener.accept_within(Duration::from_secs(1))?;
            socket.try_start(None)?;
            retire::finalize(&socket)
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
                                        .block_on(next.query().command("SELECT 1"))
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
                    |outcome| outcome.map_err(common::entry_error).expect("application entry"),
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
                connections: 2,
                inflight: 2,
                admission_waiters: 2,
                requests: config::Request {
                    frames: 2,
                    frame_bytes: 512,
                },
                responses: config::Response {
                    items: 2,
                    bytes: 256,
                    frame_bytes: 128,
                },
                notifications: config::Notification {
                    items: 1,
                    bytes: 128,
                },
            },
            Positive::from_millis::<100>(),
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
        let peer = server::Plan::<Identity, KeyTag<1>>::new(Capacity::fixed::<2>(), Identity);
        let layout =
            listener::topology::Layout::<KeyTag<1>, receive::Owned>::new(Capacity::fixed::<2>());
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
                                        fibers::TEST.try_run(&mut runtime, dope_fiber::abi::poll_fn(|context| {
                                            if client.status().is_ready() {
                                                Poll::Ready(())
                                            } else {
                                                context.wake();
                                                Poll::Pending
                                            }
                                        })).expect("first PostgreSQL connection ready");
                                        assert!(
                                            matches!(
                                                runtime.block_on(fibers::TEST.poll_once(
                                                    client.command("SELECT 1"),
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
                                        let mut waiting = pin!(Task::new(client.command("SELECT 2")));
                                        assert!(matches!(
                                            runtime.block_on(fibers::TEST.poll_once(waiting.as_mut())),
                                            Ok(None)
                                        ));
                                        let exchange = dope_fiber::fiber!('_ => async move {
                                            let mut second = peer.accept().await.ok().expect("second connection");
                                            startup::request(&mut second, true).await;
                                            assert!(second.write_all(startup::AUTHENTICATED).await.is_ok());
                                            startup::request(&mut second, false).await;
                                            assert!(second.write_all(startup::IDLE).await.is_ok());
                                            startup::request(&mut second, false).await;
                                            assert!(second.write_all(transaction::DETACHED_RESPONSES[0]).await.is_ok());
                                            second
                                        });
                                        let (result, mut second) = fibers::TEST
                                            .try_run(&mut runtime, Join::new(waiting.as_mut(), exchange))
                                            .expect("new peer admission driver");
                                        assert!(result.is_ok(), "new peer must wake and admit the waiting query");
                                        client.status().set_policy(PickPolicy::LeastInflight);
                                        let mut held = pin!(Task::new(client.command("SELECT 3")));
                                        assert!(matches!(
                                            runtime.block_on(fibers::TEST.poll_once(held.as_mut())),
                                            Ok(None)
                                        ));
                                        fibers::TEST.try_run(&mut runtime, startup::request(&mut second, false))
                                            .expect("retained request");
                                        let fence = b"A\0\0\0\x0c\0\0\0\x01a\0f\0";
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
                                        assert!(matches!(fibers::TEST.try_run(&mut runtime, io.write_all(fence)), Ok(Ok(()))));
                                        assert!(matches!(runtime.block_on(client.next()), Ok(Ok(_))));
                                        assert!(matches!(fibers::TEST.try_run(&mut runtime, second.write_all(transaction::DETACHED_RESPONSES[0])), Ok(Ok(()))));
                                        assert!(matches!(fibers::TEST.try_run(&mut runtime, second.write_all(fence)), Ok(Ok(()))));
                                        assert!(matches!(runtime.block_on(client.next()), Ok(Ok(_))));
                                        let peer = &mut io;
                                        let exchange = dope_fiber::fiber!('_ => async move {
                                            startup::request(peer, false).await;
                                            assert!(peer.write_all(b"C\0\0\0\x0aBEGIN\0Z\0\0\0\x05T").await.is_ok());
                                        });
                                        let (result, ()) = fibers::TEST.try_run(&mut runtime,
                                            Join::new(client.transaction().begin(), exchange))
                                            .expect("least-inflight transaction driver");
                                        let tx = result.ok().expect("skip the retained completed response");
                                        let peer = &mut io;
                                        let exchange = dope_fiber::fiber!('_ => async move {
                                            startup::request(peer, false).await;
                                            assert!(peer.write_all(b"C\0\0\0\x0bCOMMIT\0Z\0\0\0\x05I").await.is_ok());
                                        });
                                        let (result, ()) = fibers::TEST.try_run(&mut runtime, Join::new(tx.commit(), exchange))
                                            .expect("least-inflight commit driver");
                                        assert!(result.is_ok());
                                        let peer = &mut io;
                                        let exchange = dope_fiber::fiber!('_ => async move {
                                            startup::request(peer, false).await;
                                            assert!(peer.write_all(transaction::DETACHED_RESPONSES[0]).await.is_ok());
                                        });
                                        let (result, ()) = fibers::TEST
                                            .try_run(
                                                &mut runtime,
                                                Join::new(client.command("SELECT 1"), exchange),
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
            |outcome| outcome.map_err(common::entry_error).expect("application entry"),
        )
        .expect("application installation completed")
        .expect("application session completed");
        assert!(reports.events().expect("native reclamation").is_empty());
    }
}
