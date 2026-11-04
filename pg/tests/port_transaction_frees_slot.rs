use cartel_pg::api::traits::{
    Begin as _, Client as _, Notify as _, Status as _, Transaction as _, Unchecked as _,
};
use dope::core::driver::life::recovery;
use dope_test::harness::peer;
use std::{
    net::{SocketAddr, TcpListener},
    pin::pin,
    sync::{mpsc, mpsc::TryRecvError},
    task::Poll,
    thread,
    time::Duration,
};

use cartel_pg::{api::tx::FinishError, port, port::config, reply};
use common::{
    port::*,
    postgres::{accept::Listener, retire, startup::Stream},
    topology,
};
use dope::{
    core::{driver::route::Id, platform::wake::Ends},
    manifold::{app::env::Bundle, timing::Throughput},
    net::{tcp::Tcp, wire::Identity},
    runtime::executor::session::Completion,
};
use dope_fiber::{BlockOn as _, abi::Task, abi::poll_fn};
use dope_test::{fibers, harness::GUARD, reclaim::Reports};
use o3::base::time::Positive;

type Env = Bundle<Tcp, Identity, Throughput>;

pub mod common;

#[test]
fn throttled_query_cannot_enter_later_transaction() {
    let (wait, release) = Ends::blocking().expect("first reply gate").split();
    peer::AcceptScript::with(
        move |listener| common::transaction::throttled(listener, wait),
        |server| {
            let mut capacity = Queries::try_cancel().expect("PG capacity").capacities();
            capacity.inflight = 1;
            let prepared = topology::Prepare::try_new::<Env>(
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
                                assert!(matches!(
                                    runtime.block_on(fibers::TEST.poll_once(first.as_mut())),
                                    Ok(None)
                                ));
                                let mut waiting =
                                    pin!(Task::new(client.unchecked().command("SELECT 2")));
                                assert!(matches!(
                                    runtime.block_on(fibers::TEST.poll_once(waiting.as_mut())),
                                    Ok(None)
                                ));
                                release.notify().expect("complete first query");
                                assert!(matches!(runtime.block_on(first.as_mut()), Ok(Ok(()))));
                                let tx = runtime
                                    .block_on(client.transaction().begin())
                                    .expect("begin driver")
                                    .ok()
                                    .expect("exclusive transaction");
                                assert!(matches!(
                                    runtime.block_on(waiting.as_mut()),
                                    Ok(Err(reply::Error::Driver(
                                        cartel_pg::Error::Backpressure { .. }
                                    )))
                                ));
                                assert!(matches!(runtime.block_on(tx.commit()), Ok(Ok(()))));
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

#[test]
fn transaction_status_rejects_stale_guard() {
    for invalid_begin in [false, true] {
        peer::AcceptScript::with(
            move |listener| common::transaction::ended(listener, invalid_begin),
            |server| {
                let prepared = topology::Prepare::try_new::<Env>(
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
                                        runtime.block_on(
                                            fibers::TEST
                                                .poll_once(client.unchecked().command("SELECT 1"))
                                        ),
                                        Ok(None)
                                    ));
                                    let started = runtime
                                        .block_on(client.transaction().begin())
                                        .expect("begin driver");
                                    let stale = if invalid_begin {
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
                let prepared = topology::Prepare::try_new::<Env>(
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
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).expect("listener");
        let address = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || {
            peer::TcpAcceptor::try_adopt(listener.into())
                .expect("listener admission")
                .finalizer()
        });
        let mut capacity = Queries::try_cancel()
            .expect("bounded PostgreSQL config")
            .capacities();
        capacity.inflight = 1;
        let config = config::Config::with_window(capacity, Positive::from_millis::<100>())
            .expect("finalizer deadline");
        let prepared = topology::Prepare::try_new::<Env>(
            port::Port::<Queries>::plan(Queries::try_config().expect("database config"), config),
            address,
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
            .join()
            .expect("finalizer peer")
            .expect("finalizer protocol");
    }
}

#[test]
fn retains_next_owner() {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).expect("listener");
    let address = listener.local_addr().expect("listener address");
    let server = thread::spawn(move || {
        let listener = peer::TcpAcceptor::try_adopt(listener.into()).expect("listener admission");
        let mut socket = listener.accept_within(Duration::from_secs(1))?;
        socket.try_start(None)?;
        retire::finalize(&mut socket)
    });
    let prepared = topology::Prepare::try_new::<Env>(
        port::Port::<Queries>::plan(
            Queries::try_config().expect("database config"),
            Queries::try_cancel().expect("two in-flight replies on one connection"),
        ),
        address,
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
                            let result = runtime.block_on(next.commit()).expect("commit driver");
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
        .join()
        .expect("finalizer peer")
        .expect("finalizer protocol");
}

#[test]
fn transaction_frees_slot() {
    for failed in [false, true] {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).expect("listener");
        let addr = listener.local_addr().expect("listener address");
        let (release, permit) = mpsc::sync_channel(1);
        let (done, drained) = mpsc::sync_channel(1);
        let server = thread::spawn(move || {
            let listener =
                peer::TcpAcceptor::try_adopt(listener.into()).expect("listener admission");
            let mut socket = Listener::accept_within(&listener, Duration::from_secs(1))
                .expect("peer connection");
            Stream::try_start(&mut socket, None).expect("peer handshake");
            permit.recv().expect("detached response release");
            retire::reply(&mut socket, failed).expect("detached response");
            done.send(()).expect("drained receiver");
            retire::reply(&mut socket, false).expect("reused response");
        });
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
        let prepared = topology::Prepare::try_new::<Env>(
            port::Port::<Queries>::plan(Queries::try_config().expect("database config"), config),
            addr,
            Duration::from_millis(1),
            Duration::from_millis(1_024),
        )
        .expect("topology fixture completed");
        let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
        let mut report = Completion::new();
        prepared.with_local(
            |worker| {
                worker.lend(|node, stage| {
                    node.install(stage, |host, stage| {
                        stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                            let client = runtime.root_client();
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
                            release.send(()).expect("release detached response");
                            assert!(
                                matches!(
                                    runtime.block_on(poll_fn(move |cx| match drained.try_recv() {
                                        Ok(()) => Poll::Ready(Ok(())),
                                        Err(TryRecvError::Empty) => {
                                            cx.as_ref().waker().wake();
                                            Poll::Pending
                                        }
                                        Err(error @ TryRecvError::Disconnected) => Poll::Ready(Err(error)),
                                    })),
                                    Ok(Ok(()))
                                ),
                                "server must send detached response"
                            );
                            assert!(
                                matches!(runtime.block_on(client.unchecked().command("SELECT 1")), Ok(Ok(()))),
                                "same connection generation remained reusable"
                            );
                        })
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
        server.join().expect("server");
    }
}
