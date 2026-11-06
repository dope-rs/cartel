use cartel_pg::api::traits::{
    Begin as _, Client as _, Notify as _, Query as _, Status as _, Transaction as _, Unchecked as _,
};
use dope::core::driver::life::recovery;
use std::{
    net::{SocketAddr, TcpListener},
    num::NonZero,
    pin::pin,
    task::Poll,
    time::Duration,
};

use cartel_pg::{api, port, port::config, reply};
use common::wire::checked::{Database, TextQuery};
use common::{port::*, topology};
use dope::{
    core::driver::{route::Id, schedule::LIMIT},
    manifold::{app::env::Bundle, client::attempt::ConnectFault, timing::Throughput},
    net::{link::event, tcp::Tcp, wire::Identity},
    runtime::executor::session::Completion,
};
use dope_fiber::{
    BlockOn as _,
    abi::{Join, PollFn, Task, poll_fn},
    task::done,
};
use dope_test::{
    fibers,
    harness::{
        GUARD,
        peer::AcceptScript,
        scenario::run::{Entry, Task as _},
    },
    reclaim::Reports,
};
use o3::base::time::Positive;

use crate::common::postgres::{self, accept::Listener as _};

type Env = Bundle<Tcp, Identity, Throughput>;

pub mod common;

#[test]
fn failed_reconnect_preserves_healthy_connection() {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).expect("listener");
    let addr = listener.local_addr().expect("listener address");
    let ((), peer) = Entry(move || postgres::retire::partial_pool(listener))
        .with(|| {
            let mut capacity = Queries::try_reply()
                .expect("PostgreSQL capacity")
                .capacities();
            capacity.connections = 2;
            let config = config::Config::new(capacity).expect("two connection capacity");
            let prepared = topology::try_new::<_, Env>(
                port::Port::<Queries>::plan(
                    Queries::try_config().expect("database config"),
                    config,
                ),
                addr,
                Duration::from_millis(1),
                Duration::from_secs(1),
            )
            .expect("PostgreSQL topology");
            let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
            let mut report = Completion::new();
            prepared.with_local(
                |worker| {
                    worker.lend(|node, stage| {
                        node.install(stage, |host, stage| {
                            stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                        let client = runtime.root_client();
                        fibers::TEST
                            .try_run(
                                &mut runtime,
                                poll_fn(|context| {
                                    if client.status().live_count() == 1
                                        && client.status().is_failed()
                                    {
                                        Poll::Ready(())
                                    } else {
                                        context.wake();
                                        Poll::Pending
                                    }
                                }),
                            )
                            .expect("one healthy connection and one refused reconnect");
                        assert!(matches!(
                            runtime.block_on(client.status().wait_ready()),
                            Ok(Err(reply::Error::Open(reply::Open::Transport {
                                cause: ConnectFault::Transport(event::ConnectError::Connect(errno)),
                                ..
                            }))) if errno.is_refused()
                        ));
                        assert!(matches!(
                            runtime.block_on(client.unchecked().command("SELECT 1")),
                            Ok(Ok(()))
                        ));
                        fibers::TEST
                            .try_run(
                                &mut runtime,
                                poll_fn(|context| {
                                    if client.status().live_count() == 0 {
                                        Poll::Ready(())
                                    } else {
                                        context.wake();
                                        Poll::Pending
                                    }
                                }),
                            )
                            .expect("all connections closed");
                        assert!(matches!(
                            runtime.block_on(client.unchecked().command("SELECT 1")),
                            Ok(Err(reply::Error::Open(reply::Open::Transport {
                                cause: ConnectFault::Transport(event::ConnectError::Connect(errno)),
                                ..
                            }))) if errno.is_refused()
                        ));
                    })
                        })
                    })
                },
                recovery::Policy::new(|event| reports.push(event), common::recover),
                |result| assert!(matches!(result, Ok(Ok(Ok(())))), "PostgreSQL scope"),
            );
            assert!(
                matches!(reports.events(), Ok(events) if events.is_empty()),
                "unexpected reclamation"
            );
        })
        .expect("PostgreSQL peer start and join");
    peer.expect("partial pool");
}

#[test]
fn stalled_partial_reconnects_idle_connection() {
    AcceptScript::with(postgres::retire::stalled_partial, |server| {
        let config = config::Config::with_window(
            Queries::try_reply()
                .expect("PostgreSQL capacity")
                .capacities(),
            Positive::from_secs::<2>(),
        )
        .expect("PostgreSQL admission deadline");
        let prepared = topology::try_new::<
            _,
            Bundle<Tcp, Identity, Throughput, postgres::retire::ShortIdle>,
        >(
            port::Port::<Queries>::plan(Queries::try_config().expect("database config"), config),
            server.addr(),
            Duration::from_millis(1),
            Duration::from_secs(1),
        )
        .expect("PostgreSQL topology");
        let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
        let mut report = Completion::new();
        prepared.with_local(
            |worker| {
                worker.lend(|node, stage| {
                    node.install(stage, |host, stage| {
                        stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                            let client = runtime.root_client();
                            for _ in 0..2 {
                                assert!(matches!(
                                    runtime.block_on(client.unchecked().command("SELECT 1")),
                                    Ok(Ok(()))
                                ));
                            }
                        })
                    })
                })
            },
            recovery::Policy::new(|event| reports.push(event), common::recover),
            |result| assert!(matches!(result, Ok(Ok(Ok(())))), "PostgreSQL scope"),
        );
        assert!(
            matches!(reports.events(), Ok(events) if events.is_empty()),
            "unexpected reclamation"
        );
        assert!(
            matches!(server.finish(), Ok(Ok(()))),
            "PostgreSQL peer completion"
        );
    })
    .expect("spawn PostgreSQL peer");
}

#[test]
fn unsolicited_partial_cannot_bind_next_request() {
    for throttled in [false, true] {
        let mut capacity = Queries::try_reply()
            .expect("PostgreSQL capacity")
            .capacities();
        capacity.inflight = 1;
        let config = config::Config::with_window(capacity, Positive::from_millis::<100>())
            .expect("PostgreSQL admission deadline");
        AcceptScript::with(
            |peer| postgres::retire::unsolicited_partial(peer.accept()?),
            |server| {
                let prepared = topology::try_new::<_, Env>(
                    port::Port::<Queries>::plan(Queries::try_config().expect("database config"), config),
                    server.addr(),
                    Duration::from_millis(1),
                    Duration::from_secs(1),
                )
                .expect("PostgreSQL topology");
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
                                    let mut first = pin!(Task::new(client.unchecked().command("SELECT 1")));
                                    let mut second = pin!(Task::new(client.unchecked().command("SELECT 1")));
                                    runtime.block_on(PollFn::admitted(|admit| {
                                        let (first, context) = admit.poll(first.as_mut());
                                        assert!(first.is_pending());
                                        if throttled {
                                            assert!(matches!(
                                                context.try_poll(second.as_mut()),
                                                dope_fiber::context::PollResult::Polled(Poll::Pending)
                                            ));
                                        }
                                        Poll::Ready(())
                                    })).expect("both commands polled before responses");
                                    assert!(matches!(runtime.block_on(first.as_mut()), Ok(Ok(()))));
                                    fibers::TEST
                                        .try_pause(&mut runtime, Duration::from_millis(20))
                                        .expect("process unsolicited partial response");
                                    assert!(
                                        matches!(
                                            runtime.block_on(second.as_mut()),
                                            Ok(Err(reply::Error::Driver(cartel_pg::Error::RequestTimeout(timeout))))
                                                if timeout.phase() == cartel_pg::RequestPhase::Admission
                                        ),
                                        "an unsolicited partial response must not admit the next request; throttled={throttled}"
                                    );
                                })
                            })
                        })
                    },
                    recovery::Policy::new(|event| reports.push(event), common::recover),
                    |result| assert!(matches!(result, Ok(Ok(Ok(())))), "PostgreSQL scope"),
                );
                assert!(
                    matches!(reports.events(), Ok(events) if events.is_empty()),
                    "unexpected reclamation"
                );
                assert!(
                    matches!(server.finish(), Ok(Ok(()))),
                    "PostgreSQL peer completion"
                );
            },
        )
        .expect("spawn PostgreSQL peer");
    }
}

#[test]
fn asynchronous_partial_resumes_pinned_request() {
    use dope::{
        core::{
            driver::route::{KeyTag, table::conn::Capacity},
            io::socket,
        },
        manifold::{app::receive, network::listener, service},
        net::wire::domain,
        runtime::topology::lending::Stage,
    };
    use dope_fiber::net::server;
    use postgres::{retire, startup};

    let bound = socket::Listener::bind(([127, 0, 0, 1], 0).into()).expect("peer listener");
    let addr = bound.local_addr();
    let snapshot = service::Snapshot::from_one(
        service::Revision::new(1),
        service::Endpoint::new(addr, addr),
    );
    let (_, node) = port::Port::<Queries>::plan(
        Queries::try_config().expect("database config"),
        Queries::try_reply().expect("PostgreSQL capacity"),
    )
    .assemble::<Env>(snapshot, Duration::from_millis(1), Duration::from_secs(1))
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
                                    let peer = &mut io;
                                    let exchange = dope_fiber::fiber!('_ => async move {
                                        startup::request(peer, false).await;
                                        assert!(peer.write_all(retire::ASYNCHRONOUS_PARTIAL[0].0).await.is_ok());
                                    });
                                    let (tx, ()) = fibers::TEST
                                        .try_run(&mut runtime, Join::new(client.transaction().begin(), exchange))
                                        .expect("begin driver");
                                    let tx = tx.ok().expect("transaction");
                                    for (index, (_, tail)) in
                                        retire::ASYNCHRONOUS_PARTIAL.into_iter().enumerate()
                                    {
                                        fibers::TEST
                                            .try_pause(&mut runtime, Duration::from_millis(20))
                                            .expect("process asynchronous prefix");
                                        let mut command =
                                            pin!(Task::new(tx.unchecked().command("SELECT 1")));
                                        assert!(matches!(
                                            runtime
                                                .block_on(fibers::TEST.poll_once(command.as_mut())),
                                            Ok(None)
                                        ));
                                        assert!(matches!(
                                            fibers::TEST.try_run(&mut runtime, io.write_all(tail)),
                                            Ok(Ok(()))
                                        ));
                                        let response = retire::ASYNCHRONOUS_PARTIAL
                                            .get(index + 1)
                                            .map_or(retire::TRANSACTION_SELECT, |(prefix, _)| {
                                                *prefix
                                            });
                                        let peer = &mut io;
                                        let exchange = dope_fiber::fiber!('_ => async move {
                                            startup::request(peer, false).await;
                                            assert!(peer.write_all(response).await.is_ok());
                                        });
                                        let (result, ()) = fibers::TEST
                                            .try_run(&mut runtime, Join::new(command.as_mut(), exchange))
                                            .expect("pinned SELECT driver");
                                        assert!(matches!(
                                            result,
                                            Ok(())
                                        ));
                                    }
                                    let notice = runtime
                                        .block_on(client.notify().next())
                                        .expect("notification driver")
                                        .ok()
                                        .expect("asynchronous notification");
                                    assert_eq!(
                                        (notice.pid(), notice.channel(), notice.payload()),
                                        (7, "partial", "ready")
                                    );
                                    let peer = &mut io;
                                    let exchange = dope_fiber::fiber!('_ => async move {
                                        startup::request(peer, false).await;
                                        assert!(peer.write_all(retire::COMMITTED).await.is_ok());
                                    });
                                    let (result, ()) = fibers::TEST
                                        .try_run(&mut runtime, Join::new(tx.commit(), exchange))
                                        .expect("commit driver");
                                    assert!(matches!(result, Ok(())));
                                },
                            )
                        },
                    )
                    .expect("install peer")
                })
            })
        },
        recovery::Policy::new(|event| reports.push(event), common::recover),
        |result| assert!(matches!(result, Ok(Ok(Ok(())))), "PostgreSQL scope"),
    );
    assert!(
        matches!(reports.events(), Ok(events) if events.is_empty()),
        "unexpected reclamation"
    );
}

#[test]
fn detached_partial_preserves_pipelined_request() {
    AcceptScript::with(
        |peer| postgres::retire::detached_partial(peer.accept()?),
        |server| {
            let prepared = topology::try_new::<_, Env>(
                port::Port::<Queries>::plan(
                    Queries::try_config().expect("database config"),
                    Queries::try_reply().expect("PostgreSQL capacity"),
                ),
                server.addr(),
                Duration::from_millis(1),
                Duration::from_secs(1),
            )
            .expect("PostgreSQL topology");
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
                                fibers::TEST
                                    .try_pause(&mut runtime, Duration::from_millis(20))
                                    .expect("process detached partial response");
                                assert!(matches!(
                                    runtime.block_on(client.unchecked().command("SELECT 2")),
                                    Ok(Ok(()))
                                ));
                            })
                        })
                    })
                },
                recovery::Policy::new(|event| reports.push(event), common::recover),
                |result| assert!(matches!(result, Ok(Ok(Ok(())))), "PostgreSQL scope"),
            );
            assert!(
                matches!(reports.events(), Ok(events) if events.is_empty()),
                "unexpected reclamation"
            );
            assert!(
                matches!(server.finish(), Ok(Ok(()))),
                "PostgreSQL peer completion"
            );
        },
    )
    .expect("spawn PostgreSQL peer");
}

#[test]
fn pending_ready_settles() {
    const COMMANDS: usize = LIMIT + 1;
    const CAPACITY: usize = COMMANDS + 4;
    AcceptScript::with(
        |listener| listener.group_settle(),
        |server| {
            let address = server.addr();
            let mut server = Some(server);
            let config = config::Config::new(config::Capacities {
                connections: 1,
                inflight: CAPACITY,
                admission_waiters: 2,
                requests: config::Request {
                    frames: CAPACITY,
                    frame_bytes: 16_384,
                },
                responses: config::Response {
                    items: CAPACITY,
                    bytes: 4096,
                    frame_bytes: 128,
                },
                notifications: config::Notification {
                    items: 1,
                    bytes: 64,
                },
            })
            .and_then(|config| {
                config.with_groups(config::Groups {
                    active: NonZero::<usize>::MIN,
                    records: NonZero::new(2).expect("diagnostic records"),
                    bytes: NonZero::new(128).expect("diagnostic bytes"),
                })
            })
            .expect("group configuration");
            let prepared = topology::try_new::<_, Env>(
                port::Port::<Database>::plan(Queries::try_config().expect("credentials"), config),
                address,
                Duration::from_millis(1),
                Duration::from_secs(1),
            )
            .expect("prepared topology");
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
                                    let mut tx = Some(
                                        runtime
                                            .block_on(client.transaction().begin())
                                            .expect("begin driver")
                                            .ok()
                                            .expect("transaction"),
                                    );
                                    let tx_ptr: *mut Option<_> = &mut tx;
                                    let query = Box::pin(Task::new(
                                        tx.as_ref()
                                            .expect("transaction")
                                            .query()
                                            .run_group::<TextQuery, COMMANDS>([(); COMMANDS]),
                                    ));
                                    runtime
                                        .drive_for(
                                            Duration::from_secs(1),
                                            postgres::Quarantine::new(query, move || {
                                                // SAFETY: the root destroys the only future borrowing this
                                                // guard before this callback. The outer Option stays live
                                                // and is not accessed while the root owns that future.
                                                drop(unsafe { (*tx_ptr).take() });
                                            }),
                                        )
                                        .expect("quarantine after yielded ReadyForQuery drain")
                                        .expect("one quarantine continuation");
                                    assert!(tx.is_none());
                                    let next = runtime.drive_for(
                                        Duration::from_secs(1),
                                        done::Once::new(client.status().wait_ready()),
                                    );
                                    if !matches!(&next, Ok(Ok(()))) {
                                        let peer = server
                                            .take()
                                            .expect("settlement peer")
                                            .finish()
                                            .expect("settlement peer lifecycle");
                                        let cause = next.as_ref().map(|ready| {
                                            ready.as_ref().err().map(reply::Error::kind)
                                        });
                                        assert!(
                                            matches!(&next, Ok(Ok(()))),
                                            "replacement: {cause:?}; settlement peer: {peer:?}"
                                        );
                                    }
                                    next.expect("replacement driver")
                                        .ok()
                                        .expect("replacement connection");
                                    runtime
                                        .block_on(client.unchecked().command("SELECT 1"))
                                        .expect("replacement command driver")
                                        .ok()
                                        .expect("replacement command");
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
                .expect("installation")
                .expect("session");
            assert!(reports.events().expect("native reclamation").is_empty());
            server
                .take()
                .expect("settlement peer")
                .finish()
                .expect("settlement peer lifecycle")
                .expect("settlement peer");
        },
    )
    .expect("settlement peer spawn");
}

#[test]
fn late_error_keeps_batch() {
    AcceptScript::with(
        |listener| listener.reply_settle(),
        |server| {
            let addr = server.addr();
            let config = Queries::try_cancel().expect("pipeline config");
            let ((), events) =
                {
                    let prepared = topology::try_new::<_, Env>(
                        port::Port::<Queries>::plan(
                            Queries::try_config().expect("database config"),
                            config,
                        ),
                        addr,
                        Duration::from_millis(1),
                        Duration::from_millis(1_024),
                    )
                    .expect("topology fixture completed");
                    let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
                    let mut report = Completion::new();
                    let scoped =
                        prepared.with_local(
                            |worker| {
                                worker.lend(|node, stage| {
                                    node.install(stage, |host, stage| {
                                        stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                            let client: api::Client<'_, '_, '_, '_, '_, '_, '_, _, _> =
                                runtime.root_client();
                            assert!(
                                matches!(
                                    runtime.block_on(client.status().wait_ready()),
                                    Ok(Ok(()))
                                ),
                                "PostgreSQL connection ready"
                            );
                            let mut first = pin!(Task::new(client.unchecked().command("SELECT 1")));
                            assert!(
                                matches!(
                                    runtime.block_on(fibers::TEST.poll_once(first.as_mut())),
                                    Ok(None)
                                ),
                                "first command admitted"
                            );
                            let marker = runtime
                                .block_on(client.notify().next())
                                .expect("notification runtime")
                                .ok()
                                .expect("CommandComplete marker");
                            assert_eq!(marker.channel(), "settlement");
                            assert!(
                                matches!(
                                    runtime.block_on(fibers::TEST.poll_once(first.as_mut())),
                                    Ok(None)
                                ),
                                "CommandComplete remains pending before ReadyForQuery"
                            );
                            let (first, second) = runtime
                                .block_on(Join::new(
                                    first.as_mut(),
                                    client.unchecked().command("SELECT 2"),
                                ))
                                .expect("pipeline runtime");
                            assert!(
                                matches!(
                                    first,
                                    Err(reply::Error::Server(error))
                                        if error.fields().code().as_str() == "23505"
                                            && error.fields().message() == "late sync"
                                ),
                                "the first command retains its late Sync error"
                            );
                            assert!(matches!(second, Ok(())), "second batch completes");
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
            server
                .finish()
                .expect("server")
                .expect("late Sync error peer");
        },
    )
    .expect("late Sync peer spawn");
}

#[test]
fn response_reconnects() {
    for (idle, held) in [(true, false), (false, false), (false, true)] {
        AcceptScript::with(
            move |listener| listener.recovery(idle, held),
            |server| {
                let addr = server.addr();
                let capacity = config::Capacities {
                    connections: 1,
                    inflight: 1,
                    admission_waiters: 2,
                    requests: config::Request {
                        frames: 1,
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
                };
                let config = if held {
                    Queries::try_reply()
                } else {
                    config::Config::with_window(capacity, Positive::from_millis::<50>())
                };
                let config = config.expect("valid pg configuration");
                let ((), events) = {
                    let prepared = topology::try_new::<_, Env>(
                        port::Port::<Queries>::plan(
                            Queries::try_config().expect("database config"),
                            config,
                        ),
                        addr,
                        Duration::from_millis(1),
                        Duration::from_millis(1_024),
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
                                        assert!(
                                            matches!(runtime.block_on(client.status().wait_ready()), Ok(Ok(()))),
                                            "PostgreSQL connection ready"
                                        );
                                        let mut first = pin!(held.then(|| {
                                            Task::new(client.unchecked().command("SELECT 1"))
                                        }));
                                        if let Some(first) = first.as_mut().as_pin_mut() {
                                            assert!(
                                                matches!(runtime.block_on(fibers::TEST.poll_once(first)), Ok(None)),
                                                "first command admitted"
                                            );
                                        }
                                        if idle || held {
                                            let notice = fibers::TEST
                                                .try_run(&mut runtime, client.notify().next())
                                                .expect("replacement notification driver")
                                                .ok()
                                                .expect("replacement notification");
                                            assert_eq!(
                                                (notice.pid(), notice.channel(), notice.payload()),
                                                (2, "replacement", "ready")
                                            );
                                        } else {
                                            assert!(
                                                matches!(
                                                    runtime.block_on(client.unchecked().command("SELECT 1")),
                                                    Ok(Err(reply::Error::Driver(cartel_pg::Error::RequestTimeout(timeout))))
                                                        if timeout.phase() == cartel_pg::RequestPhase::Response
                                                ),
                                                "response must time out"
                                            );
                                            assert!(
                                                matches!(runtime.block_on(client.status().wait_ready()), Ok(Ok(()))),
                                                "replacement server must reconnect"
                                            );
                                        }
                                        assert!(
                                            matches!(runtime.block_on(client.unchecked().command("SELECT 1")), Ok(Ok(()))),
                                            "replacement command"
                                        );
                                        if let Some(first) = first.as_mut().as_pin_mut() {
                                            assert!(
                                                matches!(
                                                    fibers::TEST.try_run(&mut runtime, first),
                                                    Ok(Err(reply::Error::Server(error)))
                                                        if error.fields().code().as_str() == "23505"
                                                            && error.fields().message() == "held response"
                                                ),
                                                "previous generation retains its unconsumed server error"
                                            );
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
                    );
                    let result = scoped.expect("application installation completed");
                    (result.expect("application session completed"), reports)
                };
                assert!(
                    events.events().expect("native reclamation").is_empty(),
                    "unexpected events PostgreSQL operations: {events:?}"
                );
                server.finish().expect("server").expect("recovery peer");
            },
        )
        .expect("recovery peer spawn");
    }
}
