use cartel_pg::api::traits::{
    Begin as _, Client as _, Notify as _, Query as _, Status as _, Transaction as _, Unchecked as _,
};
use dope::core::driver::life::recovery;
use std::{
    io::{self, ErrorKind, Write as _},
    net::{SocketAddr, TcpListener},
    num::NonZero,
    pin::pin,
    sync::{
        mpsc,
        mpsc::{RecvTimeoutError, TryRecvError},
    },
    task::Poll,
    thread,
    time::Duration,
};

use cartel_pg::{api, port, port::config, reply};
use common::wire::checked::{Database, TextQuery};
use common::{port::*, topology};
use dope::{
    core::driver::{route::Id, schedule::LIMIT},
    core::platform::wake::{Ends, Wait},
    manifold::{app::env::Bundle, timing::Throughput},
    net::{tcp::Tcp, wire::Identity},
    runtime::executor::session::Completion,
};
use dope_fiber::{
    BlockOn as _,
    abi::{Join, PollFn, Task, poll_fn},
    task::done,
};
use dope_test::{fibers, harness::GUARD, reclaim::Reports};
use o3::base::time::Positive;

use crate::common::postgres::{self, accept::Listener as _, startup::Stream as _};

type Env = Bundle<Tcp, Identity, Throughput>;

pub mod common;

#[test]
fn pending_ready_settles() {
    const COMMANDS: usize = LIMIT + 1;
    const CAPACITY: usize = COMMANDS + 4;
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).expect("listener");
    let address = listener.local_addr().expect("listener address");
    let (ready, release) = Ends::<Wait>::blocking().expect("settlement pipe").split();
    let server = thread::spawn(move || listener.group_settle(ready));
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
    let prepared = topology::Prepare::try_new::<Env>(
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
                            let mut query = Box::pin(Task::new(
                                tx.as_ref()
                                    .expect("transaction")
                                    .query()
                                    .run_group::<TextQuery, COMMANDS>([(); COMMANDS]),
                            ));
                            assert!(
                                runtime
                                    .block_on(PollFn::admitted(|admit| {
                                        Poll::Ready(admit.poll(query.as_mut()).0.is_pending())
                                    }))
                                    .expect("group admission driver"),
                                "group is admitted before responses"
                            );
                            runtime
                                .drive_for(
                                    Duration::from_secs(1),
                                    postgres::Quarantine::new(query, release, move || {
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
                                    .join()
                                    .expect("server thread");
                                let cause = next
                                    .as_ref()
                                    .map(|ready| ready.as_ref().err().map(reply::Error::kind));
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
        .join()
        .expect("server thread")
        .expect("settlement peer");
}

#[test]
fn late_error_keeps_batch() {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).expect("listener");
    let addr = listener.local_addr().expect("listener address");
    let (release, ready) = mpsc::sync_channel(1);
    let server = thread::spawn(move || {
        let stream = listener.reply_settle()?;
        ready
            .recv_timeout(Duration::from_secs(1))
            .map_err(|error| match error {
                RecvTimeoutError::Timeout => io::Error::new(ErrorKind::TimedOut, error),
                RecvTimeoutError::Disconnected => io::Error::new(ErrorKind::BrokenPipe, error),
            })?;
        stream.ready()
    });
    let config = Queries::try_cancel().expect("pipeline config");
    let ((), events) = {
        let prepared = topology::Prepare::try_new::<Env>(
            port::Port::<Queries>::plan(Queries::try_config().expect("database config"), config),
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
                                matches!(
                                    runtime.block_on(client.status().wait_ready()),
                                    Ok(Ok(()))
                                ),
                                "PostgreSQL connection ready"
                            );
                            let (first, second) = runtime
                                .block_on(Join::new(
                                    client.unchecked().command("SELECT 1"),
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

                            let mut command =
                                pin!(Task::new(client.unchecked().command("SELECT 3")));
                            assert!(
                                matches!(
                                    runtime.block_on(fibers::TEST.poll_once(command.as_mut())),
                                    Ok(None)
                                ),
                                "third command admitted"
                            );
                            let marker = runtime
                                .block_on(client.notify().next())
                                .expect("notification runtime")
                                .ok()
                                .expect("CommandComplete marker");
                            assert_eq!(marker.channel(), "settlement");
                            assert!(
                                matches!(
                                    runtime.block_on(fibers::TEST.poll_once(command.as_mut())),
                                    Ok(None)
                                ),
                                "CommandComplete remains pending before ReadyForQuery"
                            );
                            release.send(()).expect("release ReadyForQuery");
                            assert!(
                                matches!(runtime.block_on(command.as_mut()), Ok(Ok(()))),
                                "ReadyForQuery completes the command"
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
        );
        let result = scoped.expect("application installation completed");
        (result.expect("application session completed"), reports)
    };
    assert!(
        events.events().expect("native reclamation").is_empty(),
        "unexpected events PostgreSQL operations: {events:?}"
    );
    server
        .join()
        .expect("server")
        .expect("late Sync error peer");
}

#[test]
fn response_reconnects() {
    for idle in [true, false] {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).expect("listener");
        let addr = listener.local_addr().expect("listener address");
        let (notify, ready) = mpsc::sync_channel(1);
        let server = thread::spawn(
            move || -> Result<Result<(), mpsc::SendError<()>>, postgres::Error> {
                let (first, next) = if idle {
                    let mut first = listener.try_accept(Duration::from_secs(1))?;
                    first.try_start(None)?;
                    first.write_all(&postgres::Frame::new(b'Z', b"I").encode())?;
                    first.read_closed()?;
                    let mut next = listener.try_accept(Duration::from_secs(1))?;
                    next.try_start(None)?;
                    (first, next)
                } else {
                    listener.recovery()?
                };
                if let Err(error) = notify.send(()) {
                    return Ok(Err(error));
                }
                next.recover()?;
                drop(first);
                Ok(Ok(()))
            },
        );
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
        let config = config::Config::with_window(capacity, Positive::from_millis::<50>());
        let config = config.expect("valid pg configuration");
        let ((), events) = {
            let prepared = topology::Prepare::try_new::<Env>(
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
                                let client: api::Client<'_, '_, '_, '_, '_, '_, '_, _, _> = runtime.root_client();

                assert!(
                    matches!(runtime.block_on(client.status().wait_ready()), Ok(Ok(()))),
                    "PostgreSQL connection ready"
                );
                if !idle {
                    assert!(
                        matches!(
                            runtime.block_on(client.unchecked().command("SELECT 1")),
                            Ok(Err(reply::Error::Driver(cartel_pg::Error::RequestTimeout(timeout))))
                                if timeout.phase() == cartel_pg::RequestPhase::Response
                        ),
                        "response must time out"
                    );
                }

                assert!(
                    matches!(
                        runtime.block_on(poll_fn(move |cx| match ready.try_recv() {
                            Ok(()) => Poll::Ready(Ok(())),
                            Err(TryRecvError::Empty) => {
                                cx.as_ref().waker().wake();
                                Poll::Pending
                            }
                            Err(error @ TryRecvError::Disconnected) => Poll::Ready(Err(error)),
                        })),
                        Ok(Ok(()))
                    ),
                    "replacement server must reconnect"
                );
                assert!(
                    matches!(runtime.block_on(client.unchecked().command("SELECT 1")), Ok(Ok(()))),
                    "replacement command"
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
        );

            let result = scoped.expect("application installation completed");
            (result.expect("application session completed"), reports)
        };
        assert!(
            events.events().expect("native reclamation").is_empty(),
            "unexpected events PostgreSQL operations: {events:?}"
        );
        server
            .join()
            .expect("server")
            .expect("recovery peer")
            .expect("replacement readiness receiver");
    }
}
