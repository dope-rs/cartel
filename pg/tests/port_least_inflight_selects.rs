use cartel_pg::api::traits::{Client as _, Status as _, Unchecked as _};
use cartel_pg::{port, port::config, protocol::PickPolicy};
use common::{
    port::*,
    postgres::{Error, accept::Listener as _, startup::Stream as _},
    topology,
};
use dope::core::driver::life::recovery;
use dope::runtime::executor::session::Completion;
use dope::{
    core::driver::route::Id, manifold::app::env::Bundle, manifold::timing::Throughput,
    net::tcp::Tcp, net::wire::Identity,
};
use dope_fiber::{
    BlockOn as _,
    abi::{Task, poll_fn},
};
use dope_test::fibers;
use dope_test::harness::peer;
use dope_test::{harness::GUARD, reclaim::Reports};
use std::{
    iter,
    net::SocketAddr,
    net::TcpListener,
    pin::pin,
    sync::mpsc,
    task::Poll,
    thread,
    time::{Duration, Instant},
};

type Env = Bundle<Tcp, Identity, Throughput>;

pub mod common;

/// Two initial requests expose stale singleton membership; three expose clamped ranks
/// after one completes. Both cases leave loads 4 and 2 on the respective lanes.
#[test]
fn least_inflight_selects() {
    for initial in [2, 3] {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).expect("listener");
        let addr = listener.local_addr().expect("listener address");
        let (report, selected) = mpsc::channel();
        let server = thread::spawn(
            move || -> Result<Result<(), mpsc::SendError<usize>>, Error> {
                let listener =
                    peer::TcpAcceptor::try_adopt(listener.into()).expect("listener admission");
                let (mut sockets, choices) = listener.least_loaded(initial)?;
                for choice in choices {
                    if let Err(error) = report.send(choice) {
                        return Ok(Err(error));
                    }
                }
                for socket in &mut sockets {
                    socket.read_closed()?;
                }
                Ok(Ok(()))
            },
        );
        let capacity = config::Capacities {
            connections: 2,
            inflight: 4,
            admission_waiters: 2,
            requests: config::Request {
                frames: 4,
                frame_bytes: 512,
            },
            responses: config::Response {
                items: 4,
                bytes: 512,
                frame_bytes: 128,
            },
            notifications: config::Notification {
                items: 1,
                bytes: 128,
            },
        };
        let config = config::Config::new(capacity);
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
                                let client = runtime.root_client();
                                client.status().set_policy(PickPolicy::LeastInflight);

                                let deadline = Instant::now() + Duration::from_secs(1);
                                runtime
                                    .block_on(poll_fn(|context| {
                                        if client.status().is_ready() {
                                            return Poll::Ready(());
                                        }
                                        assert!(
                                            Instant::now() < deadline,
                                            "first connection ready"
                                        );
                                        context.wake();
                                        Poll::Pending
                                    }))
                                    .expect("first connection runtime");

                                {
                                    let mut first =
                                        pin!(Task::new(client.unchecked().command("SELECT 1")));
                                    assert!(
                                        matches!(
                                            runtime
                                                .block_on(fibers::TEST.poll_once(first.as_mut())),
                                            Ok(None)
                                        ),
                                        "first singleton request remains in flight"
                                    );
                                    for _ in 1..initial {
                                        assert!(
                                            matches!(
                                                runtime.block_on(fibers::TEST.poll_once(
                                                    client.unchecked().command("SELECT 1")
                                                )),
                                                Ok(None)
                                            ),
                                            "singleton request remains in flight"
                                        );
                                    }

                                    assert!(
                                        matches!(
                                            runtime.block_on(client.status().wait_ready()),
                                            Ok(Ok(()))
                                        ),
                                        "both PostgreSQL connections ready"
                                    );
                                    assert!(
                                        matches!(
                                            runtime.block_on(
                                                fibers::TEST.poll_once(
                                                    client.unchecked().command("SELECT 1")
                                                ),
                                            ),
                                            Ok(None)
                                        ),
                                        "idle connection request remains in flight"
                                    );
                                    if initial == 3 {
                                        assert!(
                                            matches!(runtime.block_on(first.as_mut()), Ok(Ok(()))),
                                            "first connection settles one of three requests"
                                        );
                                    }
                                }
                                assert!(
                                    matches!(
                                        runtime.block_on(client.unchecked().command("SELECT 1")),
                                        Ok(Ok(()))
                                    ),
                                    "least-inflight command"
                                );

                                for expected in iter::repeat_n(0, initial).chain([1, 1]) {
                                    assert_eq!(
                                        selected
                                            .recv_timeout(Duration::from_secs(1))
                                            .expect("selected connection"),
                                        expected
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
        server
            .join()
            .expect("server")
            .expect("selection peer")
            .expect("selected connections");
    }
}
