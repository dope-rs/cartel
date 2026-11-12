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
use dope_test::harness::peer::AcceptScript;
use dope_test::{harness::GUARD, reclaim::Reports};
use std::{
    pin::pin,
    task::Poll,
    time::{Duration, Instant},
};

type Env = Bundle<Tcp, Identity, Throughput>;

pub mod common;

/// Two initial requests expose stale singleton membership; three expose clamped ranks
/// after one completes. Both cases leave loads 4 and 2 on the respective lanes.
#[test]
fn least_inflight_selects() {
    for initial in [2, 3] {
        AcceptScript::with(
            move |listener| -> Result<(), Error> {
                let (mut sockets, selected) = listener.least_loaded(initial)?;
                assert_eq!(selected, 1);
                for socket in &mut sockets {
                    socket.read_closed()?;
                }
                Ok(())
            },
            |server| {
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
                        server.addr(),
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
                                        pin!(Task::new(client.command("SELECT 1")));
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
                                                    client.command("SELECT 1")
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
                                                    client.command("SELECT 1")
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
                                        runtime.block_on(client.command("SELECT 1")),
                                        Ok(Ok(()))
                                    ),
                                    "least-inflight command"
                                );
                            })
                                    })
                                })
                            },
                            recovery::Policy::new(|event| reports.push(event), common::recover),
                            |outcome| {
                                outcome
                                    .map_err(common::entry_error)
                                    .expect("application entry")
                            },
                        );

                    let result = scoped.expect("application installation completed");
                    (result.expect("application session completed"), reports)
                };
                assert!(
                    events.events().expect("native reclamation").is_empty(),
                    "unexpected events PostgreSQL operations: {events:?}"
                );
                server.finish().expect("server").expect("selection peer");
            },
        )
        .expect("selection listener");
    }
}
