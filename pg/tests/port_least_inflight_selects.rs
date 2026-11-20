use cartel_pg::api::traits::{Client as _, Query as _, Status as _};
use cartel_pg::{
    port,
    port::{config, ready::LeastInflight},
};
use common::{
    port::*,
    postgres::{Error, accept::Listener as _, startup::Stream as _},
    topology,
};
use dope::manifold::timing::Throughput;
use dope_fiber::{
    BlockOn as _,
    abi::{Task, poll_fn},
};
use dope_test::fibers;
use dope_test::harness::peer::AcceptScript;
use std::{
    pin::pin,
    task::Poll,
    time::{Duration, Instant},
};

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
                let config = config::Configuration::new(capacity);
                let config = config.expect("valid pg configuration");
                {
                    let plan = port::Port::<(Queries, LeastInflight)>::plan(
                        Queries::try_config().expect("database config"),
                        config,
                    );
                    topology::run(plan, server.addr(), (1, 1_024), Throughput, |mut runtime| {
                        let client = runtime.root_client();

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
                    });
                }
                server.finish().expect("server").expect("selection peer");
            },
        )
        .expect("selection listener");
    }
}
