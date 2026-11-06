use cartel_redis::api::{Client as _, Connection as _};
use dope::core::driver::life::recovery;
use dope::runtime::executor::session::Completion;
use std::{net::SocketAddr, pin::pin, time::Duration};

use cartel_redis::client::{
    settings,
    settings::{Builder, init::Plan},
};
use common::{
    recover,
    session::{
        config,
        deadline::*,
        held::{PING, first_request},
    },
    topology,
};
use dope::{
    core::driver::route::Id,
    manifold::{app::env::Bundle, timing::Throughput},
    net::{tcp::Tcp, wire::Identity},
};
use dope_fiber::{
    BlockOn as _,
    abi::{Join, Task, poll_fn},
    context::raw::Live,
};
use dope_test::{
    fibers,
    harness::{
        GUARD,
        peer::{self, AcceptScript},
    },
    reclaim::Reports,
};
use o3::base::time::Positive;

type Env = Bundle<Tcp, Identity, Throughput>;

pub mod common;

#[test]
fn waits_expire() {
    let config = config(Positive::from_millis::<5>()).expect("valid redis configuration");
    {
        let prepared =
            topology::try_new::<Env>(config, SocketAddr::from(([127, 0, 0, 1], 0)), Plan::new())
                .expect("topology fixture completed");
        let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
        let mut report = Completion::new();
        prepared.with_local(
            |worker| {
                worker.lend(|node, stage| {
                    node.install(stage, |host, stage| {
                        let timer = stage.driver().driver_ref().timer();
                        stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                            let redis = runtime.root_client();

                            assert!(matches!(
                                runtime.block_on(redis.server().ping()),
                                Ok(Err(cartel_redis::value::reply::Failure::Control(
                                    cartel_redis::Error::RequestTimeout(timeout)
                                )))
                                    if timeout.phase() == cartel_redis::RequestPhase::Admission
                            ));
                            let mut acquire = pin!(redis.connection());
                            let mut alarms = Vec::new();
                            let result = runtime.block_on(poll_fn(|context| {
                                while let Ok(alarm) = timer.try_alarm() {
                                    alarms.push(alarm);
                                }
                                // SAFETY: acquire stays pinned; Ready ends this closure's
                                // polling authority while retaining the completed future.
                                let poll = unsafe { Live::new(acquire.as_mut()) }.poll(context);
                                if poll.is_ready() {
                                    assert!(timer.try_alarm().is_ok(), "completed lease timer");
                                }
                                poll
                            }));
                            assert!(matches!(
                                result,
                                Ok(Err(cartel_redis::Error::RequestTimeout(timeout)))
                                    if timeout.phase() == cartel_redis::RequestPhase::Lease
                            ));
                        })
                    })
                })
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            |result| assert!(matches!(result, Ok(Ok(Ok(())))), "Redis scope"),
        );
        reports.events().expect("valid reclaim observations");
    };
}

#[test]
fn detached_blocks_lease() {
    let config = config(Positive::from_millis::<10>()).expect("valid redis configuration");
    AcceptScript::with(
        |peer| {
            let socket = accept_within(peer)?;
            socket.read_wait(Some(Duration::from_secs(1)))?;
            assert_ne!(socket.read(&mut [0; 128])?, 0);
            wait_closed(&socket)
        },
        |server| {
            let prepared = topology::try_new::<Env>(config, server.addr(), Plan::new())
                .expect("topology fixture completed");
            let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
            let mut report = Completion::new();
            prepared.with_local(
                |worker| {
                    worker.lend(|node, stage| {
                        node.install(stage, |host, stage| {
                            stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                                let redis = runtime.root_client();

                                assert!(matches!(
                                    runtime.block_on(redis.wait_active()),
                                    Ok(Ok(()))
                                ));
                                assert!(
                                    matches!(
                                        runtime.block_on(
                                            fibers::TEST.poll_once(redis.server().ping())
                                        ),
                                        Ok(None)
                                    ),
                                    "command completed before it could be detached"
                                );
                                assert!(matches!(
                                    runtime.block_on(redis.connection()),
                                    Ok(Err(cartel_redis::Error::RequestTimeout(timeout)))
                                        if timeout.phase() == cartel_redis::RequestPhase::Lease
                                ));
                            })
                        })
                    })
                },
                recovery::Policy::new(|event| reports.push(event), recover),
                |result| assert!(matches!(result, Ok(Ok(Ok(())))), "Redis scope"),
            );
            reports.events().expect("valid reclaim observations");
            server
                .finish()
                .expect("Redis lifecycle")
                .expect("Redis server");
        },
    )
    .expect("start Redis server");
}

#[test]
fn pool_timeout_reconnects() {
    let config = config(Positive::from_millis::<10>()).expect("valid redis configuration");
    AcceptScript::with(recovery_server, |server| {
        let prepared = topology::try_new::<Env>(config, server.addr(), Plan::new())
            .expect("topology fixture completed");
        let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
        let mut report = Completion::new();
        prepared.with_local(
            |worker| {
                worker.lend(|node, stage| {
                    node.install(stage, |host, stage| {
                        let timer = stage.driver().driver_ref().timer();
                        stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                            let redis = runtime.root_client();

                            assert!(matches!(runtime.block_on(redis.wait_active()), Ok(Ok(()))));
                            assert!(matches!(
                                runtime.block_on(redis.server().ping()),
                                Ok(Err(cartel_redis::value::reply::Failure::Control(
                                    cartel_redis::Error::RequestTimeout(timeout)
                                )))
                                    if timeout.phase() == cartel_redis::RequestPhase::Response
                            ));
                            assert!(
                                matches!(
                                    fibers::TEST.try_run(&mut runtime, redis.wait_active()),
                                    Ok(Ok(()))
                                ),
                                "replacement connection becomes active"
                            );
                            let mut ping = pin!(redis.server().ping());
                            let mut alarms = Vec::new();
                            let result = runtime.block_on(poll_fn(|context| {
                                while let Ok(alarm) = timer.try_alarm() {
                                    alarms.push(alarm);
                                }
                                // SAFETY: ping stays pinned here; Ready ends this closure's
                                // polling authority while retaining the completed command.
                                let poll = unsafe { Live::new(ping.as_mut()) }.poll(context);
                                if poll.is_ready() {
                                    assert!(timer.try_alarm().is_ok(), "completed command timer");
                                }
                                poll
                            }));
                            assert!(
                                matches!(result, Ok(Ok(()))),
                                "replacement connection serves the command"
                            );
                        })
                    })
                })
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            |result| assert!(matches!(result, Ok(Ok(Ok(())))), "Redis scope"),
        );
        reports.events().expect("valid reclaim observations");
        server
            .finish()
            .expect("Redis lifecycle")
            .expect("Redis server");
    })
    .expect("start Redis server");
}

#[test]
fn lease_timeout_reconnects() {
    let config = config(Positive::from_millis::<10>()).expect("valid redis configuration");
    AcceptScript::with(recovery_server, |server| {
        let prepared = topology::try_new::<Env>(config, server.addr(), Plan::new())
            .expect("topology fixture completed");
        let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
        let mut report = Completion::new();
        prepared.with_local(
            |worker| {
                worker.lend(|node, stage| {
                    node.install(stage, |host, stage| {
                        stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                            let redis = runtime.root_client();

                            assert!(matches!(runtime.block_on(redis.wait_active()), Ok(Ok(()))));
                            let conn = runtime
                                .block_on(redis.connection())
                                .expect("drive active connection");
                            let mut conn = conn.ok().expect("lease active connection");
                            assert!(matches!(
                                runtime.block_on(conn.ping()),
                                Ok(Err(cartel_redis::value::reply::Failure::Control(
                                    cartel_redis::Error::RequestTimeout(timeout)
                                )))
                                    if timeout.phase() == cartel_redis::RequestPhase::Response
                            ));
                            assert!(
                                matches!(
                                    fibers::TEST.try_run(&mut runtime, redis.wait_active()),
                                    Ok(Ok(()))
                                ),
                                "replacement connection becomes active"
                            );
                            let next = runtime
                                .block_on(redis.connection())
                                .expect("drive replacement lease");
                            let mut next = next.ok().expect("lease replacement generation");
                            assert!(matches!(
                                runtime.block_on(conn.ping()),
                                Ok(Err(cartel_redis::value::reply::Failure::Control(
                                    cartel_redis::Error::Unavailable
                                )))
                            ));
                            drop(conn);
                            assert!(
                                matches!(runtime.block_on(next.ping()), Ok(Ok(()))),
                                "dropping the retired lease preserves the replacement lease"
                            );
                        })
                    })
                })
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            |result| assert!(matches!(result, Ok(Ok(Ok(())))), "Redis scope"),
        );
        reports.events().expect("valid reclaim observations");
        server
            .finish()
            .expect("Redis lifecycle")
            .expect("Redis server");
    })
    .expect("start Redis server");
}

#[test]
fn release_wakes_waiters() {
    let config = Builder::new(settings::Capacities {
        connections: 2,
        admission_waiters: 2,
        inflight: 2,
        request_frame_bytes: 128,
        response_bytes: 256,
        response_values: 2,
        response_frame_bytes: 128,
        response_frame_values: 1,
    })
    .window(Positive::from_secs::<1>())
    .build()
    .expect("valid redis configuration");
    AcceptScript::with(
        |peer| {
            let sockets = [accept_within(peer)?, accept_within(peer)?];
            let mut sockets = sockets.each_ref();
            let first = first_request(&mut sockets, PING, &mut [0; PING.len()])?;
            sockets[first].write_all(b"+PONG\r\n")?;
            for socket in sockets {
                socket.read_wait(Some(Duration::from_millis(500)))?;
                wait_closed(socket)?;
            }
            Ok::<_, peer::Error>(())
        },
        |server| {
            let prepared = topology::try_new::<Env>(config, server.addr(), Plan::new())
                .expect("topology fixture completed");
            let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
            let mut report = Completion::new();
            prepared.with_local(
                |worker| {
                    worker.lend(|node, stage| {
                        node.install(stage, |host, stage| {
                            stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                                let redis = runtime.root_client();

                                let first = runtime
                                    .block_on(redis.connection())
                                    .expect("drive first connection");
                                let first = first.ok().expect("lease first connection");
                                let second = runtime
                                    .block_on(redis.connection())
                                    .expect("drive second connection");
                                let second = second.ok().expect("lease second connection");
                                let joined = Box::pin(Task::new(Join::new(
                                    redis.server().ping(),
                                    redis.connection(),
                                )));
                                let release = release_after_poll(joined, (first, second));
                                let result = runtime.block_on(release);
                                let (ping, lease) = result.expect("drive released waiters");
                                assert!(ping.is_ok(), "released request must complete");
                                assert!(lease.is_ok(), "released lease must wake its waiter");
                            })
                        })
                    })
                },
                recovery::Policy::new(|event| reports.push(event), recover),
                |result| assert!(matches!(result, Ok(Ok(Ok(())))), "Redis scope"),
            );
            reports.events().expect("valid reclaim observations");
            server
                .finish()
                .expect("Redis lifecycle")
                .expect("Redis server");
        },
    )
    .expect("start Redis server");
}

#[test]
fn idle_lease_remains() {
    let config = Builder::new(settings::Capacities {
        connections: 2,
        admission_waiters: 1,
        inflight: 2,
        request_frame_bytes: 128,
        response_bytes: 256,
        response_values: 2,
        response_frame_bytes: 128,
        response_frame_values: 1,
    })
    .window(Positive::from_millis::<20>())
    .build()
    .expect("valid redis configuration");
    AcceptScript::with(
        |peer| {
            for socket in [accept_within(peer)?, accept_within(peer)?] {
                socket.read_wait(Some(Duration::from_millis(500)))?;
                match socket.read(&mut [0; 128]) {
                    Ok(0) | Err(peer::Error::TimedOut) => {}
                    Ok(_) => wait_closed(&socket)?,
                    Err(error) => return Err(error),
                }
            }
            Ok(())
        },
        |server| {
            let prepared = topology::try_new::<Env>(config, server.addr(), Plan::new())
                .expect("topology fixture completed");
            let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
            let mut report = Completion::new();
            prepared.with_local(
                |worker| {
                    worker.lend(|node, stage| {
                        node.install(stage, |host, stage| {
                            stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                                let redis = runtime.root_client();

                                let blocked = runtime
                                    .block_on(redis.connection())
                                    .expect("drive blocked connection");
                                let mut blocked = blocked.ok().expect("lease blocked connection");
                                let idle = runtime
                                    .block_on(redis.connection())
                                    .expect("drive idle connection");
                                let idle = idle.ok().expect("lease idle connection");
                                assert!(
                                    matches!(
                                        runtime.block_on(fibers::TEST.poll_once(blocked.ping())),
                                        Ok(None)
                                    ),
                                    "leased command completed before detachment"
                                );
                                drop(blocked);
                                drop(idle);
                                assert!(
                                    matches!(runtime.block_on(redis.connection()), Ok(Ok(_))),
                                    "released connections must return to the pool"
                                );
                            })
                        })
                    })
                },
                recovery::Policy::new(|event| reports.push(event), recover),
                |result| assert!(matches!(result, Ok(Ok(Ok(())))), "Redis scope"),
            );
            reports.events().expect("valid reclaim observations");
            server
                .finish()
                .expect("Redis lifecycle")
                .expect("Redis server");
        },
    )
    .expect("start Redis server");
}
