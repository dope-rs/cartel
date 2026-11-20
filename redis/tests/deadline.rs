use cartel_redis::api::{Client as _, Commands as _, Connection as _};
use std::{net::SocketAddr, pin::pin, time::Duration};

use cartel_redis::client::settings::{self, Builder};
use common::{
    session::{
        config,
        deadline::*,
        held::{PING, first_request},
    },
    topology,
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
};
use o3::base::time::Positive;

pub mod common;

#[test]
fn waits_expire() {
    let config = config(Positive::from_millis::<5>()).expect("valid redis configuration");
    let reports = topology::run(config, SocketAddr::from(([127, 0, 0, 1], 0)), |mut runtime| {
        let redis = runtime.root_client();

        assert!(matches!(
            runtime.block_on(redis.server().ping()),
            Ok(Err(cartel_redis::value::reply::Failure::Control(
                cartel_redis::Error::RequestTimeout(timeout)
            )))
                if timeout.phase() == cartel_redis::RequestPhase::Admission
        ));
        let acquired = runtime
            .block_on(redis.acquire())
            .expect("frame drive")
            .ok()
            .expect("request frame");
        fibers::TEST
            .try_pause(&mut runtime, Duration::from_millis(10))
            .expect("original deadline fires");
        let mut command = pin!(Task::new(acquired.server().ping()));
        assert!(matches!(
            runtime.block_on(fibers::TEST.poll_once(command.as_mut())),
            Ok(Some(Err(cartel_redis::value::reply::Failure::Control(
                cartel_redis::Error::RequestTimeout(timeout)
            )))) if timeout.phase() == cartel_redis::RequestPhase::Admission
        ));
        let mut acquire = pin!(redis.connection());
        let mut alarms = Vec::new();
        let result = runtime.block_on(poll_fn(|mut context| {
            let timer = context.as_mut().driver().driver_ref().timer();
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
    });
    reports.events().expect("valid reclaim observations");
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
            let reports = topology::run(config, server.addr(), |mut runtime| {
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
            });
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
        let reports = topology::run(config, server.addr(), |mut runtime| {
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
            let result = runtime.block_on(poll_fn(|mut context| {
                let timer = context.as_mut().driver().driver_ref().timer();
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
        });
        reports.events().expect("valid reclaim observations");
        server
            .finish()
            .expect("Redis lifecycle")
            .expect("Redis server");
    })
    .expect("start Redis server");
}

#[test]
fn completed_pool_timeout_preserves_new_lease() {
    let config = config(Positive::from_millis::<100>()).expect("valid redis configuration");
    AcceptScript::with(
        |peer| {
            let socket = accept_within(peer)?;
            socket.read_wait(Some(GUARD))?;
            let mut request = [0; PING.len()];
            socket.read_exact(&mut request)?;
            assert_eq!(request, PING);
            socket.write_all(b"-ERR completed\r\n")?;
            common::session::held::pong_once(&socket)
        },
        |server| {
            let reports = topology::run(config, server.addr(), |mut runtime| {
                let redis = runtime.root_client();
                assert!(matches!(
                    runtime.block_on(redis.wait_active()),
                    Ok(Ok(()))
                ));
                let mut old = Box::pin(Task::new(redis.server().ping()));
                assert!(
                    runtime
                        .block_on(fibers::TEST.poll_once(old.as_mut()))
                        .expect("submit pooled command")
                        .is_none()
                );
                let mut lease = fibers::TEST
                    .try_run(&mut runtime, redis.connection())
                    .expect("drive replacement lease")
                    .ok()
                    .expect("completed reply releases connection order");
                fibers::TEST
                    .try_pause(&mut runtime, Duration::from_millis(200))
                    .expect("expire completed command");
                assert!(matches!(
                    fibers::TEST.try_run(&mut runtime, old.as_mut()),
                    Ok(Err(cartel_redis::value::reply::Failure::Control(
                        cartel_redis::Error::RequestTimeout(timeout)
                    ))) if timeout.phase() == cartel_redis::RequestPhase::Response
                ));
                assert!(
                    matches!(
                        fibers::TEST.try_run(&mut runtime, lease.ping()),
                        Ok(Ok(()))
                    ),
                    "completed pooled timeout preserves the new lease"
                );
            });
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
fn lease_timeout_reconnects() {
    let config = config(Positive::from_millis::<10>()).expect("valid redis configuration");
    AcceptScript::with(recovery_server, |server| {
        let reports = topology::run(config, server.addr(), |mut runtime| {
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
        });
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
            let reports = topology::run(config, server.addr(), |mut runtime| {
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
            });
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
            let reports = topology::run(config, server.addr(), |mut runtime| {
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
            });
            reports.events().expect("valid reclaim observations");
            server
                .finish()
                .expect("Redis lifecycle")
                .expect("Redis server");
        },
    )
    .expect("start Redis server");
}
