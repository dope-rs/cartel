use cartel_redis::api::{Client as _, Connection as _};
use dope::core::driver::life::recovery;
use dope::runtime::executor::session::Completion;
use std::{
    io::{ErrorKind, Read as _, Write as _},
    iter,
    net::{SocketAddr, TcpListener, TcpStream},
    thread,
    time::{Duration, Instant},
};

use cartel_redis::client::{
    settings,
    settings::{Builder, init::Plan},
};
use common::{
    recover,
    session::{config, deadline::*},
    topology,
};
use dope::{
    core::{
        driver::route::Id,
        platform::wake::{Ends, Notify},
    },
    manifold::{app::env::Bundle, timing::Throughput},
    net::{tcp::Tcp, wire::Identity},
};
use dope_fiber::{
    BlockOn as _,
    abi::{Join, Task},
};
use dope_test::{fibers, harness::GUARD, reclaim::Reports};
use o3::base::time::Positive;

type Env = Bundle<Tcp, Identity, Throughput>;

pub mod common;

fn accept_within(peer: &TcpListener, limit: Duration) -> TcpStream {
    (peer.set_nonblocking(true)).expect("nonblocking listener");
    let until = Instant::now() + limit;
    loop {
        match peer.accept() {
            Err(error) if error.kind() == ErrorKind::WouldBlock && Instant::now() < until => {
                thread::yield_now();
            }
            outcome => {
                let (socket, _) = outcome.expect("connection was not accepted before deadline");
                socket.set_nonblocking(false).expect("blocking socket");
                return socket;
            }
        }
    }
}

fn wait_closed(socket: &mut TcpStream) {
    let mut bytes = [0; 128];
    loop {
        match socket.read(&mut bytes) {
            Ok(0) => return,
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::ConnectionReset => return,
            outcome => {
                outcome.expect("client did not close its timed-out connection");
            }
        }
    }
}

fn blackhole_server(peer: TcpListener) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut socket = accept_within(&peer, Duration::from_secs(1));
        (socket.set_read_timeout(Some(Duration::from_secs(1)))).expect("required I/O outcome");
        let mut buf = [0_u8; 128];
        assert!(matches!(socket.read(&mut buf), Ok(count) if count != 0));
        wait_closed(&mut socket);
    })
}

fn recovery_server(peer: TcpListener, ready: Notify) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut first = accept_within(&peer, Duration::from_secs(1));
        (first.set_read_timeout(Some(Duration::from_secs(1)))).expect("required I/O outcome");
        let mut buf = [0_u8; 128];
        assert!(matches!(first.read(&mut buf), Ok(count) if count != 0));

        let mut next = accept_within(&peer, Duration::from_secs(1));
        ready.notify().expect("accepted replacement connection");
        (next.set_read_timeout(Some(Duration::from_secs(1)))).expect("required I/O outcome");
        let _ = first.write_all(b"-ERR stale generation\r\n");
        assert!(matches!(next.read(&mut buf), Ok(count) if count != 0));
        (next.write_all(b"+PONG\r\n")).expect("PONG response");
    })
}

fn pooled_server(peer: TcpListener, count: usize, pong: bool) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let peers: Vec<_> = iter::repeat_with(|| accept_within(&peer, Duration::from_secs(1)))
            .take(count)
            .collect();
        let jobs: Vec<_> = peers
            .into_iter()
            .map(|mut socket| {
                thread::spawn(move || {
                    (socket.set_read_timeout(Some(Duration::from_millis(500))))
                        .expect("required I/O outcome");
                    let mut buf = [0_u8; 128];
                    match socket.read(&mut buf) {
                        Ok(0) => {}
                        Ok(_) if pong => socket.write_all(b"+PONG\r\n").expect("PONG response"),
                        Ok(_) => wait_closed(&mut socket),
                        Err(error)
                            if matches!(
                                error.kind(),
                                ErrorKind::WouldBlock | ErrorKind::TimedOut
                            ) => {}
                        outcome => {
                            outcome.expect("Redis request read failed");
                        }
                    }
                })
            })
            .collect();
        for job in jobs {
            (job.join()).expect("Redis worker");
        }
    })
}

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
                        stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                            let redis = runtime.root_client();

                            assert!(matches!(
                                runtime.block_on(redis.server().ping()),
                                Ok(Err(cartel_redis::value::reply::Failure::Control(
                                    cartel_redis::Error::RequestTimeout(timeout)
                                )))
                                    if timeout.phase() == cartel_redis::RequestPhase::Admission
                            ));
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
    };
}

#[test]
fn detached_blocks_lease() {
    let config = config(Positive::from_millis::<10>()).expect("valid redis configuration");
    let peer =
        (TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))).expect("bind Redis listener");
    let addr = (peer.local_addr()).expect("Redis listener address");
    let server = blackhole_server(peer);
    {
        let prepared = topology::try_new::<Env>(config, addr, Plan::new())
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
                            assert!(
                                matches!(
                                    runtime.block_on(fibers::TEST.poll_once(redis.server().ping())),
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
    };
    (server.join()).expect("Redis server");
}

#[test]
fn pool_timeout_reconnects() {
    let config = config(Positive::from_millis::<10>()).expect("valid redis configuration");
    let peer =
        (TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))).expect("bind Redis listener");
    let addr = (peer.local_addr()).expect("Redis listener address");
    let (ready, accepted) = Ends::blocking().expect("replacement signal").split();
    let server = recovery_server(peer, accepted);
    {
        let prepared = topology::try_new::<Env>(config, addr, Plan::new())
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
                            assert!(matches!(
                                runtime.block_on(redis.server().ping()),
                                Ok(Err(cartel_redis::value::reply::Failure::Control(
                                    cartel_redis::Error::RequestTimeout(timeout)
                                )))
                                    if timeout.phase() == cartel_redis::RequestPhase::Response
                            ));
                            let started = Instant::now();
                            assert!(
                                matches!(
                                    fibers::TEST.try_run(&mut runtime, redis.wait_active()),
                                    Ok(Ok(()))
                                ),
                                "replacement connection becomes active"
                            );
                            ready
                                .wait_timeout(GUARD.saturating_sub(started.elapsed()))
                                .expect("replacement TCP connection must be accepted");
                            assert!(
                                matches!(runtime.block_on(redis.server().ping()), Ok(Ok(()))),
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
    };
    (server.join()).expect("Redis server");
}

#[test]
fn lease_timeout_reconnects() {
    let config = config(Positive::from_millis::<10>()).expect("valid redis configuration");
    let peer =
        (TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))).expect("bind Redis listener");
    let addr = (peer.local_addr()).expect("Redis listener address");
    let (ready, accepted) = Ends::blocking().expect("replacement signal").split();
    let server = recovery_server(peer, accepted);
    {
        let prepared = topology::try_new::<Env>(config, addr, Plan::new())
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
                            let started = Instant::now();
                            assert!(
                                matches!(
                                    fibers::TEST.try_run(&mut runtime, redis.wait_active()),
                                    Ok(Ok(()))
                                ),
                                "replacement connection becomes active"
                            );
                            ready
                                .wait_timeout(GUARD.saturating_sub(started.elapsed()))
                                .expect("replacement TCP connection must be accepted");
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
    };
    (server.join()).expect("Redis server");
}

#[test]
fn release_wakes_waiters() {
    let peer =
        (TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))).expect("bind Redis listener");
    let addr = (peer.local_addr()).expect("Redis listener address");
    let server = pooled_server(peer, 2, true);
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
    {
        let prepared = topology::try_new::<Env>(config, addr, Plan::new())
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
    };
    (server.join()).expect("Redis server");
}

#[test]
fn idle_lease_remains() {
    let peer =
        (TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))).expect("bind Redis listener");
    let addr = (peer.local_addr()).expect("Redis listener address");
    let server = pooled_server(peer, 2, false);
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
    {
        let prepared = topology::try_new::<Env>(config, addr, Plan::new())
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
    };
    (server.join()).expect("Redis server");
}
