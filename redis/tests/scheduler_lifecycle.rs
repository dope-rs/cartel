use cartel_redis::api::Client as _;
use cartel_redis::value::source::Bulks as _;
use dope::core::driver::life::recovery;
use dope::runtime::executor::session::Completion;
use std::{
    fmt, io,
    io::{Read as _, Write as _},
    mem,
    net::Shutdown,
    sync::mpsc,
    time::Duration,
};

use cartel_redis::{
    client::{
        settings,
        settings::{Builder, Bulk, init::Plan},
    },
    value::{self, reply},
};
use common::{
    recover,
    session::{config, held::*},
    topology,
};
use dope::{
    core::driver::route::Id,
    manifold::{app::env::Bundle, timing::Throughput},
    net::{tcp::Tcp, wire::Identity},
};
use dope_fiber::{BlockOn, abi::Join};
use dope_test::{
    fibers,
    harness::{GUARD, peer::AcceptScript},
    reclaim::Reports,
};
use o3::base::time::Positive;

type Env = Bundle<Tcp, Identity, Throughput>;

pub mod common;

fn assert_committed_close<T, B>(result: Result<T, reply::Failure<cartel_redis::Error<'_>, B>>) {
    let error = result
        .err()
        .expect("a saved request must fail when its peer closes");
    assert!(
        matches!(
            error,
            value::reply::Failure::Control(cartel_redis::Error::Closed(
                dope::manifold::client::close::Reason::Remote
                    | dope::manifold::client::close::Reason::Transport
            ))
        ),
        "unexpected close failure"
    );
}

#[test]
fn inflight_close_drains() {
    let config = config(Positive::from_secs::<1>()).expect("valid redis configuration");
    AcceptScript::with(
        |peer| {
            let mut failed = peer.accept()?;
            failed.read_wait(Some(Duration::from_secs(1)))?;
            let mut buf = [0_u8; PING.len() * 2];
            failed.read_exact(&mut buf)?;
            assert_eq!(&buf[..PING.len()], PING);
            assert_eq!(&buf[PING.len()..], PING);
            failed.shutdown(Shutdown::Both)?;
            let socket = peer.accept()?;
            pong_once(&socket)
        },
        |server| {
            let addr = server.addr();

            {
                let prepared = topology::try_new::<Env>(config, addr, Plan::new())
                    .expect("topology fixture completed");
                let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
                let mut report = Completion::new();
                prepared.with_local(
                    |worker| {
                        worker.lend(|node, stage| {
                            node.install(stage, |host, stage| {
                                stage.with_quiesce(&mut report, host, GUARD, |mut rt| {
                                    let redis = rt.root_client();

                                    assert!(matches!(
                                        BlockOn::block_on(&mut rt, redis.wait_active()),
                                        Ok(Ok(()))
                                    ));
                                    let result = BlockOn::block_on(
                                        &mut rt,
                                        Join::new(redis.server().ping(), redis.server().ping()),
                                    );
                                    let (first, second) = result.expect("drive saved requests");
                                    assert_committed_close(first);
                                    assert_committed_close(second);
                                    assert!(matches!(
                                        BlockOn::block_on(&mut rt, redis.wait_active()),
                                        Ok(Ok(()))
                                    ));
                                    assert!(matches!(
                                        BlockOn::block_on(&mut rt, redis.server().ping()),
                                        Ok(Ok(()))
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
            assert!(
                matches!(server.finish(), Ok(Ok(()))),
                "finish Redis peer; Redis peer I/O"
            );
        },
    )
    .expect("spawn failing Redis peer");
}

#[test]
fn mixed_close_drains() {
    const ARGUMENT_BYTES: usize = 300 * 1024;

    let mut wire = b"*4\r\n$4\r\nEVAL\r\n$8\r\nreturn 1\r\n$1\r\n0\r\n".to_vec();
    wire.extend_from_slice(format!("${ARGUMENT_BYTES}\r\n").as_bytes());
    wire.resize(wire.len() + ARGUMENT_BYTES, b'x');
    wire.extend_from_slice(b"\r\n");
    let bytes = wire.len();
    AcceptScript::with(
        move |peer| {
            let mut failed = peer.accept()?;
            failed.read_wait(Some(Duration::from_secs(1)))?;
            let mut buf = vec![0_u8; PING.len() + wire.len()];
            failed.read_exact(&mut buf)?;
            assert!(
                buf == [PING, wire.as_slice()].concat() || buf == [wire.as_slice(), PING].concat()
            );
            failed.shutdown(Shutdown::Both)?;
            let socket = peer.accept()?;
            pong_once(&socket)
        },
        |server| {
            let addr = server.addr();
            let config = Builder::new(settings::Capacities {
                connections: 1,
                admission_waiters: 2,
                inflight: 2,
                request_frame_bytes: 128,
                response_bytes: 128,
                response_values: 2,
                response_frame_bytes: 128,
                response_frame_values: 2,
            })
            .bulk(Bulk::new(1, bytes))
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
                                stage.with_quiesce(&mut report, host, GUARD, |mut rt| {
                                    let redis = rt.root_client();

                                    assert!(matches!(
                                        BlockOn::block_on(&mut rt, redis.wait_active()),
                                        Ok(Ok(()))
                                    ));
                                    let keys: [Vec<u8>; 0] = [];
                                    let args = [vec![b'x'; ARGUMENT_BYTES]];
                                    let result = BlockOn::block_on(
                                        &mut rt,
                                        Join::new(
                                            redis.server().ping(),
                                            redis.scripts().eval::<i64, _, _>(
                                                b"return 1",
                                                &keys,
                                                &args,
                                            ),
                                        ),
                                    );
                                    let (direct, bulk) =
                                        result.expect("drive direct and bulk requests");
                                    assert_committed_close(direct);
                                    assert_committed_close(bulk);
                                    assert!(matches!(
                                        BlockOn::block_on(&mut rt, redis.wait_active()),
                                        Ok(Ok(()))
                                    ));
                                    assert!(matches!(
                                        BlockOn::block_on(&mut rt, redis.server().ping()),
                                        Ok(Ok(()))
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
            assert!(
                matches!(server.finish(), Ok(Ok(()))),
                "finish Redis peer; Redis peer I/O"
            );
        },
    )
    .expect("spawn failing Redis peer");
}

#[test]
fn bulk_admission() {
    const CAPACITY: usize = 16 * 1024 * 1024;
    const ARGUMENT_BYTES: usize = 338_000;

    let mut wire = b"*4\r\n$4\r\nEVAL\r\n$8\r\nreturn 1\r\n$1\r\n0\r\n".to_vec();
    wire.extend_from_slice(format!("${ARGUMENT_BYTES}\r\n").as_bytes());
    wire.resize(wire.len() + ARGUMENT_BYTES, b'x');
    wire.extend_from_slice(b"\r\n");

    AcceptScript::with(
        move |peer| {
            let mut socket = peer.accept()?;
            socket.read_wait(Some(Duration::from_secs(2)))?;
            let mut received = vec![0_u8; wire.len()];
            socket.read_exact(&mut received)?;
            assert_eq!(received, wire, "bulk frame must reach the wire");
            socket.write_all(b":1\r\n")?;
            let mut ping = [0_u8; PING.len()];
            socket.read_exact(&mut ping)?;
            assert_eq!(ping, PING, "capacity must return after the bulk frame");
            socket.write_all(b"+PONG\r\n")
        },
        |server| {
            let addr = server.addr();
            let config = Builder::new(settings::Capacities {
                connections: 1,
                admission_waiters: 1,
                inflight: 1,
                request_frame_bytes: 256 * 1024,
                response_bytes: 64,
                response_values: 1,
                response_frame_bytes: 64,
                response_frame_values: 1,
            })
            .bulk(Bulk::new(1, CAPACITY))
            .window(Positive::from_secs::<1>())
            .build()
            .expect("production-shaped redis configuration");

            {
                let prepared = topology::try_new::<Env>(config, addr, Plan::new())
                    .expect("topology fixture completed");
                let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
                let mut report = Completion::new();
                prepared.with_local(
                    |worker| {
                        worker.lend(|node, stage| {
                            node.install(stage, |host, stage| {
                                stage.with_quiesce(&mut report, host, GUARD, |mut rt| {
                                    let redis = rt.root_client();

                                    assert!(matches!(
                                        BlockOn::block_on(&mut rt, redis.wait_active()),
                                        Ok(Ok(()))
                                    ));
                                    let keys: [Vec<u8>; 0] = [];
                                    let args = [vec![b'x'; ARGUMENT_BYTES]];
                                    let value = BlockOn::block_on(
                                        &mut rt,
                                        redis.scripts().eval::<i64, _, _>(b"return 1", &keys, &args),
                                    );
                                    assert!(
                                        matches!(value, Ok(Ok(1))),
                                        "a frame past the direct limit must be admitted through the bulk buffer"
                                    );
                                    assert!(matches!(
                                        BlockOn::block_on(&mut rt, redis.server().ping()),
                                        Ok(Ok(()))
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
            assert!(
                matches!(server.finish(), Ok(Ok(()))),
                "finish Redis peer; Redis peer I/O"
            );
        },
    )
    .expect("spawn Redis peer");
}

#[test]
fn binding_reconnects() {
    use cartel_redis::Error::{Closed, Receive, RequestTimeout, ResponseRetention, Timeout};
    use reply::Failure;

    let config = config(Positive::from_millis::<25>()).expect("valid redis configuration");
    AcceptScript::with(
        |peer| {
            let socket = peer.accept()?;
            pong_once(&socket)?;
            drop(socket);
            let socket = peer.accept()?;
            pong_once(&socket)
        },
        |server| {
            let addr = server.addr();

            {
                let prepared = topology::try_new::<Env>(config, addr, Plan::new())
                    .expect("topology fixture completed");
                let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
                let mut report = Completion::new();
                prepared.with_local(
                    |worker| {
                        worker.lend(|node, stage| {
                            node.install(stage, |host, stage| {
                                stage.with_quiesce(&mut report, host, GUARD, |mut rt| {
                                    let redis = rt.root_client();

                                    assert!(matches!(
                                        BlockOn::block_on(&mut rt, redis.wait_active()),
                                        Ok(Ok(()))
                                    ));
                                    let first = BlockOn::block_on(&mut rt, redis.server().ping());
                                    let cause: Option<(&str, &dyn fmt::Debug)> = match &first {
                                        Ok(Err(Failure::Control(Closed(value)))) => {
                                            Some(("Closed", value))
                                        }
                                        Ok(Err(Failure::Control(RequestTimeout(value)))) => {
                                            Some(("RequestTimeout", value))
                                        }
                                        Ok(Err(Failure::Control(Timeout(value)))) => {
                                            Some(("Timeout", value))
                                        }
                                        Ok(Err(Failure::Control(Receive(value)))) => {
                                            Some(("Receive", value))
                                        }
                                        Ok(Err(Failure::Control(ResponseRetention(value)))) => {
                                            Some(("ResponseRetention", value))
                                        }
                                        _ => None,
                                    };
                                    assert!(
                                        matches!(&first, Ok(Ok(()))),
                                        "initial PING: driver={:?}, cause={cause:?}",
                                        first.as_ref().err(),
                                    );

                                    let result = BlockOn::block_on(&mut rt, redis.server().ping());
                                    let result = result.expect("drive reconnecting request");
                                    if result.is_err() {
                                        assert!(matches!(
                                            BlockOn::block_on(&mut rt, redis.wait_active()),
                                            Ok(Ok(()))
                                        ));
                                        assert!(matches!(
                                            BlockOn::block_on(&mut rt, redis.server().ping()),
                                            Ok(Ok(()))
                                        ));
                                    }
                                })
                            })
                        })
                    },
                    recovery::Policy::new(|event| reports.push(event), recover),
                    |result| assert!(matches!(result, Ok(Ok(Ok(())))), "Redis scope"),
                );
                reports.events().expect("valid reclaim observations");
            };
            assert!(
                matches!(server.finish(), Ok(Ok(()))),
                "finish Redis peer; Redis peer I/O"
            );
        },
    )
    .expect("spawn Redis peer");
}

#[test]
fn waiter_slot_reclaims() {
    let (allow, gate) = mpsc::channel();
    let (stop, done) = mpsc::channel();
    AcceptScript::with(
        move |peer| {
            let mut socket = peer.accept()?;
            socket.read_wait(Some(Duration::from_secs(1)))?;
            let mut buf = [0_u8; 128];
            assert_ne!(socket.read(&mut buf)?, 0);
            (gate.recv_timeout(GUARD)).expect("allow AUTH response");
            socket.write_all(b"+OK\r\n")?;
            (done.recv_timeout(GUARD)).expect("finish active connection");
            Ok::<_, io::Error>(())
        },
        |server| {
            let addr = server.addr();

            let config = Builder::new(settings::Capacities {
                connections: 1,
                admission_waiters: 1,
                inflight: 1,
                request_frame_bytes: 128,
                response_bytes: 128,
                response_values: 1,
                response_frame_bytes: 128,
                response_frame_values: 1,
            })
            .window(Positive::from_millis::<1_000>())
            .build()
            .expect("valid redis configuration");
            let init =
                (Plan::new().password("secret")).expect("authentication storage must allocate");
            {
                let prepared = topology::try_new::<Env>(config, addr, init)
                    .expect("topology fixture completed");
                let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
                let mut report = Completion::new();
                prepared.with_local(
                    |worker| {
                        worker.lend(|node, stage| {
                            node.install(stage, |host, stage| {
                                stage.with_quiesce(&mut report, host, GUARD, |mut rt| {
                                    let redis = rt.root_client();

                                    let wait = redis.wait_active();
                                    assert_eq!(
                                        mem::size_of_val(&wait),
                                        6 * mem::size_of::<usize>()
                                    );
                                    let result =
                                        BlockOn::block_on(&mut rt, retain_after_pending(wait));
                                    let (held, waits) = result.expect("poll original waiter");
                                    assert!(
                                        waits,
                                        "readiness completed before the gated handshake"
                                    );

                                    assert!(matches!(
                                        BlockOn::block_on(&mut rt, redis.wait_active()),
                                        Ok(Err(cartel_redis::Error::Wait(
                                            dope_fiber::wait::Error::QueueFull { capacity, .. }
                                        ))) if capacity == dope_fiber::wait::Capacity::new(1)
                                    ));

                                    drop(held);
                                    let result = BlockOn::block_on(
                                        &mut rt,
                                        retain_after_pending(redis.wait_active()),
                                    );
                                    let (wait, waits) = result.expect("poll replacement waiter");
                                    assert!(waits, "replacement waiter was not registered");
                                    assert!(allow.send(()).is_ok(), "release AUTH response");
                                    assert!(matches!(BlockOn::block_on(&mut rt, wait), Ok(Ok(()))));
                                    assert!(stop.send(()).is_ok(), "finish active connection");
                                })
                            })
                        })
                    },
                    recovery::Policy::new(|event| reports.push(event), recover),
                    |result| assert!(matches!(result, Ok(Ok(Ok(())))), "Redis scope"),
                );
                reports.events().expect("valid reclaim observations");
            };
            assert!(
                matches!(server.finish(), Ok(Ok(()))),
                "finish Redis readiness peer; Redis readiness peer I/O"
            );
        },
    )
    .expect("spawn Redis readiness peer");
}

#[test]
fn parser_lane_waits_for_retained_replies() {
    const REQUEST: &[u8] = b"*2\r\n$8\r\nSMEMBERS\r\n$3\r\nset\r\n";
    let config = Builder::new(settings::Capacities {
        connections: 2,
        admission_waiters: 2,
        inflight: 3,
        request_frame_bytes: 128,
        response_bytes: 128,
        response_values: 3,
        response_frame_bytes: 64,
        response_frame_values: 3,
    })
    .parser_lanes(1)
    .window(Positive::from_secs::<2>())
    .build()
    .expect("one parser lane with retained reply capacity");
    AcceptScript::with(
        |peer| {
            let left = peer.accept()?;
            let right = peer.accept()?;
            let mut sockets = [&left, &right];
            let first = first_request(&mut sockets, REQUEST)?;
            let (mut a, b) = if first == 0 {
                (&left, &right)
            } else {
                (&right, &left)
            };
            b.read_wait(Some(GUARD))?;
            a.write_all(b"*2\r\n$3\r\none\r\n$3\r\nuno\r\n")?;
            for (mut socket, reply) in [
                (b, b"*2\r\n$3\r\ntwo\r\n$3\r\ndos\r\n".as_slice()),
                (a, b"*2\r\n$3\r\ntri\r\n$4\r\ntres\r\n".as_slice()),
            ] {
                let mut request = [0; REQUEST.len()];
                socket.read_exact(&mut request)?;
                assert_eq!(request, REQUEST);
                socket.write_all(reply)?;
            }
            pong_once(b)?;
            pong_once(a)
        },
        |server| {
            let prepared = topology::try_new::<Env>(config, server.addr(), Plan::new())
                .expect("Redis topology");
            let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
            let mut report = Completion::new();
            prepared.with_local(
                |worker| {
                    worker.lend(|node, stage| {
                        node.install(stage, |host, stage| {
                            stage.with_quiesce(&mut report, host, GUARD, |mut rt| {
                                let redis = rt.root_client();
                                {
                                    let (a, b) = fibers::TEST
                                        .try_run(
                                            &mut rt,
                                            Join::new(redis.connection(), redis.connection()),
                                        )
                                        .expect("activate both connections");
                                    assert!(a.is_ok() && b.is_ok());
                                }
                                let held = fibers::TEST
                                    .try_run(&mut rt, redis.sets().members(b"set"))
                                    .expect("first array operation")
                                    .ok()
                                    .expect("first retained array");
                                assert!(held.iter().eq([b"one".as_slice(), b"uno".as_slice()]));
                                let (mut second, waits) = rt
                                    .block_on(retain_after_pending(redis.sets().members(b"set")))
                                    .expect("submit second array");
                                assert!(waits);
                                fibers::TEST
                                    .try_pause(&mut rt, Duration::from_millis(20))
                                    .expect("retain the second decoded reply");
                                let (mut third, waits) = rt
                                    .block_on(retain_after_pending(redis.sets().members(b"set")))
                                    .expect("submit third array");
                                assert!(waits);
                                fibers::TEST
                                    .try_pause(&mut rt, Duration::from_millis(20))
                                    .expect("drive parser backpressure");
                                assert!(
                                    rt.block_on(second.poll_pending()).expect("second pending")
                                );
                                assert!(rt.block_on(third.poll_pending()).expect("third pending"));
                                drop(held);
                                {
                                    let values = fibers::TEST
                                        .try_run(&mut rt, second)
                                        .expect("resume second array")
                                        .ok()
                                        .expect("second reply preserves its connection");
                                    assert!(
                                        values.iter().eq([b"two".as_slice(), b"dos".as_slice()])
                                    );
                                    assert!(
                                        rt.block_on(third.poll_pending())
                                            .expect("third waits for credit")
                                    );
                                }
                                {
                                    let values = fibers::TEST
                                        .try_run(&mut rt, third)
                                        .expect("resume third array")
                                        .ok()
                                        .expect("third reply preserves FIFO");
                                    assert!(
                                        values.iter().eq([b"tri".as_slice(), b"tres".as_slice()])
                                    );
                                }
                                let (a, b) = fibers::TEST
                                    .try_run(
                                        &mut rt,
                                        Join::new(redis.server().ping(), redis.server().ping()),
                                    )
                                    .expect("reuse both original connections");
                                assert!(a.is_ok() && b.is_ok());
                            })
                        })
                    })
                },
                recovery::Policy::new(|event| reports.push(event), recover),
                |result| assert!(matches!(result, Ok(Ok(Ok(())))), "Redis scope"),
            );
            reports.events().expect("valid reclaim observations");
            assert!(
                matches!(server.finish(), Ok(Ok(()))),
                "Redis peer completion"
            );
        },
    )
    .expect("spawn Redis peer");
}
