use cartel_redis::api::Client as _;
use cartel_redis::{
    client::settings::{
        self, Builder,
        init::{Plan, Step},
    },
    init, value,
};
use common::{
    recover,
    session::{config, hello::*},
    topology,
};
use dope::core::driver::life::recovery;
use dope::runtime::executor::session::Completion;
use dope::{
    core::driver::route::Id, manifold::app::env::Bundle, manifold::client::close,
    manifold::timing::Throughput, net::tcp::Tcp, net::wire::Identity,
};
use dope_fiber::BlockOn;
use dope_test::harness::peer;
use dope_test::{
    fibers::{self, TEST},
    harness::{GUARD, peer::AcceptScript},
    reclaim::Reports,
};
use o3::base::time::Positive;
use std::{net::Shutdown, sync::mpsc, time::Duration};

type Env = Bundle<Tcp, Identity, Throughput>;

pub mod common;

#[test]
fn init_accessors() {
    let init =
        (Plan::new().user("alice", "swordfish")).expect("authentication storage must allocate");
    let init = (init.select(3).name("worker")).expect("client name storage must allocate");
    assert_eq!(init.database(), Some(3));
    assert_eq!(init.client_name(), Some(b"worker".as_slice()));
}

#[test]
fn reconnect_replays_init() {
    let (finish, done) = mpsc::channel();
    AcceptScript::with(
        move |peer| {
            {
                let socket = peer.accept()?;
                assert_eq!(socket.read(&mut [0])?, 0, "AUTH needs a free request frame");
            }
            for epoch in 0..2 {
                let mut socket = peer.accept()?;
                for (request, step) in REQUESTS {
                    assert_eq!(
                        read_request(&mut socket, request.len())?,
                        request,
                        "{step:?}"
                    );
                    socket.write_all(b"+OK\r\n")?;
                }
                assert_eq!(read_request(&mut socket, PING.len())?, PING);
                socket.write_all(b"+PONG\r\n")?;
                if epoch == 1 {
                    done.recv_timeout(GUARD)
                        .expect("finish replacement connection");
                }
            }
            Ok::<_, peer::Error>(())
        },
        |server| {
            let init = Plan::new()
                .user("alice", "swordfish")
                .expect("authentication storage must allocate");
            let init = init
                .select(3)
                .name("worker")
                .expect("client name storage must allocate");
            let prepared = topology::try_new::<Env>(
                config(Positive::from_millis::<25>()).expect("valid redis configuration"),
                server.addr(),
                init,
            )
            .expect("topology fixture completed");
            let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
            let mut report = Completion::new();
            prepared.with_local(
                |worker| {
                    worker.lend(|node, stage| {
                        node.install(stage, |host, stage| {
                            stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                                let redis = runtime.root_client();

                                let held = dope_fiber::abi::Join::new(
                                    redis.server().ping(),
                                    redis.server().ping(),
                                );
                                assert!(matches!(
                                    TEST.try_run(&mut runtime, redis.wait_active()),
                                    Ok(Err(cartel_redis::Error::Request(
                                        cartel_redis::request::Error::Entries { .. }
                                    )))
                                ));
                                let (left, right) = TEST
                                    .try_run(&mut runtime, held)
                                    .expect("release waiting command frames");
                                for result in [left, right] {
                                    assert!(matches!(
                                        result,
                                        Err(value::reply::Failure::Control(
                                            cartel_redis::Error::Request(
                                                cartel_redis::request::Error::Entries { .. }
                                            )
                                        ))
                                    ));
                                }
                                TEST.try_run(
                                    &mut runtime,
                                    dope_fiber::fiber!('_ => async move {
                                        loop {
                                            match redis.wait_active().await {
                                                Ok(()) => break,
                                                Err(cartel_redis::Error::Request(
                                                    cartel_redis::request::Error::Entries { .. }
                                                )) => fibers::Yield::new().await,
                                                Err(error) => panic!(
                                                    "unexpected init failure: {:?}",
                                                    std::mem::discriminant(&error)
                                                ),
                                            }
                                        }
                                    }),
                                )
                                .expect("request capacity recovery must reconnect");
                                assert!(matches!(
                                    runtime.block_on(redis.server().ping()),
                                    Ok(Ok(_))
                                ));
                                assert!(
                                    matches!(runtime.block_on(redis.server().ping()), Ok(Err(_))),
                                    "closed connection must be observed before replacement"
                                );
                                assert!(matches!(runtime.block_on(redis.wait_active()), Ok(Ok(_))));
                                assert!(matches!(
                                    runtime.block_on(redis.server().ping()),
                                    Ok(Ok(_))
                                ));
                                finish.send(()).expect("finish server");
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
                .expect("finish Redis peer")
                .expect("Redis peer I/O");
        },
    )
    .expect("spawn Redis peer");
}

#[test]
fn rejection_stops_init() {
    for expected in [Step::Authenticate, Step::Select, Step::SetClientName] {
        let (seen, gate) = mpsc::channel();
        AcceptScript::with(
            move |peer| {
                let mut socket = peer.accept()?;
                for (request, step) in REQUESTS {
                    assert_eq!(
                        read_request(&mut socket, request.len())?,
                        request,
                        "{step:?}"
                    );
                    if step == expected {
                        socket.write_all(REJECTED)?;
                        gate.recv_timeout(GUARD).expect("failure observed");
                        socket.nonblock(true);
                        let mut byte = [0; 1];
                        match socket.read(&mut byte) {
                            Err(peer::Error::Syscall(error)) if error.is_block() => {}
                            outcome => {
                                assert_eq!(outcome?, 0, "a command followed rejected {expected:?}")
                            }
                        }
                        return Ok::<_, peer::Error>(Some(step));
                    }
                    socket.write_all(b"+OK\r\n")?;
                }
                Ok(None)
            },
            |server| {
                let init = Plan::new()
                    .user("alice", "swordfish")
                    .expect("authentication storage must allocate");
                let init = init
                    .select(3)
                    .name("worker")
                    .expect("client name storage must allocate");
                let prepared = topology::try_new::<Env>(
                    config(Positive::from_millis::<25>()).expect("valid redis configuration"),
                    server.addr(),
                    init,
                )
                .expect("topology fixture completed");
                let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
                let mut report = Completion::new();
                prepared.with_local(
                    |worker| {
                        worker.lend(|node, stage| {
                            node.install(stage, |host, stage| {
                                stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                                    let redis = runtime.root_client();
                                    let first = runtime.block_on(redis.wait_active())
                                        .expect("drive rejected connection")
                                        .expect_err("initialization must fail");
                                    runtime.block_on(yield_turns(2)).expect("yield rejected connection");
                                    {
                                        let second = runtime.block_on(redis.wait_active())
                                            .expect("drive repeated failure")
                                            .expect_err("rejection must persist");
                                        match &second {
                                            cartel_redis::Error::Handshake(error) => assert!(
                                                error.step == expected && error.kind == init::Kind::Rejected
                                                    && matches!(&error.frame, Ok(frame) if frame.as_ref() == REJECTED),
                                                "repeated initialization failure: {error:?}; frame: {:?}",
                                                error.frame.as_ref().map(|frame| frame.as_ref())
                                            ),
                                            error => assert!(
                                                matches!(error, cartel_redis::Error::Handshake(_)),
                                                "unexpected initialization failure: {:?}",
                                                std::mem::discriminant(error)
                                            ),
                                        }
                                    }
                                    assert!(matches!(&first, cartel_redis::Error::Handshake(error)
                                        if error.step == expected && error.kind == init::Kind::Rejected
                                            && matches!(&error.frame, Ok(frame) if frame.as_ref() == REJECTED)));
                                    seen.send(()).expect("report observed failure");
                                })
                            })
                        })
                    },
                    recovery::Policy::new(|event| reports.push(event), recover),
                    |result| assert!(matches!(result, Ok(Ok(Ok(())))), "Redis scope"),
                );
                reports.events().expect("valid reclaim observations");
                let rejected = server
                    .finish()
                    .expect("finish rejection peer")
                    .expect("rejection peer I/O")
                    .expect("rejected stage was not reached");
                assert_eq!(rejected, expected);
            },
        )
        .expect("spawn Redis rejection peer");
    }
}

#[test]
fn lane_failure_survives() {
    for invalid in [false, true] {
        let (allow, gate) = mpsc::channel();
        let (sent, recv) = mpsc::channel();
        let (close, stop) = mpsc::channel();
        AcceptScript::with(
        move |peer| {
            let bad = peer.accept()?;
            let good = peer.accept()?;
            bad.read_wait(Some(Duration::from_secs(1)))?;
            good.read_wait(Some(Duration::from_secs(1)))?;
            let mut buf = [0_u8; 128];
            assert_ne!(bad.read(&mut buf)?, 0);
            assert_ne!(good.read(&mut buf)?, 0);
            bad.write_all(DENIED)?;
            (gate.recv_timeout(GUARD)).expect("allow healthy AUTH");
            good.write_all(b"+OK\r\n")?;
            (sent.send(())).expect("healthy AUTH sent");
            assert_ne!(good.read(&mut buf)?, 0);
            good.write_all(b"+PONG\r\n")?;
            (stop.recv_timeout(GUARD)).expect("close healthy connection");
            let mut ping = [0; PING.len()];
            good.read_exact(&mut ping)?;
            assert_eq!(&ping, PING, "the closing lane owns the pending request");
            if invalid {
                good.write_all(b"?\r\n")?;
            } else {
                good.shutdown(Shutdown::Write)?;
            }
            Ok::<_, peer::Error>(())
        },
        |server| {
            let addr = server.addr();

            let config = Builder::new(settings::Capacities {
                connections: 2,
                admission_waiters: 2,
                inflight: 2,
                request_frame_bytes: 128,
                response_bytes: 256,
                response_values: 4,
                response_frame_bytes: 128,
                response_frame_values: 2,
            })
            .window(Positive::from_millis::<25>())
            .build();
            let config = config.expect("valid redis configuration");
            let init = (Plan::new().password("secret")).expect("authentication storage must allocate");
            {
                let prepared =
                    topology::try_new::<Env>(config, addr, init).expect("topology fixture completed");
                let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
                let mut report = Completion::new();
                prepared.with_local(
                    |worker| {
                        worker.lend(|node, stage| {
                            node.install(stage, |host, stage| {
                                stage.with_quiesce(&mut report, host, GUARD, |mut rt| {
                                    let redis = rt.root_client();

                                    let first = BlockOn::block_on(&mut rt, redis.wait_active())
                                        .expect("drive rejected lane")
                                        .expect_err("authentication must fail");

                                    assert!(allow.send(()).is_ok(), "allow healthy connection");
                                    assert!(recv.recv_timeout(GUARD).is_ok(), "healthy AUTH response");
                                    assert!(BlockOn::block_on(&mut rt, fibers::Yield::new()).is_ok());
                                    assert!(BlockOn::block_on(&mut rt, fibers::Yield::new()).is_ok());
                                    assert!(matches!(
                                        BlockOn::block_on(&mut rt, redis.wait_active()),
                                        Ok(Ok(()))
                                    ));
                                    assert!(matches!(
                                        BlockOn::block_on(&mut rt, redis.server().ping()),
                                        Ok(Ok(()))
                                    ));

                                    assert!(close.send(()).is_ok(), "close healthy connection");
                                    let closed = BlockOn::block_on(&mut rt, redis.server().ping());
                                    let closed = closed.expect("drive closed connection request");
                                    if invalid {
                                        assert!(matches!(closed, Err(value::reply::Failure::Control(cartel_redis::Error::Protocol(_)))));
                                    } else {
                                        assert!(matches!(closed, Err(value::reply::Failure::Control(cartel_redis::Error::Closed(
                                            close::Reason::Remote | close::Reason::Transport
                                        )))));
                                    }
                                    {
                                        let second = BlockOn::block_on(&mut rt, TEST.poll_once(redis.wait_active()))
                                            .expect("poll rejected lane")
                                            .expect("rejection must be ready")
                                            .expect_err("authentication failure must persist");
                                        assert!(matches!(&second, cartel_redis::Error::Handshake(error)
                                            if error.step == Step::Authenticate && error.kind == init::Kind::Rejected
                                                && matches!(&error.frame, Ok(frame) if frame.as_ref() == DENIED)));
                                    }
                                    assert!(matches!(&first, cartel_redis::Error::Handshake(error)
                                        if error.step == Step::Authenticate && error.kind == init::Kind::Rejected
                                            && matches!(&error.frame, Ok(frame) if frame.as_ref() == DENIED)));
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
                "finish Redis lanes; Redis lanes I/O"
            );
        },
    )
    .expect("spawn Redis lanes");
    }
}
