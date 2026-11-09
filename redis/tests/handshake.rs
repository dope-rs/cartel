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
use dope_fiber::{BlockOn, abi::Join};
use dope_test::harness::peer;
use dope_test::{
    fibers::{self, TEST},
    harness::{GUARD, peer::AcceptScript},
    reclaim::Reports,
};
use o3::base::time::Positive;

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
    AcceptScript::with(
        move |peer| {
            {
                let socket = peer.accept()?;
                assert_eq!(socket.read(&mut [0])?, 0, "AUTH needs a free request frame");
            }
            for epoch in 0..2 {
                let socket = peer.accept()?;
                for (request, step) in REQUESTS {
                    assert_eq!(
                        read_request(&socket, request.len())?,
                        request,
                        "{step:?}"
                    );
                    socket.write_all(b"+OK\r\n")?;
                }
                assert_eq!(read_request(&socket, PING.len())?, PING);
                socket.write_all(b"+PONG\r\n")?;
                if epoch == 1 {
                    socket.read_wait(Some(GUARD))?;
                    assert_eq!(socket.read(&mut [0])?, 0, "replacement connection EOF");
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
        AcceptScript::with(
            move |peer| {
                let socket = peer.accept()?;
                for (request, step) in REQUESTS {
                    assert_eq!(
                        read_request(&socket, request.len())?,
                        request,
                        "{step:?}"
                    );
                    if step == expected {
                        socket.write_all(REJECTED)?;
                        socket.read_wait(Some(GUARD))?;
                        assert_eq!(
                            socket.read(&mut [0])?,
                            0,
                            "a command followed rejected {expected:?}"
                        );
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
                                    runtime.block_on(dope_fiber::fiber!('_ => async move {
                                        fibers::Yield::new().await;
                                        fibers::Yield::new().await;
                                    })).expect("yield rejected connection");
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
    use dope::{
        core::{
            driver::route::{KeyTag, table::conn::Capacity},
            io::socket,
        },
        manifold::{app::receive, network::listener, service},
        net::wire::domain,
        runtime::topology::{self, lending::Stage},
    };
    use dope_fiber::net::server;

    for invalid in [false, true] {
        let bound = socket::Listener::bind(([127, 0, 0, 1], 0).into()).expect("peer listener");
        let addr = bound.local_addr();
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
        .build()
        .expect("valid redis configuration");
        let init = (Plan::new().password("secret")).expect("authentication storage must allocate");
        let snapshot = service::Snapshot::from_one(
            service::Revision::new(1),
            service::Endpoint::new(addr, addr),
        );
        let (_, node) = cartel_redis::client::plan::Plan::<0>::new(config)
            .assemble::<Env>(
                snapshot,
                settings::DEFAULT_BACKOFF,
                settings::DEFAULT_BACKOFF_MAXIMUM,
                init,
            )
            .expect("Redis plan")
            .into_parts();
        let peer = server::Plan::<Identity, KeyTag<1>>::new(Capacity::fixed::<2>(), Identity);
        let layout =
            listener::topology::Layout::<KeyTag<1>, receive::Owned>::new(Capacity::fixed::<2>());
        let prepared = topology::Plan::new((), (node, layout))
            .for_install::<Throughput>()
            .expect("topology fixture completed");
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
                                backlog: socket::Backlog::fixed::<2>(),
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
                                    common::connect::App::new(host, listener),
                                    GUARD,
                                    |mut rt| {
                                        let redis = rt.client(common::connect::App::connector);
                                        let mut bad = TEST
                                            .try_run(&mut rt, peer.accept())
                                            .expect("bad lane accept driver")
                                            .ok()
                                            .expect("bad lane connection");
                                        let mut good = TEST
                                            .try_run(&mut rt, peer.accept())
                                            .expect("healthy lane accept driver")
                                            .ok()
                                            .expect("healthy lane connection");
                                        TEST.try_run(
                                            &mut rt,
                                            common::connect::expect(
                                                &mut bad,
                                                b"*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n",
                                            ),
                                        )
                                        .expect("bad lane AUTH request");
                                        TEST.try_run(
                                            &mut rt,
                                            common::connect::expect(
                                                &mut good,
                                                b"*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n",
                                            ),
                                        )
                                        .expect("healthy lane AUTH request");
                                        assert!(matches!(
                                            TEST.try_run(&mut rt, bad.write_all(DENIED)),
                                            Ok(Ok(()))
                                        ));
                                        let first = TEST
                                            .try_run(&mut rt, redis.wait_active())
                                            .expect("drive rejected lane")
                                            .expect_err("authentication must fail");
                                        assert!(matches!(
                                            TEST.try_run(&mut rt, good.write_all(b"+OK\r\n")),
                                            Ok(Ok(()))
                                        ));
                                        assert!(
                                            BlockOn::block_on(&mut rt, fibers::Yield::new())
                                                .is_ok()
                                        );
                                        assert!(
                                            BlockOn::block_on(&mut rt, fibers::Yield::new())
                                                .is_ok()
                                        );
                                        let client = dope_fiber::fiber!('_ => async move {
                                            assert!(redis.wait_active().await.is_ok());
                                            assert!(redis.server().ping().await.is_ok());
                                            redis.server().ping().await
                                        });
                                        let server = dope_fiber::fiber!('_ => async move {
                                            common::connect::expect(&mut good, PING).await;
                                            assert!(good.write_all(b"+PONG\r\n").await.is_ok());
                                            common::connect::expect(&mut good, PING).await;
                                            if invalid {
                                                assert!(good.write_all(b"?\r\n").await.is_ok());
                                            }
                                        });
                                        let (closed, ()) = TEST
                                            .try_run(&mut rt, Join::new(client, server))
                                            .expect("drive healthy lane through close");
                                        if invalid {
                                            assert!(matches!(
                                                closed,
                                                Err(value::reply::Failure::Control(
                                                    cartel_redis::Error::Protocol(_)
                                                ))
                                            ));
                                        } else {
                                            assert!(matches!(
                                                closed,
                                                Err(value::reply::Failure::Control(
                                                    cartel_redis::Error::Closed(
                                                        close::Reason::Remote
                                                            | close::Reason::Transport
                                                    )
                                                ))
                                            ));
                                        }
                                        {
                                            let second = BlockOn::block_on(
                                                &mut rt,
                                                TEST.poll_once(redis.wait_active()),
                                            )
                                            .expect("poll rejected lane")
                                            .expect("rejection must be ready")
                                            .expect_err("authentication failure must persist");
                                            assert!(matches!(
                                                &second,
                                                cartel_redis::Error::Handshake(error)
                                                    if error.step == Step::Authenticate
                                                        && error.kind == init::Kind::Rejected
                                                        && matches!(&error.frame, Ok(frame)
                                                            if frame.as_ref() == DENIED)
                                            ));
                                        }
                                        assert!(matches!(
                                            &first,
                                            cartel_redis::Error::Handshake(error)
                                                if error.step == Step::Authenticate
                                                    && error.kind == init::Kind::Rejected
                                                    && matches!(&error.frame, Ok(frame)
                                                        if frame.as_ref() == DENIED)
                                        ));
                                    },
                                )
                            },
                        )
                        .expect("install peer")
                    })
                })
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            |result| assert!(matches!(result, Ok(Ok(Ok(())))), "Redis scope"),
        );
        reports.events().expect("valid reclaim observations");
    }
}
