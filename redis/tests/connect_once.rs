use cartel_redis::{
    api::Commands as _,
    client::{connect, settings::init},
};
use dope::{
    core::{
        driver::{
            life::recovery,
            route::{Id, KeyTag, table::conn::Capacity},
        },
        io::socket,
    },
    manifold::{
        app::receive,
        client::{
            attempt::{ConnectFault, StreamTarget},
            close,
        },
        network::listener,
        timing::Balanced,
    },
    net::{
        link::event::ConnectError,
        tcp::Tcp,
        wire::{Identity, domain},
    },
    runtime::{
        client::Pair,
        executor::session::Completion,
        topology::{self, lending::Stage},
    },
};
use dope_fiber::{abi::Join, net::server};
use dope_test::{fibers, harness::GUARD, reclaim::Reports};
use o3::base::time::Positive;

pub mod common;

#[test]
fn native_refusal_retains_exact_first_attempt_before_starting_second_source() {
    let config = common::session::config(Positive::from_secs::<1>()).expect("Redis config");
    let plan = connect::Plan::<0>::new(config, init::Plan::new()).expect("one connection");
    let second = connect::Plan::<1>::new(config, init::Plan::new()).expect("second source");
    let prepared = topology::Plan::new((), (plan, second))
        .for_install::<Balanced>()
        .expect("topology");
    let mut reports = Reports::new(prepared.layout(), [Id::fixed::<0>(), Id::fixed::<1>()]);
    let mut report = Completion::new();
    prepared.with_local(
        |worker| {
            worker.lend(|(plan, second), stage| {
                plan.try_with(stage, Stage::driver, |first, start, stage| {
                    second.try_with(stage, Stage::driver, |second, second_start, stage| {
                    stage.with_quiesce(&mut report, Pair::new(first, second), GUARD, |mut runtime| {
                        let target = StreamTarget::new(
                            ([127, 0, 0, 1], 0).into(),
                            socket::option::Set::EMPTY,
                        );
                        let (contact, _) = runtime.root_client();
                        let result = fibers::TEST
                            .try_run(&mut runtime, start.connect(contact, target))
                            .expect("first attempt must settle");
                        assert!(matches!(
                            result,
                            Err(connect::Error::Attempt {
                                cause: ConnectFault::Transport(ConnectError::Connect(cause)),
                                ..
                            }) if cause.is_refused()
                        ));
                        // No target is published for B when A fails.
                        drop(second_start);
                    })
                    }).expect("install second connector")
                })
                .expect("install connector")
            })
        },
        recovery::Policy::new(|event| reports.push(event), common::recover),
        |result| assert!(matches!(result, Ok(Ok(())))),
    );
    assert!(reports.events().expect("native reclamation").is_empty());
}

#[test]
fn initialization_is_terminal_and_success_keeps_connection_until_drop() {
    for auth_reply in [
        Some(b"+OK\r\n+OK\r\n".as_slice()),
        Some(b"+OK\r\n+OK".as_slice()),
        None,
        Some(b"-WRONGPASS invalid user\r\n".as_slice()),
        Some(b"+OK\r\n".as_slice()),
    ] {
        let bound = socket::Listener::bind(([127, 0, 0, 1], 0).into()).expect("peer listener");
        let addr = bound.local_addr();
        let config = common::session::config(Positive::from_secs::<1>()).expect("Redis config");
        let init = init::Plan::new()
            .user(b"user", b"secret")
            .expect("ACL auth")
            .select(3);
        let plan = connect::Plan::<0>::new(config, init).expect("one connection");
        let peer = server::Plan::<Identity, KeyTag<1>>::new(Capacity::fixed::<1>(), Identity);
        let layout =
            listener::topology::Layout::<KeyTag<1>, receive::Owned>::new(Capacity::fixed::<1>());
        let prepared = topology::Plan::new((), (plan, layout))
            .for_install::<Balanced>()
            .expect("topology");
        let mut reports = Reports::new(prepared.layout(), [Id::fixed::<0>(), Id::fixed::<1>()]);
        let mut report = Completion::new();
        prepared.with_local(
            |worker| worker.lend(|(plan, _), stage| {
                plan.try_with(stage, Stage::driver, |host, start, stage| {
                    let hash = stage.hash_state(listener::Domain::DEFAULT);
                    let port = peer.try_build(stage.driver().storage).expect("peer storage");
                    server::Listener::<KeyTag<1>, Tcp, Identity, _>::adopt(
                        &port,
                        server::Config {
                            direct_flights: 0.into(), bind: addr, backlog: socket::Backlog::fixed::<1>(),
                            stream: Default::default(), transport: Default::default(), egress: Default::default(),
                            bound, wire: domain::Local::new(Identity),
                        },
                        |cause| cause.eliminate(), hash, stage, Stage::driver,
                        |listener, stage| {
                            let peer = listener.handle();
                            stage.with_quiesce(&mut report, common::connect::App::new(host, listener), GUARD, |mut runtime| {
                                let contact = runtime.client(common::connect::App::connector);
                                let client = dope_fiber::fiber!('_ => async move {
                                    let target = StreamTarget::new(addr, socket::option::Set::EMPTY);
                                    let result = start.connect(contact, target).await;
                                    match auth_reply {
                                        None => assert!(matches!(result, Err(connect::Error::Ready(cartel_redis::Error::Closed(close::Reason::Remote))))),
                                        Some(reply) if reply[0] == b'-' => assert!(matches!(result,
                                            Err(connect::Error::Ready(cartel_redis::Error::Handshake(error)))
                                            if error.step == init::Step::Authenticate && error.kind == cartel_redis::init::Kind::Rejected
                                                && matches!(&error.frame, Ok(frame) if frame.as_ref() == b"-WRONGPASS invalid user\r\n")
                                        )),
                                        Some(reply) if reply != b"+OK\r\n" => assert!(matches!(result,
                                            Err(connect::Error::Ready(cartel_redis::Error::UnexpectedResponse))
                                        ), "buffered startup ACK must not authorize SELECT"),
                                        Some(_) => {
                                            let connected = result.ok().expect("AUTH and SELECT ready");
                                            let redis = connected.redis();
                                            assert!(redis.server().ping().await.is_ok());
                                            let value = redis.basic().get(b"key").await.ok().expect("GET reply").expect("present value");
                                            assert_eq!(value.as_ref(), b"foo");
                                        }
                                    }
                                });
                                let server = dope_fiber::fiber!('_ => async move {
                                    let mut io = peer.accept().await.ok().expect("accept first connection");
                                    common::connect::expect(&mut io, b"*3\r\n$4\r\nAUTH\r\n$4\r\nuser\r\n$6\r\nsecret\r\n").await;
                                    let Some(reply) = auth_reply else { return; };
                                    assert!(io.write_all(reply).await.is_ok());
                                    if reply == b"+OK\r\n" {
                                        common::connect::expect(&mut io, b"*2\r\n$6\r\nSELECT\r\n$1\r\n3\r\n").await;
                                        assert!(io.write_all(b"+OK\r\n").await.is_ok());
                                        common::connect::expect(&mut io, b"*1\r\n$4\r\nPING\r\n").await;
                                        assert!(io.write_all(b"+PONG\r\n").await.is_ok());
                                        common::connect::expect(&mut io, b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n").await;
                                        assert!(io.write_all(b"$3\r\nfoo\r\n").await.is_ok());
                                    }
                                    assert!(matches!(io.read().await, Ok(None)), "client must close owned attempt");
                                });
                                fibers::TEST.try_run(&mut runtime, Join::new(client, server)).expect("one-shot exchange");
                            })
                        },
                    ).expect("install peer")
                }).expect("install connector")
            }),
            recovery::Policy::new(|event| reports.push(event), common::recover),
            |result| assert!(matches!(result, Ok(Ok(())))),
        );
        assert!(reports.events().expect("native reclamation").is_empty());
    }
}
