use cartel_pg::{
    api::{
        self,
        traits::{Client as _, Query as _, Status as _},
    },
    port,
    protocol::{self, Codec, encode},
    reply,
};
use common::{
    port::Queries,
    postgres::Frame,
    topology,
    wire::{
        capacity,
        checked::{BadResultFormat, Bind, CountedBind, Database, OVERSIZED},
    },
};
use dope::{
    core::driver::{life::recovery, route::Id},
    manifold::{app::env::Bundle, client::session::codec::Parse, timing::Throughput},
    net::{tcp::Tcp, wire::Identity},
    runtime::executor::session::Completion,
};
use dope_fiber::BlockOn as _;
use dope_test::{
    harness::{GUARD, peer::TcpScript},
    reclaim::Reports,
};
use std::{cell::Cell, num::NonZero, time::Duration};

pub mod common;

fn request_capacity(failure: &reply::Error<'_>) -> (usize, usize) {
    match failure.as_driver() {
        Some(cartel_pg::Error::Request(encode::Error::Capacity { required, capacity })) => {
            (required.get(), *capacity)
        }
        _ => panic!("unexpected request failure {:?}", failure.kind()),
    }
}

#[test]
fn row_borrows_ingress() {
    let config = capacity::try_frame(32).expect("valid test capacity");
    let codec = Codec::new(&config);
    let ingress =
        (Frame::new(b'D', b"payload").try_ingress(4096)).expect("fixture ingress allocation");
    capacity::parse(
        &codec,
        &ingress,
        |frame| {
            frame.message(|message| {
                assert_eq!(message.payload.as_slice(), b"payload");
                assert_eq!(
                    message.payload.as_slice().as_ptr(),
                    ingress.as_slice()[5..].as_ptr()
                );
            })
        },
        |parsed| {
            assert!(
                matches!(parsed, Ok(Parse::Item { head: Ok(()), consumed }) if consumed.count().get() == 12)
            )
        },
    );
}

#[test]
fn header_rejects_oversize() {
    let config = capacity::try_frame(12).expect("valid test capacity");
    let codec = Codec::new(&config);
    let ingress =
        (Frame::new(b'D', b"12345678").try_ingress(4096)).expect("fixture ingress allocation");
    capacity::parse(
        &codec,
        &ingress,
        |frame| frame.message(|_| ()),
        |parsed| {
            assert!(matches!(
                parsed,
                Ok(Parse::Item {
                    head: Err(cartel_pg::Error::Protocol(protocol::Reason::FrameCapacity {
                        declared: 12, capacity,
                    })),
                    consumed,
                }) if capacity.get() == 12 && consumed.count().get() == 5
            ));
        },
    );
}

#[test]
fn collected_rows_stop_before_control_partial_or_invalid_frame() {
    let codec = Codec::new(&capacity::try_frame(12).expect("frame capacity"));
    let first = Frame::new(b'D', &[0, 1, 0xff, 0xff, 0xff, 0xff]).encode();
    let second = Frame::new(b'D', &[0, 1, 0, 0, 0, 0]).encode();
    for suffix in [
        &b"Z\0\0\0\x05I"[..],
        b"D\0",
        b"D\0\0\0\x0a\0",
        b"D\0\0\0\x03",
        b"D\0\0\0\x0c",
    ] {
        let wire = [first.as_slice(), second.as_slice(), suffix].concat();
        let ingress = o3::buffer::storage::Shared::try_copy(&wire).expect("row ingress");
        capacity::parse(
            &codec,
            &ingress,
            |frame| {
                frame.collected(|payload, rows| {
                    assert_eq!(payload.as_slice(), &wire[..22]);
                    assert_eq!(payload.as_slice().as_ptr(), ingress.as_slice().as_ptr());
                    rows.get()
                })
            },
            |parsed| {
                assert!(
                    matches!(parsed, Ok(Parse::Item { head: Ok(2), consumed }) if consumed.count().get() == 22)
                )
            },
        );
    }
}

#[test]
fn bind_prefix_exhaustion_is_exact() {
    let cases = [(8, 4, 2), (10, 1, 0), (12, 2, 1), (16, 2, 1), (17, 4, 0), (47, 4, 3)];
    for (frame_bytes, required, room) in cases {
        let mut capacity = Database::config().expect("database capacity").capacities();
        capacity.requests.frame_bytes = frame_bytes;
        let prepared = topology::try_new::<_, Bundle<Tcp, Identity, Throughput>>(
            port::Port::<Bind>::plan(
                Queries::try_config().expect("database credentials"),
                port::config::Config::new(capacity).expect("prefix capacity"),
            ),
            ([127, 0, 0, 1], 0).into(),
            Duration::from_millis(1),
            Duration::from_secs(1),
        )
        .expect("PostgreSQL topology");
        let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
        let mut report = Completion::new();
        prepared
            .with_local(
                |worker| {
                    worker.lend(|node, stage| {
                        node.install(stage, |host, stage| {
                            stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                                let client: api::Client<
                                    api::Context<'_, '_, '_, '_, '_, '_, '_, _, Bind>,
                                > = runtime.root_client();
                                let calls = Cell::new(0);
                                let ordinal = Cell::new(0);
                                for attempt in 1..=2 {
                                    let failure = if frame_bytes == 47 {
                                        // Bind and Execute fit; the final Sync length is short.
                                        let query = client.run_first::<CountedBind>((&calls, &ordinal));
                                        assert_eq!(ordinal.get(), attempt);
                                        runtime.block_on(query).expect("fixed request driver").err()
                                    } else {
                                        runtime
                                            .block_on(client.run_no_rows::<Bind>((7, "ok", &["x"])))
                                            .expect("prefix driver")
                                            .err()
                                    }
                                    .expect("undersized request frame");
                                    assert_eq!(request_capacity(&failure), (required, room));
                                }
                            })
                        })
                    })
                },
                recovery::Policy::new(|event| reports.push(event), common::recover),
                |outcome| outcome.map_err(common::entry_error).expect("application entry"),
            )
            .expect("PostgreSQL installation")
            .as_ref()
            .expect("PostgreSQL scope");
        assert!(reports.events().expect("native reclamation").is_empty());
    }
}

#[test]
fn bind_failures_recover() {
    TcpScript::with(Bind::serve, |server| {
        let config = Database::config()
            .and_then(|config| {
                config.with_groups(port::config::Groups {
                    active: NonZero::<usize>::MIN,
                    records: NonZero::<usize>::MIN,
                    bytes: NonZero::<usize>::MIN,
                })
            })
            .expect("one request frame and group");
        let prepared = topology::try_new::<_, Bundle<Tcp, Identity, Throughput>>(
            port::Port::<Bind>::plan(
                Queries::try_config().expect("database credentials"),
                config,
            ),
            server.addr(),
            Duration::from_millis(1),
            Duration::from_secs(1),
        )
        .expect("PostgreSQL topology");
        let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
        let mut report = Completion::new();
        prepared.with_local(
            |worker| {
                worker.lend(|node, stage| {
                    node.install(stage, |host, stage| {
                        stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                            let client: api::Client<
                                api::Context<'_, '_, '_, '_, '_, '_, '_, _, Bind>,
                            > = runtime.root_client();
                            let failure = runtime
                                .block_on(client.run_all::<BadResultFormat>(()))
                                .expect("request driver");
                            assert!(matches!(
                                failure,
                                Err(reply::Error::Driver(cartel_pg::Error::Request(
                                    encode::Error::ResultEncoding {
                                        index: 1,
                                        expected: 1,
                                        actual: 0,
                                    }
                                )))
                            ));
                            runtime
                                .block_on(client.status().wait_ready())
                                .expect("ready driver")
                                .as_ref()
                                .map_err(reply::Error::kind)
                                .expect("ready");
                            let calls = Cell::new(0);
                            let single = Cell::new(0);
                            drop(client.run_first::<CountedBind>((&calls, &single)));
                            assert_eq!(single.get(), 1);
                            let order = [const { Cell::new(0) }; 13];
                            let params = order.each_ref().map(|ordinal| (&calls, ordinal));
                            let failure = runtime
                                .block_on(client.run_group::<CountedBind, 13>(params))
                                .expect("group driver")
                                .err()
                                .expect("twelfth Execute exceeds the 512-byte frame");
                            let cause = failure.cause().expect("group cause");
                            assert_eq!(request_capacity(cause), (10, 6));
                            assert_eq!(
                                order.map(Cell::into_inner),
                                [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 0]
                            );
                            for (text, array, expected) in [
                                (OVERSIZED, &["x"][..], (512, 483)),
                                ("ok", &["kept", OVERSIZED][..], (512, 445)),
                                // Three bytes remain for the result count and its two-byte code.
                                (&OVERSIZED[..456], &[][..], (2, 1)),
                            ] {
                                let failure = runtime
                                    .block_on(client.run_no_rows::<Bind>((7, text, array)))
                                    .expect("bind driver")
                                    .expect_err("short request frame");
                                assert_eq!(request_capacity(&failure), expected);
                            }
                            runtime
                                .block_on(client.run_no_rows::<Bind>((7, "ok", &["x"])))
                                .expect("bind driver")
                                .as_ref()
                                .map_err(reply::Error::kind)
                                .expect("reused request frame");
                        })
                    })
                })
            },
            recovery::Policy::new(|event| reports.push(event), common::recover),
            |outcome| {
                outcome
                    .as_ref()
                    .map_err(|error| error.cause())
                    .expect("application entry")
                    .as_ref()
                    .expect("PostgreSQL installation")
                    .as_ref()
                    .expect("PostgreSQL scope");
            },
        );
        let wire = server
            .finish()
            .expect("script execution")
            .expect("bind peer");
        assert_eq!(wire, [Bind::BODY]);
        assert!(reports.events().expect("native reclamation").is_empty());
    })
    .expect("script listener");
}
