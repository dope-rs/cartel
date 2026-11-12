use cartel_pg::api::traits::{Client as _, Status as _};
use cartel_pg::{
    api::{Client, traits},
    port,
    protocol::{Reason, Wire},
    schema::array::Element as _,
};
use common::port::Reply;
use common::topology::{self, with_row};
use common::wire::checked::{ArrayQuery, Database, MixedQuery, MixedRow, TextQuery, TextRow};
use dope::core::driver::life::recovery;
use dope::{
    core::driver::route::Id, manifold::app::env::Bundle, manifold::timing::Throughput,
    net::tcp::Tcp, net::wire::Identity, runtime::executor::session::Completion,
};
use dope_fiber::BlockOn as _;
use dope_test::alloc::Tracker;
use dope_test::{harness::GUARD, harness::peer::TcpScript, reclaim::Reports};
use std::time::Duration;

pub mod common;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn retains_source() {
    let bytes = TextRow::wire(i64::MIN, "여름".as_bytes());
    with_row::<TextQuery>(&bytes, |row| {
        let ptr = row.decode().expect("decoded row").name.as_ptr();
        let (row, count) = Tracker::count(|| row.validate());
        let row = row.expect("validated row");
        let (view, read) = Tracker::count(|| row.view());
        assert_eq!((count, read), (0, 0));
        assert_eq!(view.lease, i64::MIN);
        assert_eq!(view.name, "여름");
        assert_eq!(view.name.as_ptr(), ptr);
    });
}

#[test]
fn collects_multiple_rows() {
    let first = MixedRow::wire(3, 4, 0, b"\x01[]", 5);
    let second = MixedRow::wire(7, 8, 1, b"\x01{}", 9);
    let value = ArrayQuery::wire(&[b"value"], &[]);
    let empty = ArrayQuery::wire(&[], &[]);
    let mut trailing = ArrayQuery::NULL.to_vec();
    trailing.push(9);
    TcpScript::with(
        |socket| {
            Database::serve(
                socket,
                [
                    &[first.as_slice(), second.as_slice()][..],
                    &[value.as_slice()][..],
                    &[empty.as_slice()][..],
                    &[ArrayQuery::NULL][..],
                    &[trailing.as_slice()][..],
                ]
                .map(|rows| rows.iter().copied()),
            )
        },
        |server| {
            let login = cartel_pg::Config::new("user".into(), "password".into(), "database".into())
                .expect("database credentials");
            let mut caps = Database::config().expect("bounded PostgreSQL config").capacities();
            caps.responses.items = 2;
            caps.responses.bytes = 65536;
            let config = port::config::Config::new(caps).expect("two retained rows");
            let ready = topology::try_new::<_, Bundle<Tcp, Identity, Throughput>>(
                port::Port::<Database>::plan(login, config),
                server.addr(),
                Duration::from_millis(1),
                Duration::from_secs(1),
            )
            .expect("prepared PostgreSQL topology");
            let mut reports = Reports::<_>::new(ready.layout(), [Id::fixed::<0>()]);
            let mut report = Completion::new();
            ready
                .with_local(
                    |worker| {
                        worker.lend(|node, stage| {
                            node.install(stage, |host, stage| {
                                stage.with_quiesce(&mut report, host, GUARD, |mut app| {
                                    let client: Client<cartel_pg::api::Context<'_, '_, '_, '_, '_, '_, '_, _, Database>> =
                                        app.root_client();
                                    app.block_on(client.status().wait_ready())
                                        .expect("driver ready")
                                        .ok()
                                        .expect("PostgreSQL ready");
                                    let (rows, allocations) = Tracker::count(|| app
                                        .block_on(traits::Query::run_all::<MixedQuery>(
                                            client,
                                            (),
                                        ))
                                        .expect("driver query"));
                                    assert_eq!(allocations, 0);
                                    let rows = rows
                                        .map_err(|error| format!("{:?}", Reply::from(&error)))
                                        .expect("query rows");
                                    let (rows, count) = Tracker::count(|| rows.validate());
                                    let retained = rows.expect("validated rows");
                                    let mut rows = retained.iter();
                                    let (first, read) = Tracker::count(|| rows.next());
                                    let first = first.expect("first row");
                                    assert_eq!((count, read), (0, 0));
                                    assert_eq!(
                                        (
                                            first.low,
                                            first.middle,
                                            first.active,
                                            first.tags,
                                            first.high
                                        ),
                                        (3, 4, false, "[]", 5)
                                    );
                                    let second = rows.next().expect("second row");
                                    assert_eq!(
                                        (
                                            second.low,
                                            second.middle,
                                            second.active,
                                            second.tags,
                                            second.high
                                        ),
                                        (7, 8, true, "{}", 9)
                                    );
                                    assert!(rows.next().is_none());
                                    drop(retained);
                                    for expected in [
                                        Ok(Some((1, Some("value")))),
                                        Ok(Some((0, None))),
                                        Ok(None),
                                        Err(()),
                                    ] {
                                        let row = app
                                            .block_on(traits::Query::run_scalar::<ArrayQuery<true>>(
                                                client,
                                                (),
                                            ))
                                            .expect("driver scalar")
                                            .map_err(|error| format!("{:?}", Reply::from(&error)))
                                            .expect("scalar query")
                                            .expect("scalar row");
                                        match expected {
                                            Ok(expected) => {
                                                let view = row.decode().expect("scalar decode");
                                                assert_eq!(
                                                    view.map(|array| array.len()),
                                                    expected.map(|value| value.0),
                                                );
                                                let owned = row.into_owned().expect("owned scalar");
                                                assert_eq!(
                                                    owned.as_ref().map(|array| (
                                                        array.len(),
                                                        array.first().map(String::as_str),
                                                    )),
                                                    expected,
                                                );
                                            }
                                            Err(()) => {
                                                for error in [row.decode().err(), row.into_owned().err()] {
                                                    assert!(matches!(
                                                        error,
                                                        Some(cartel_pg::Error::Protocol(Reason::RowTrailing { actual }))
                                                            if actual.get() == 1
                                                    ));
                                                }
                                            }
                                        }
                                    }
                                })
                            })
                        })
                    },
                    recovery::Policy::new(|event| reports.push(event), common::recover),
                    |outcome| outcome.map_err(common::entry_error).expect("application entry"),
                )
                .expect("scoped PostgreSQL")
                .expect("application scope");
            assert!(
                reports.events().expect("native reclamation").is_empty(),
                "unexpected native recovery"
            );
            server
                .finish()
                .expect("script execution")
                .expect("PostgreSQL script");
        },
    )
    .expect("script listener");
}

#[test]
fn owned_batch_failure() {
    let first = TextRow::wire(3, b"first");
    let mut second = TextRow::wire(7, b"second");
    second.push(9);
    TcpScript::with(
        |socket| Database::serve_batch(socket, [first.as_slice(), second.as_slice()]),
        |server| {
            let login = cartel_pg::Config::new("user".into(), "password".into(), "database".into())
                .expect("database credentials");
            let config = Database::config().expect("bounded PostgreSQL config");
            let ready = topology::try_new::<_, Bundle<Tcp, Identity, Throughput>>(
                port::Port::<Database>::plan(login, config),
                server.addr(),
                Duration::from_millis(1),
                Duration::from_secs(1),
            )
            .expect("prepared PostgreSQL topology");
            let mut reports = Reports::<_>::new(ready.layout(), [Id::fixed::<0>()]);
            let mut report = Completion::new();
            ready
                .with_local(
                    |worker| {
                        worker.lend(|node, stage| {
                            node.install(stage, |host, stage| {
                                stage.with_quiesce(&mut report, host, GUARD, |mut app| {
                                    let client: Client<
                                        cartel_pg::api::Context<
                                            '_,
                                            '_,
                                            '_,
                                            '_,
                                            '_,
                                            '_,
                                            '_,
                                            _,
                                            Database,
                                        >,
                                    > = app.root_client();
                                    app.block_on(client.status().wait_ready())
                                        .expect("driver ready")
                                        .ok()
                                        .expect("PostgreSQL ready");
                                    let rows = app
                                        .block_on(traits::Query::run_all::<TextQuery>(client, ()))
                                        .expect("driver query");
                                    let rows = rows.ok().expect("query rows");
                                    let (result, counts) = Tracker::measure(|| rows.into_owned());
                                    assert!(matches!(
                                        result,
                                        Err(cartel_pg::Error::Protocol(Reason::RowTrailing {
                                            actual
                                        })) if actual.get() == 1
                                    ));
                                    let bytes = 2 * size_of::<TextRow>() + 11;
                                    assert_eq!(counts, [3, bytes, 5, bytes + 80]);
                                })
                            })
                        })
                    },
                    recovery::Policy::new(|event| reports.push(event), common::recover),
                    |outcome| outcome.ok().expect("application entry"),
                )
                .expect("scoped PostgreSQL")
                .expect("application scope");
            assert!(
                reports.events().expect("native reclamation").is_empty(),
                "unexpected native recovery"
            );
            server
                .finish()
                .expect("script execution")
                .expect("PostgreSQL script");
        },
    )
    .expect("script listener");
}

#[test]
fn rejects_invalid_utf8() {
    with_row::<TextQuery>(&TextRow::wire(3, &[0xff]), |row| {
        assert!(matches!(
            row.validate(),
            Err(cartel_pg::Error::Protocol(Reason::TextUtf8 {
                column: 1,
                ..
            }))
        ));
    });
}

#[test]
fn rejects_scalar_width() {
    let mut bytes = TextRow::wire(3, b"name");
    bytes[2..6].copy_from_slice(&7i32.to_be_bytes());
    with_row::<TextQuery>(&bytes, |row| {
        assert!(matches!(
            row.validate(),
            Err(cartel_pg::Error::Protocol(Reason::ColumnWidth {
                column: 0,
                ..
            }))
        ));
    });
}

#[test]
fn rejects_null_text() {
    let mut bytes = TextRow::wire(3, b"");
    bytes[14..18].copy_from_slice(&(-1i32).to_be_bytes());
    with_row::<TextQuery>(&bytes, |row| {
        assert!(matches!(
            row.validate(),
            Err(cartel_pg::Error::Protocol(Reason::ColumnNull { column: 1 }))
        ));
    });
}

#[test]
fn rejects_row_shape() {
    let mut bytes = TextRow::wire(3, b"name");
    bytes[..2].copy_from_slice(&1u16.to_be_bytes());
    with_row::<TextQuery>(&bytes, |row| {
        assert!(matches!(
            row.validate(),
            Err(cartel_pg::Error::Protocol(Reason::ColumnCount {
                actual: 1,
                expected: 2
            }))
        ));
    });
    bytes[..2].copy_from_slice(&2u16.to_be_bytes());
    bytes.push(0);
    with_row::<TextQuery>(&bytes, |row| {
        assert!(matches!(
            row.validate(),
            Err(cartel_pg::Error::Protocol(Reason::RowTrailing { .. }))
        ));
    });
}

#[test]
fn rejects_truncated_text() {
    let mut bytes = TextRow::wire(3, b"name");
    bytes.pop();
    with_row::<TextQuery>(&bytes, |row| {
        assert!(matches!(
            row.validate(),
            Err(cartel_pg::Error::Protocol(Reason::ColumnBytes {
                column: 1,
                ..
            }))
        ));
    });
}

#[test]
fn retains_mixed_widths() {
    let mut bytes = MixedRow::wire(i64::MIN, i32::MIN, 1, "\x01[\"여름\"]".as_bytes(), i64::MAX);
    with_row::<MixedQuery>(&bytes, |row| {
        let ptr = row.decode().expect("decoded row").tags.as_ptr();
        let (row, count) = Tracker::count(|| row.validate());
        let row = row.expect("mixed scalar fields");
        let (view, read) = Tracker::count(|| row.view());
        assert_eq!((count, read), (0, 0));
        assert_eq!(
            (view.low, view.middle, view.active, view.tags, view.high),
            (i64::MIN, i32::MIN, true, "[\"여름\"]", i64::MAX)
        );
        assert_eq!(view.tags.as_ptr(), ptr);
    });
    bytes[14..18].copy_from_slice(&3i32.to_be_bytes());
    with_row::<MixedQuery>(&bytes, |row| {
        assert!(matches!(
            row.validate(),
            Err(cartel_pg::Error::Protocol(Reason::ColumnWidth {
                column: 1,
                ..
            }))
        ));
    });
}

#[test]
fn rejects_invalid_mixed_fields() {
    let bytes = MixedRow::wire(3, 4, 2, b"\x01[]", 5);
    with_row::<MixedQuery>(&bytes, |row| {
        assert!(matches!(
            row.validate(),
            Err(cartel_pg::Error::Protocol(Reason::Bool {
                column: 2,
                actual: 2
            }))
        ));
    });
    let bytes = MixedRow::wire(3, 4, 1, b"\x02[]", 5);
    with_row::<MixedQuery>(&bytes, |row| {
        assert!(matches!(
            row.validate(),
            Err(cartel_pg::Error::Protocol(Reason::JsonVersion {
                column: 3,
                actual: 2
            }))
        ));
    });
    let bytes = MixedRow::wire(3, 4, 1, b"\x01[\xff]", 5);
    with_row::<MixedQuery>(&bytes, |row| {
        assert!(matches!(
            row.validate(),
            Err(cartel_pg::Error::Protocol(Reason::JsonUtf8 { column: 3, valid, error }))
                if valid.get() == 1 && error == cartel_pg::protocol::Utf8::One
        ));
    });
}

#[test]
fn owned_json_uses_one_allocation() {
    let bytes = MixedRow::wire(3, 4, 1, b"\x01[]", 5);
    for allowed in 0..=1 {
        with_row::<MixedQuery>(&bytes, |row| {
            let (result, counts) =
                Tracker::measure(|| Tracker::fail_after(allowed, || row.into_owned()));
            if allowed == 0 {
                assert!(matches!(
                    result,
                    Err(cartel_pg::Error::Allocation(o3::alloc::Error::Exhausted))
                ));
                assert_eq!(&counts[..2], &[0, 0]);
            } else {
                let row = result.expect("owned JSON row");
                assert_eq!(row.tags.as_ref(), "[]");
                assert_eq!(&counts[..2], &[1, 2]);
                let ((), counts) = Tracker::measure(|| drop(row));
                assert_eq!(counts, [0, 0, 1, 2]);
            }
        });
    }
}

#[test]
fn array_keeps_tail() {
    let mut bytes = Wire::try_new(&[
        0, 0, 0, 4, 255, 255, 255, 255, 0, 0, 0, 8, 128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0xe2,
        0x82, 0xac, 9,
    ])
    .expect("bounded array");
    assert_eq!(i32::__read(&mut bytes).expect("array i32"), -1);
    assert_eq!(i64::__read(&mut bytes).expect("array i64"), i64::MIN);
    assert_eq!(<&str>::__read(&mut bytes).expect("array text"), "€");
    assert_eq!(bytes.as_slice(), &[9]);
}

#[test]
fn array_error_progress() {
    let mut bytes = Wire::try_new(&[0, 0, 0]).expect("bounded array");
    assert!(matches!(
        i32::__read(&mut bytes),
        Err(cartel_pg::Error::Protocol(Reason::ArrayLength { .. }))
    ));
    assert_eq!(bytes.as_slice(), &[0, 0, 0]);
    let mut bytes = Wire::try_new(&[0, 0, 0, 4, 1, 2]).expect("bounded array");
    assert!(matches!(
        i32::__read(&mut bytes),
        Err(cartel_pg::Error::Protocol(Reason::ArrayBytes { .. }))
    ));
    assert_eq!(bytes.as_slice(), &[1, 2]);
    let mut bytes = Wire::try_new(&[0x7f, 0xff, 0xff, 0xff, 1, 2]).expect("bounded array");
    assert!(matches!(
        i32::__read(&mut bytes),
        Err(cartel_pg::Error::Protocol(Reason::ArrayBytes { actual, required }))
            if actual.get() == 2 && required.get() == i32::MAX as u32
    ));
    assert_eq!(bytes.as_slice(), &[1, 2]);
    let mut bytes = Wire::try_new(&[0, 0, 0, 3, 1, 2, 3, 9]).expect("bounded array");
    assert!(matches!(
        i32::__read(&mut bytes),
        Err(cartel_pg::Error::Protocol(Reason::ArrayI32Width { .. }))
    ));
    assert_eq!(bytes.as_slice(), &[9]);
    let mut bytes = Wire::try_new(&[0, 0, 0, 4, 1, 2, 3, 4, 9]).expect("bounded array");
    assert!(matches!(
        i64::__read(&mut bytes),
        Err(cartel_pg::Error::Protocol(Reason::ArrayI64Width { .. }))
    ));
    assert_eq!(bytes.as_slice(), &[9]);
    let mut bytes = Wire::try_new(&[0, 0, 0, 2, b'a', 0xff, 9]).expect("bounded array");
    assert!(matches!(
        <&str>::__read(&mut bytes),
        Err(cartel_pg::Error::Protocol(Reason::ArrayTextUtf8 { valid, error }))
            if valid.get() == 1 && error == cartel_pg::protocol::Utf8::One
    ));
    assert_eq!(bytes.as_slice(), &[9]);
}

#[test]
fn array_collect_owns() {
    for (len, count, bytes) in [(0, 0, 0), (2, 3, 2 * size_of::<String>() + 11)] {
        let values: &[&[u8]] = &[b"first", b"second"];
        let input = ArrayQuery::wire(&values[..len], &[]);
        with_row::<ArrayQuery>(&input, |row| {
            let array = row.decode().expect("array row");
            let (owned, counts) =
                Tracker::measure(|| Tracker::fail_after(count, || array.try_collect()));
            let owned = owned.expect("owned array");
            assert_eq!(owned, &["first", "second"][..len]);
            assert_eq!(counts, [count, bytes, 0, 0]);
            let ((), counts) = Tracker::measure(|| drop(owned));
            assert_eq!(counts, [0, 0, count, bytes]);
        });
    }
}

#[test]
fn array_collect_fails() {
    use cartel_pg::Error::{Allocation, Protocol};
    let outer = 2 * size_of::<String>();
    type Case<'a, 'b, 'c> = (&'a [&'b [u8]], &'c [u8], usize, usize);
    let cases: [Case<'_, '_, '_>; 3] = [
        (&[], &[9], 0, 0),
        (&[b"first", b"second"], &[9], 3, outer + 11),
        (&[b"first", b"a\xff"], &[], 2, outer + 5),
    ];
    for (values, trailing, count, size) in cases {
        let bytes = ArrayQuery::wire(values, trailing);
        with_row::<ArrayQuery>(&bytes, |row| {
            let array = row.decode().expect("array row");
            if count == 3 {
                for (allows, bytes) in [0, outer, outer + 5].into_iter().enumerate() {
                    let (result, counts) =
                        Tracker::measure(|| Tracker::fail_after(allows, || array.try_collect()));
                    assert!(matches!(
                        result,
                        Err(Allocation(o3::alloc::Error::Exhausted))
                    ));
                    assert_eq!(counts, [allows, bytes, allows, bytes]);
                }
            }
            let (result, counts) =
                Tracker::measure(|| Tracker::fail_after(count, || array.try_collect()));
            assert!(matches!(result, Err(Protocol(reason)) if match reason {
                Reason::ArrayTrailing { actual } => !trailing.is_empty() && actual.get() == 1,
                Reason::ArrayTextUtf8 { valid, error } => trailing.is_empty()
                    && valid.get() == 1 && error == cartel_pg::protocol::Utf8::One,
                _ => false,
            }));
            assert_eq!(counts, [count, size, count, size]);
        });
    }
}
