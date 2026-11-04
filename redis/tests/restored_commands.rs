use cartel_redis::api::Client as _;
use cartel_redis::value::source::{
    Bulks as _, Bytes as _, Coordinates as _, Scan as _, Scored as _,
};
use dope::core::driver::life::recovery;
use std::{num::NonZero, time::Duration};

use cartel_redis::{
    client::{
        nonempty::Slice,
        ops::commands::{geo::Unit, strings::Operation},
        settings::init::Plan,
    },
    value::reply,
};
use common::{
    command::{list, restored::SCRIPT},
    recover, topology,
};
use dope::{
    core::driver::route::Id,
    manifold::{app::env::Bundle, timing::Throughput},
    net::{tcp::Tcp, wire::Identity},
    runtime::executor::session::Completion,
};
use dope_fiber::BlockOn as _;
use dope_test::{
    alloc::{Activity, Tracker},
    harness::{GUARD, peer::TcpScript},
    reclaim::Reports,
};

pub mod common;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn preserves_wire_and_geo() {
    TcpScript::with(|socket| {
            socket
                .read_wait(Some(Duration::from_secs(1)))
                .expect("read timeout");
            let mut received = [0; 128];
            for (request, reply) in SCRIPT {
                let received = &mut received[..request.len()];
                socket.read_exact(received).expect("Redis request");
                assert_eq!(received, request);
                socket.write_all(reply).expect("Redis response");
            }
        }, |server| {
        let prepared = topology::try_new::<Bundle<Tcp, Identity, Throughput>>(
            list::try_config(2).expect("Redis configuration"),
            server.addr(),
            Plan::new(),
        )
        .expect("Redis topology");
        let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
        let mut report = Completion::new();
        prepared.with_local(
            |worker| {
                worker.lend(|node, stage| {
                    node.install(stage, |host, stage| {
                        stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                            let redis = runtime.root_client();
                            assert!(matches!(runtime.block_on(redis.wait_active()), Ok(Ok(()))));
                            let values = [b"a".as_slice(), b"b".as_slice()];
                            let pair = Slice::from_pair(&values);
                            assert!(matches!(runtime.block_on(redis.basic().set_millis(
                                b"key", b"val", NonZero::new(1250).expect("positive TTL")
                            )), Ok(Ok(()))));
                            assert!(matches!(runtime.block_on(redis.basic().set_absent(b"key", b"val")), Ok(Ok(true))));
                            assert!(matches!(runtime.block_on(redis.basic().set_absent(b"key", b"val")), Ok(Ok(false))));
                            assert!(matches!(runtime.block_on(redis.basic().expire(
                                b"key", NonZero::new(60).expect("positive TTL")
                            )), Ok(Ok(true))));
                            assert!(matches!(runtime.block_on(redis.basic().incr_float(b"key", 1.25)), Ok(Ok(value)) if value == 2.5));
                            assert!(matches!(runtime.block_on(redis.sorted_sets().scored_range(b"board", 0, -1)),
                                Ok(Ok(values)) if values.iter().eq([(b"a".as_slice(), 2.5)])));
                            assert!(matches!(runtime.block_on(redis.sorted_sets().score_range(b"board", f64::NEG_INFINITY, f64::INFINITY)),
                                Ok(Ok(found)) if found.iter().eq(values)));
                            assert!(matches!(runtime.block_on(redis.lists().push_front(b"list", pair)), Ok(Ok(2))));
                            assert!(matches!(runtime.block_on(redis.lists().push_back(b"list", pair)), Ok(Ok(4))));
                            assert!(matches!(runtime.block_on(redis.lists().pop_front(b"list")),
                                Ok(Ok(Some(value))) if value.as_slice() == b"b"));
                            assert!(matches!(runtime.block_on(redis.lists().pop_back(b"list")), Ok(Ok(None))));
                            assert!(matches!(runtime.block_on(redis.lists().range(b"list", 0, -1)),
                                Ok(Ok(found)) if found.iter().eq(values)));
                            assert!(matches!(runtime.block_on(redis.lists().len(b"list")), Ok(Ok(2))));
                            assert!(matches!(runtime.block_on(redis.bits().count(b"bits")), Ok(Ok(7))));
                            assert!(matches!(runtime.block_on(redis.bits().count_range(b"bits", 0, -1)), Ok(Ok(7))));
                            assert!(matches!(runtime.block_on(redis.bits().set(b"bits", 9, true)), Ok(Ok(false))));
                            assert!(matches!(runtime.block_on(redis.bits().get(b"bits", 9)), Ok(Ok(true))));
                            assert!(matches!(runtime.block_on(redis.bits().position(b"bits", false)), Ok(Ok(-1))));
                            for op in [Operation::And, Operation::Or, Operation::Xor] {
                                assert!(matches!(runtime.block_on(redis.bits().combine(op, b"out", pair)), Ok(Ok(2))));
                            }
                            assert!(matches!(runtime.block_on(redis.bits().not(b"out", b"a")), Ok(Ok(1))));
                            assert!(matches!(runtime.block_on(redis.hyperloglog().add(b"hll", &values)), Ok(Ok(true))));
                            assert!(matches!(runtime.block_on(redis.hyperloglog().add(b"hll", &[])), Ok(Ok(false))));
                            assert!(matches!(runtime.block_on(redis.hyperloglog().count(pair)), Ok(Ok(2))));
                            assert!(matches!(runtime.block_on(redis.hyperloglog().merge(b"out", &values)), Ok(Ok(()))));
                            assert!(matches!(runtime.block_on(redis.hyperloglog().merge(b"out", &[])), Ok(Ok(()))));
                            assert!(matches!(runtime.block_on(redis.geo().add(b"map", 1.25, -2.5, b"a")), Ok(Ok(1))));
                            assert!(matches!(runtime.block_on(redis.geo().distance(b"map", b"a", b"b", None)), Ok(Ok(None))));
                            for unit in [Unit::Meters, Unit::Kilometers, Unit::Miles, Unit::Feet] {
                                assert!(matches!(runtime.block_on(redis.geo().distance(b"map", b"a", b"b", Some(unit))),
                                    Ok(Ok(Some(value))) if value == 4.25));
                            }
                            let coords = runtime.block_on(redis.geo().positions(b"map", &values))
                                .expect("coordinate operation").ok().expect("coordinate reply");
                            assert!(matches!(runtime.block_on(redis.geo().search(b"map", 1.25, -2.5, 3.5, Unit::Kilometers)),
                                Ok(Ok(found)) if found.iter().eq(values)));
                            let usage = Tracker::begin();
                            assert_eq!(coords.len(), 2);
                            assert!(!coords.is_empty());
                            let mut iter = coords.iter();
                            let first = iter.next().expect("first member").expect("known member");
                            assert_eq!((first.longitude, first.latitude), (1.25, -2.5));
                            assert!(matches!(iter.next(), Some(None)));
                            assert!(iter.next().is_none());
                            assert_eq!(usage.finish(), Activity::default());
                            drop(coords);
                            assert!(matches!(runtime.block_on(redis.geo().positions(b"map", &values[..1])),
                                Ok(Err(reply::Failure::Decode(reply::Decode::ArrayLength {
                                    expected: 2, actual: 1,
                                })))));
                            assert!(matches!(runtime.block_on(redis.geo().positions(b"map", &values[..1])),
                                Ok(Err(reply::Failure::Decode(reply::Decode::Expected {
                                    expected: reply::Expectation::ArrayOrNil,
                                    actual: reply::Actual::Status(value),
                                }))) if value.as_slice() == b"bad"));
                            let error = runtime.block_on(redis.geo().positions(b"map", &values))
                                .expect("invalid coordinate operation").err().expect("late malformed latitude");
                            assert!(matches!(runtime.block_on(redis.geo().positions(b"map", &[])),
                                Ok(Ok(found)) if found.is_empty() && found.len() == 0 && found.iter().next().is_none()));
                            assert!(matches!(error,
                                reply::Failure::Number(reply::Number::Float {
                                    value,
                                    cause: reply::Float::Parse {
                                        cause: Some(reply::Invalid::Byte { offset: 0, actual: b'x' }),
                                        ..
                                    },
                                }) if value.as_slice() == b"x.y"));
                            assert!(matches!(runtime.block_on(redis.keyspace().kind(b"key")),
                                Ok(Ok(value)) if value.as_slice() == b"string"));
                            assert!(matches!(runtime.block_on(redis.keyspace().kind(b"key")),
                                Ok(Err(reply::Failure::Decode(reply::Decode::Expected {
                                    expected: reply::Expectation::TypeName,
                                    actual: reply::Actual::Ok,
                                })))));
                            assert!(matches!(runtime.block_on(redis.keyspace().scan(
                                17, Some(b"a*"), NonZero::new(100)
                            )), Ok(Ok(_))));
                            let scan = runtime.block_on(redis.sorted_sets().scan(
                                b"board", 17, NonZero::new(100)
                            )).expect("ZSCAN operation").ok().expect("ZSCAN reply");
                            assert_eq!(scan.cursor(), 23);
                            assert!(scan.keys().iter().eq([
                                b"a".as_slice(), b"2.5", b"b", b"-1"
                            ]));
                            drop(scan);
                            assert!(matches!(runtime.block_on(redis.server().ping()), Ok(Ok(()))));
                            let error = runtime.block_on(redis.server().ping())
                                .expect("nested error operation").expect_err("nested server error");
                            assert!(matches!(runtime.block_on(redis.server().ping()), Ok(Ok(()))));
                            assert!(matches!(error, reply::Failure::Server(value)
                                if value.as_slice() == b"ERR first"));
                        })
                    })
                })
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            |result| assert!(matches!(result, Ok(Ok(Ok(())))), "Redis scope"),
        );
        reports.events().expect("valid reclaim observations");
        server.finish().expect("Redis peer completion");
        }).expect("Redis peer");
}
