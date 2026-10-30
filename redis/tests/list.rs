use cartel_redis::api::Client as _;
use cartel_redis::protocol::parser::storage;
use cartel_redis::value::source::{
    Bulks as _, Bytes as _, Chunks as _, Exact as _, Optional as _, Pairs as _,
};
use dope::core::driver::life::recovery;
use dope::runtime::executor::session::Completion;
use std::{
    convert,
    io::{Read as _, Write as _},
    mem,
    time::Duration,
};

use cartel_redis::{
    client::{nonempty::Slice, settings::init::Plan},
    value,
    value::{
        list::{bulks, pair},
        reply,
    },
};
use common::{command::list::*, recover, topology};
use dope::{
    core::driver::route::Id,
    manifold::{app::env::Bundle, timing::Throughput},
    net::{tcp::Tcp, wire::Identity},
};
use dope_fiber::BlockOn as _;
use dope_test::{
    harness::{GUARD, peer::TcpScript},
    reclaim::Reports,
};

type Failure<'b, 'c, 'd> = reply::Failure<cartel_redis::Error<'d>, value::Bytes<'b, 'c, 'd>>;

type Env = Bundle<Tcp, Identity, Throughput>;

pub mod common;

fn run(case: List) {
    let request = case.request();
    let reply = case.reply();
    TcpScript::with(
        move |socket| {
            socket
                .read_wait(Some(Duration::from_secs(1)))
                .expect("read timeout");
            let mut received = vec![0_u8; request.len()];
            socket.read_exact(&mut received).expect("Redis request");
            assert_eq!(received, request);
            socket.write_all(reply).expect("Redis response");
        },
        |server| {
            let address = server.addr();

            {
                let prepared = topology::try_new::<Env>(
                    try_config(1).expect("list configuration"),
                    address,
                    Plan::new(),
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

                                    assert!(matches!(
                                        runtime.block_on(redis.wait_active()),
                                        Ok(Ok(()))
                                    ));
                                    match case {
                                        List::Optional => {
                                            let fields = [b"a".as_slice(), b"b".as_slice()];
                                            let values = runtime
                                                .block_on(
                                                    redis
                                                        .hashes()
                                                        .values(b"hash", Slice::from_pair(&fields)),
                                                )
                                                .expect("optional operation")
                                                .ok()
                                                .expect("optional reply");
                                            assert!(matches!(
                                                values.try_array::<3>(),
                                                Err(reply::Decode::ArrayLength {
                                                    expected: 3,
                                                    actual: 2
                                                })
                                            ));
                                            assert_eq!(
                                                values
                                                    .try_array::<2>()
                                                    .ok()
                                                    .expect("optional pair"),
                                                [Some(b"one".as_slice()), None]
                                            );
                                        }
                                        List::Array
                                        | List::ArrayProof
                                        | List::ArrayEmpty
                                        | List::ArrayLate
                                        | List::ArrayVisit
                                        | List::ArrayVisitAt => {
                                            let result =
                                                runtime.block_on(redis.sets().members(b"set"));
                                            let result =
                                                result.expect("test operation must succeed");
                                            check_array_case(case, result);
                                        }
                                        List::Pairs
                                        | List::PairsOdd
                                        | List::PairsLate
                                        | List::PairsVisit => {
                                            let result =
                                                runtime.block_on(redis.hashes().entries(b"hash"));
                                            let result =
                                                result.expect("test operation must succeed");
                                            check_pairs_case(case, result);
                                        }
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
            server.finish().expect("finish Redis peer");
        },
    )
    .expect("spawn Redis peer");
}

fn check_array(result: Result<bulks::Array<'_, '_, '_, '_>, Failure<'_, '_, '_>>) {
    let values = result.ok().expect("scoped test operation must succeed");
    assert_eq!(values.len(), 3);
    assert!(!values.is_empty());
    let mut iter = values.iter();
    assert_eq!(iter.size_hint(), (3, Some(3)));
    assert_eq!(iter.next(), Some(&b"one"[..]));
    assert_eq!(iter.next(), Some(&b"two"[..]));
    assert_eq!(iter.next(), Some(&b"three"[..]));
    assert_eq!(iter.size_hint(), (0, Some(0)));
    assert_eq!(iter.next(), None);
    assert_eq!(iter.next(), None);
    let exact = values.try_exact::<3>().ok().expect("one triple");
    assert_eq!(
        exact
            .try_map::<_, convert::Infallible, convert::Infallible>(|[a, b, c]| {
                Ok(a.len() + b.len() + c.len())
            })
            .ok(),
        Some(11)
    );
}

fn check_array_proof(result: Result<bulks::Array<'_, '_, '_, '_>, Failure<'_, '_, '_>>) {
    let values = result.ok().expect("bulk reply");
    let rejected = values.try_chunks::<4>().err().expect("incomplete chunk");
    assert!(matches!(
        rejected.error(),
        storage::Length {
            width: 4,
            actual: 6
        }
    ));
    let (values, _) = rejected.into_parts();
    let rejected = values.try_exact::<2>().err().expect("wrong exact width");
    let (values, _) = rejected.into_parts();
    let chunks = values.try_chunks::<3>().ok().expect("complete triple");
    assert_eq!(chunks.len(), 2);
    let mut seen = 0;
    let error = chunks
        .try_visit::<_, convert::Infallible>(|[a, b, c]| {
            seen += 1;
            if seen == 1 {
                assert_eq!([a, b, c], [b"one".as_slice(), b"two", b"three"]);
                Ok(())
            } else {
                assert_eq!([a, b, c], [b"four".as_slice(), b"five", b"six"]);
                Err(bulks::Select::at::<1>(17))
            }
        })
        .err()
        .expect("selected member failure");
    assert_eq!(seen, 2);
    assert!(
        matches!(error, pair::VisitError::Value { value, error: 17 } if value.as_slice() == b"five")
    );
}

fn check_array_empty(result: Result<bulks::Array<'_, '_, '_, '_>, Failure<'_, '_, '_>>) {
    let chunks = result
        .ok()
        .expect("empty reply")
        .try_chunks::<5>()
        .ok()
        .expect("empty chunks");
    assert!(chunks.is_empty());
    let mut visited = false;
    let chunks = chunks
        .try_visit::<convert::Infallible, convert::Infallible>(|_| {
            visited = true;
            Ok(())
        })
        .ok()
        .expect("no chunk callbacks");
    assert!(!visited);
    let rejected = chunks
        .into_bulks()
        .try_exact::<5>()
        .err()
        .expect("not exact");
    assert!(matches!(
        rejected.error(),
        storage::Length {
            width: 5,
            actual: 0
        }
    ));
}

fn check_array_late(result: Result<bulks::Array<'_, '_, '_, '_>, Failure<'_, '_, '_>>) {
    let error = result
        .err()
        .expect("late status was accepted as a bulk string");
    assert!(
        matches!(&error,
            value::reply::Failure::Decode(value::reply::Decode::Expected {
                expected: value::reply::Expectation::Bulk,
                actual: value::reply::Actual::Status(bytes),
            }) if bytes.as_slice() == b"last"
        ),
        "wrong late array failure"
    );
}

fn check_array_visit(result: Result<bulks::Array<'_, '_, '_, '_>, Failure<'_, '_, '_>>) {
    use std::ops::ControlFlow;

    let values = result.ok().expect("scoped test operation must succeed");
    let bulks::Rejected { value, error: () } = values
        .try_visit(|value| {
            if value == b"two" {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })
        .err()
        .expect("visitor failure was discarded");
    assert_eq!(value.as_slice(), b"two");
}

fn check_array_visit_at(result: Result<bulks::Array<'_, '_, '_, '_>, Failure<'_, '_, '_>>) {
    use std::ops::ControlFlow;

    let values = result.ok().expect("scoped test operation must succeed");
    let bulks::Rejected { value, error } = values
        .try_visit_at(|index, value| {
            if index == 2 {
                assert_eq!(value, b"three");
                ControlFlow::Break(index)
            } else {
                ControlFlow::Continue(())
            }
        })
        .err()
        .expect("indexed visitor failure was discarded");
    assert_eq!(error, 2);
    assert_eq!(value.as_slice(), b"three");
}

fn check_pairs(result: Result<pair::BulkPairs<'_, '_, '_, '_>, Failure<'_, '_, '_>>) {
    let values = result.ok().expect("scoped test operation must succeed");
    assert_eq!(values.len(), 2);
    assert!(!values.is_empty());
    let mut iter = values.iter();
    assert_eq!(iter.size_hint(), (2, Some(2)));
    assert_eq!(iter.next(), Some((&b"a"[..], &b"1"[..])));
    assert_eq!(iter.next(), Some((&b"b"[..], &b"2"[..])));
    assert_eq!(iter.size_hint(), (0, Some(0)));
    assert_eq!(iter.next(), None);
    assert_eq!(iter.next(), None);
}

fn check_pairs_odd(result: Result<pair::BulkPairs<'_, '_, '_, '_>, Failure<'_, '_, '_>>) {
    assert!(
        matches!(
            result,
            Err(value::reply::Failure::Decode(
                value::reply::Decode::ArrayMultiple {
                    width: 2,
                    actual: 3,
                }
            ))
        ),
        "an odd array was accepted as key/value pairs"
    );
}

fn check_pairs_late(result: Result<pair::BulkPairs<'_, '_, '_, '_>, Failure<'_, '_, '_>>) {
    let error = result
        .err()
        .expect("late status was accepted as a pair value");
    assert!(
        matches!(&error,
            value::reply::Failure::Decode(value::reply::Decode::Expected {
                expected: value::reply::Expectation::Bulk,
                actual: value::reply::Actual::Status(bytes),
            }) if bytes.as_slice() == b"last"
        ),
        "wrong late pair failure"
    );
}

fn check_pairs_visit(result: Result<pair::BulkPairs<'_, '_, '_, '_>, Failure<'_, '_, '_>>) {
    let values = result.ok().expect("scoped test operation must succeed");
    let result = values.try_visit::<(), convert::Infallible>(|field, value| {
        if field == b"b" {
            assert_eq!(value, b"2");
            Err(pair::Select::Second(()))
        } else {
            Ok(())
        }
    });
    assert!(
        matches!(result, Err(pair::VisitError::Value { value, error: () }) if value.as_slice() == b"2"),
        "pair visitor failure was discarded"
    );
}

#[test]
fn optional_retries_width() {
    run(List::Optional);
}

#[test]
fn array() {
    run(List::Array);
}

#[test]
fn array_keeps_failure() {
    run(List::ArrayProof);
}

#[test]
fn empty_chunks_keep_width() {
    run(List::ArrayEmpty);
}

#[test]
fn array_rejects_late_kind() {
    run(List::ArrayLate);
}

#[test]
fn array_retains_failure() {
    run(List::ArrayVisit);
}

#[test]
fn array_retains_index() {
    run(List::ArrayVisitAt);
}

#[test]
fn pairs() {
    run(List::Pairs);
}

#[test]
fn pairs_reject_odd() {
    run(List::PairsOdd);
}

#[test]
fn pairs_reject_late_kind() {
    run(List::PairsLate);
}

#[test]
fn pairs_retains_failure() {
    run(List::PairsVisit);
}

#[test]
fn bounded_layout() {
    let word = mem::size_of::<usize>();
    assert_eq!(
        mem::size_of::<bulks::Array<'static, 'static, 'static, 'static>>(),
        10 * word
    );
    assert_eq!(
        mem::size_of::<Option<bulks::Array<'static, 'static, 'static, 'static>>>(),
        mem::size_of::<bulks::Array<'static, 'static, 'static, 'static>>()
    );
    assert_eq!(
        mem::size_of::<bulks::Optional<'static, 'static, 'static, 'static>>(),
        10 * word
    );
    assert_eq!(
        mem::size_of::<Option<bulks::Optional<'static, 'static, 'static, 'static>>>(),
        mem::size_of::<bulks::Optional<'static, 'static, 'static, 'static>>()
    );
    assert_eq!(
        mem::size_of::<pair::BulkPairs<'static, 'static, 'static, 'static>>(),
        10 * word
    );
    assert_eq!(
        mem::size_of::<Option<pair::BulkPairs<'static, 'static, 'static, 'static>>>(),
        mem::size_of::<pair::BulkPairs<'static, 'static, 'static, 'static>>()
    );
    assert!(mem::size_of::<pair::Rejected<'static, 'static, 'static, 'static, u32>>() <= 9 * word);
    assert_eq!(
        mem::size_of::<Option<pair::Rejected<'static, 'static, 'static, 'static, u32>>>(),
        mem::size_of::<pair::Rejected<'static, 'static, 'static, 'static, u32>>()
    );
}

fn check_array_case<'s, 'b, 'c, 'd>(
    case: List,
    result: Result<bulks::Array<'s, 'b, 'c, 'd>, Failure<'b, 'c, 'd>>,
) {
    match case {
        List::Array => check_array(result),
        List::ArrayProof => check_array_proof(result),
        List::ArrayEmpty => check_array_empty(result),
        List::ArrayLate => check_array_late(result),
        List::ArrayVisit => check_array_visit(result),
        List::ArrayVisitAt => check_array_visit_at(result),
        _ => panic!("pair case entered array assertion"),
    }
}

fn check_pairs_case<'s, 'b, 'c, 'd>(
    case: List,
    result: Result<pair::BulkPairs<'s, 'b, 'c, 'd>, Failure<'b, 'c, 'd>>,
) {
    match case {
        List::Pairs => check_pairs(result),
        List::PairsOdd => check_pairs_odd(result),
        List::PairsLate => check_pairs_late(result),
        List::PairsVisit => check_pairs_visit(result),
        _ => panic!("array case entered pair assertion"),
    }
}
