use cartel_redis::api::Client as _;
use cartel_redis::value::source::Bytes as _;
use cartel_redis::{
    client::settings::init::Plan, value, value::list::pair::ScoredBulks, value::reply,
};
use common::{
    command::{list, scored::*},
    recover, topology,
};
use dope::core::driver::life::recovery;
use dope::runtime::executor::session::Completion;
use dope::{
    core::driver::route::Id, manifold::app::env::Bundle, manifold::timing::Throughput,
    net::tcp::Tcp, net::wire::Identity,
};
use dope_fiber::BlockOn as _;
use dope_test::{harness::GUARD, harness::peer::TcpScript, reclaim::Reports};
use std::{mem, time::Duration};
use value::source::{self, ScoredText as _};

type Failure<'b, 'c, 'd> = reply::Failure<cartel_redis::Error<'d>, value::Bytes<'b, 'c, 'd>>;

type Env = Bundle<Tcp, Identity, Throughput>;

pub mod common;

fn exercise(case: Scored) {
    let config = list::try_config(1).expect("valid redis configuration");
    let reply = case.reply();
    TcpScript::with(
        move |socket| {
            (socket.read_wait(Some(Duration::from_secs(1)))).expect("read timeout");
            let mut request = [0_u8; REQUEST.len()];
            (socket.read_exact(&mut request)).expect("ZREVRANGE request");
            assert_eq!(request, REQUEST);
            (socket.write_all(reply)).expect("ZREVRANGE response");
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
                                stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                                    let redis = runtime.root_client();

                                    assert!(matches!(
                                        runtime.block_on(redis.wait_active()),
                                        Ok(Ok(()))
                                    ));
                                    let result =
                                        runtime.block_on(query(redis)).expect("drive scored reply");
                                    check(case, result);
                                })
                            })
                        })
                    },
                    recovery::Policy::new(|event| reports.push(event), recover),
                    |result| assert!(matches!(result, Ok(Ok(Ok(())))), "Redis scope"),
                );
                reports.events().expect("valid reclaim observations");
            };
            assert!(server.finish().is_ok(), "finish Redis peer");
        },
    )
    .expect("spawn Redis peer");
}

fn check_valid<S: source::Scored, E>(result: Result<S, E>) {
    assert!(result.is_ok(), "valid scored reply");
    let values = result.ok().expect("valid scored reply");
    let mut iter = values.iter();
    assert_eq!(iter.size_hint(), (2, Some(2)));
    assert_eq!(iter.next(), Some((&b"alice"[..], 42.5)));
    assert_eq!(iter.size_hint(), (1, Some(1)));
    assert_eq!(iter.next(), Some((&b"bob"[..], -7.0)));
    assert_eq!(iter.size_hint(), (0, Some(0)));
    assert_eq!(iter.next(), None);
    assert_eq!(iter.size_hint(), (0, Some(0)));
    assert_eq!(iter.next(), None);
    assert_eq!(iter.size_hint(), (0, Some(0)));
    drop(iter);
    let text = values.try_text().ok().expect("valid member text");
    assert!(text.iter().eq([("alice", 42.5), ("bob", -7.0)]));
}

fn check_text<S: source::Scored, E>(result: Result<S, E>) {
    let values = result.ok().expect("valid binary scored reply");
    let before = values.iter().nth(1).expect("second member").0.as_ptr();
    let failed = values.try_text().err().expect("invalid member text");
    assert_eq!(failed.index, 1);
    assert_eq!(failed.error.valid_up_to(), 0);
    let mut iter = failed.value.iter();
    assert_eq!(iter.next(), Some((&b"alice"[..], 42.5)));
    let (member, score) = iter.next().expect("unchanged invalid member");
    assert_eq!((member, score), (&b"\xff"[..], -7.0));
    assert_eq!(member.as_ptr(), before);
}

fn check_odd(result: Result<ScoredBulks<'_, '_, '_, '_>, Failure<'_, '_, '_>>) {
    assert!(matches!(
        result,
        Err(value::reply::Failure::Decode(
            value::reply::Decode::ArrayMultiple {
                width: 2,
                actual: 3,
            }
        ))
    ));
}

fn check_member(result: Result<ScoredBulks<'_, '_, '_, '_>, Failure<'_, '_, '_>>) {
    let error = result.err().expect("status member was accepted");
    assert!(
        matches!(&error,
            value::reply::Failure::Decode(value::reply::Decode::Expected {
                expected: value::reply::Expectation::Bulk,
                actual: value::reply::Actual::Status(bytes),
            }) if bytes.as_slice() == b"alice"
        ),
        "wrong member failure"
    );
}

fn check_score(result: Result<ScoredBulks<'_, '_, '_, '_>, Failure<'_, '_, '_>>) {
    let error = result.err().expect("invalid score was accepted");
    assert_eq!(source::Error::kind(&error), reply::Kind::Number);
    assert!(
        matches!(&error,
            value::reply::Failure::Number(value::reply::Number::Float {
                value: bytes,
                cause: reply::Float::Parse {
                    error,
                    cause: Some(value::reply::Invalid::Byte {
                        offset: 0,
                        actual: b'x',
                    }),
                },
            }) if bytes.as_slice() == b"x.y"
                && *error == "x.y".parse::<f64>().expect_err("invalid float")
        ),
        "wrong score failure"
    );
}

fn check_score_encoding(result: Result<ScoredBulks<'_, '_, '_, '_>, Failure<'_, '_, '_>>) {
    assert!(matches!(
        result,
        Err(reply::Failure::Number(reply::Number::Float {
            value,
            cause: reply::Float::Invalid(reply::Invalid::Byte { offset: 1, actual: 0xff }),
        })) if value.as_slice() == b"+\xff"
    ));
}

#[test]
fn valid() {
    exercise(Scored::Valid);
}

#[test]
fn odd_cardinality() {
    exercise(Scored::Odd);
}

#[test]
fn wrong_member_kind() {
    exercise(Scored::Member);
}

#[test]
fn malformed_score() {
    exercise(Scored::Score);
}

#[test]
fn score_encoding_origin() {
    exercise(Scored::ScoreEncoding);
}

#[test]
fn invalid_text_keeps_owner() {
    exercise(Scored::Text);
}

#[test]
fn bounded_layout() {
    let word = mem::size_of::<usize>();
    assert_eq!(mem::size_of::<value::reply::Invalid>(), 2 * word);
    assert_eq!(
        mem::size_of::<ScoredBulks<'static, 'static, 'static, 'static>>(),
        10 * word
    );
    assert_eq!(
        mem::size_of::<Option<ScoredBulks<'static, 'static, 'static, 'static>>>(),
        mem::size_of::<ScoredBulks<'static, 'static, 'static, 'static>>()
    );
}

fn check<'s, 'b, 'c, 'd>(
    case: Scored,
    result: Result<ScoredBulks<'s, 'b, 'c, 'd>, Failure<'b, 'c, 'd>>,
) {
    match case {
        Scored::Valid => check_valid(result),
        Scored::Odd => check_odd(result),
        Scored::Member => check_member(result),
        Scored::Score => check_score(result),
        Scored::ScoreEncoding => check_score_encoding(result),
        Scored::Text => check_text(result),
    }
}
