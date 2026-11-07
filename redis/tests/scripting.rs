use cartel_redis::api::Client as _;
use cartel_redis::value::source::{Bytes as _, Reader as _, Reply as _};
use dope::core::driver::life::recovery;
use dope::runtime::executor::session::Completion;
use std::{iter, num::NonZero, time::Duration};

use cartel_redis::{
    client::{
        GeoCoord,
        nonempty::Slice,
        ops::scripts::{self, Invocation as _, Reserved as _},
        settings::init::Plan,
    },
    request, value,
};
use common::{
    command::{Changed, scripts::*},
    recover, topology,
};
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

type Env = Bundle<Tcp, Identity, Throughput>;

pub mod common;

#[test]
fn eval_owned() {
    const EXPECTED: &[u8] = b"*7\r\n$4\r\nEVAL\r\n$9\r\nreturn 17\r\n$1\r\n2\r\n$8\r\nevent:42\r\n$3\r\n\0\x01\x02\r\n$5\r\nfirst\r\n$6\r\nsecond\r\n";

    TcpScript::with(
        move |socket| {
            socket
                .read_wait(Some(Duration::from_secs(1)))
                .expect("read timeout");
            let mut got = vec![0_u8; EXPECTED.len()];
            socket.read_exact(&mut got).expect("EVAL request");
            assert_eq!(got, EXPECTED);
            socket.write_all(b":17\r\n").expect("integer response");
            socket.read_exact(&mut got).expect("custom EVAL request");
            socket
                .write_all(b"-ERR retained\r\n")
                .expect("raw error response");
            for reply in PAIR_REPLIES {
                socket.read_exact(&mut got).expect("pair EVAL request");
                assert_eq!(got, EXPECTED);
                socket.write_all(reply).expect("pair response");
            }
        },
        |server| {
            let addr = server.addr();

            {
                let prepared = topology::try_new::<Env>(
                    common::command::list::try_config(2).expect("test configuration"),
                    addr,
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

                                    let keys = vec![b"event:42".to_vec(), vec![0, 1, 2]];
                                    let args = vec![b"first".to_vec(), b"second".to_vec()];
                                    assert!(matches!(
                                        runtime.block_on(redis.scripts().eval::<i64, _, _>(
                                            b"return 17",
                                            &keys,
                                            &args
                                        ),),
                                        Ok(Ok(17))
                                    ));
                                    let reply = runtime
                                        .block_on(redis.scripts().eval::<Retain, _, _>(
                                            b"return 17",
                                            &keys,
                                            &args,
                                        ))
                                        .expect("custom decoder completed")
                                        .ok()
                                        .expect("custom decoder retained the error");
                                    let mut reader = reply.reader();
                                    assert!(matches!(
                                        reader.next_event(),
                                        Some(value::Event::Error(b"ERR retained"))
                                    ));
                                    assert!(reader.finish().is_ok());
                                    drop(reply);
                                    let (first, second) = runtime
                                        .block_on(redis.scripts().eval::<(
                                            value::Bytes<'_, '_, '_>,
                                            value::Bytes<'_, '_, '_>,
                                        ), _, _>(b"return 17", &keys, &args))
                                        .expect("pair decoder completed")
                                        .ok()
                                        .expect("two retained bulk owners");
                                    assert!(matches!(
                                        runtime.block_on(redis.scripts().eval::<GeoCoord, _, _>(
                                            b"return 17", &keys, &args
                                        )),
                                        Ok(Ok(value)) if value.longitude == 1.25 && value.latitude == -2.0
                                    ));
                                    assert!(matches!(
                                        runtime.block_on(redis.scripts().eval::<(
                                            value::Bytes<'_, '_, '_>,
                                            value::Bytes<'_, '_, '_>,
                                        ), _, _>(b"return 17", &keys, &args)),
                                        Ok(Err(value::reply::Failure::Decode(
                                            value::reply::Decode::Expected {
                                                expected: value::reply::Expectation::Bulk,
                                                actual: value::reply::Actual::Array { len: 3 },
                                            }
                                        )))
                                    ));
                                    assert_eq!(&*first, b"first");
                                    drop(first);
                                    assert!(matches!(
                                        runtime.block_on(redis.scripts().eval::<GeoCoord, _, _>(
                                            b"return 17", &keys, &args
                                        )),
                                        Ok(Err(value::reply::Failure::Decode(
                                            value::reply::Decode::Expected {
                                                expected: value::reply::Expectation::Float,
                                                actual: value::reply::Actual::Array { len: 3 },
                                            }
                                        )))
                                    ));
                                    assert_eq!(&*second, b"second");
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

#[test]
fn request_failure_recycles() {
    let unread = iter::repeat_with(|| panic!("length iterator advanced"));
    for (keys, arguments, expected) in PREFIX_ERRORS {
        assert_eq!(
            scripts::wire_len(b"", keys, unread.clone(), arguments, unread.clone()),
            Err(expected)
        );
    }
    for (count, lengths, expected) in KEY_LENGTH_ERRORS {
        assert_eq!(
            scripts::wire_len(b"", count, lengths.iter().copied(), 1, unread.clone()),
            Err(expected)
        );
    }
    const EXPECTED: &[u8] = b"*4\r\n$4\r\nEVAL\r\n$9\r\nreturn 17\r\n$1\r\n0\r\n$2\r\nok\r\n";
    const HASH: &[u8] = b"*4\r\n$4\r\nHSET\r\n$3\r\nkey\r\n$5\r\nfield\r\n$5\r\nvalue\r\n";
    const DELETE: &[u8] = b"*3\r\n$3\r\nDEL\r\n$3\r\none\r\n$3\r\ntwo\r\n";
    const ADD: &[u8] = b"*4\r\n$4\r\nSADD\r\n$3\r\nset\r\n$3\r\none\r\n$3\r\ntwo\r\n";
    TcpScript::with(
        move |socket| {
            let mut hash = [0; HASH.len()];
            socket.read_exact(&mut hash).expect("valid HSET request");
            assert_eq!(hash, HASH);
            socket.write_all(b":1\r\n").expect("HSET response");
            socket.read_exact(&mut hash).expect("fixed HSET request");
            assert_eq!(hash, HASH);
            socket.write_all(b":0\r\n").expect("HSET update response");
            let mut delete = [0; DELETE.len()];
            socket.read_exact(&mut delete).expect("valid DEL request");
            assert_eq!(delete, DELETE);
            socket.write_all(b":2\r\n").expect("DEL response");
            let mut add = [0; ADD.len()];
            socket.read_exact(&mut add).expect("valid SADD request");
            assert_eq!(add, ADD);
            socket.write_all(b":2\r\n").expect("SADD response");
            let mut ack = [0; ACK.len()];
            socket.read_exact(&mut ack).expect("valid XACK request");
            assert_eq!(ack, ACK);
            socket.write_all(b":2\r\n").expect("XACK response");
            let mut got = [0; EXPECTED.len()];
            for reply in EMPTY_REPLIES {
                socket.read_exact(&mut got).expect("empty EVAL request");
                assert_eq!(got, EXPECTED);
                socket.write_all(reply).expect("empty credit response");
            }
        },
        |server| {
            let addr = server.addr();

            {
                let prepared = topology::try_new::<Env>(
                    try_config().expect("test configuration"),
                    addr,
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
                                    let invalid = [Segmented {
                                        len: 1,
                                        chunks: [b"xx".as_slice()],
                                    }];
                                    assert!(
                                        matches!(
                                            runtime.block_on(
                                                redis.scripts().eval::<i64, &[u8], _>(
                                                    b"return 17",
                                                    &[],
                                                    &invalid,
                                                )
                                            ),
                                            Ok(Err(value::reply::Failure::Control(
                                                cartel_redis::Error::Request(
                                                    request::Error::BulkLength {
                                                        declared: 1,
                                                        actual: 2
                                                    }
                                                )
                                            )))
                                        ),
                                        "invalid bulk must fail before submission"
                                    );

                                    let fields = [(b"a".as_slice(), b"b".as_slice())];
                                    assert!(matches!(
                                        runtime.block_on(redis.hashes().set_iter(
                                            b"key",
                                            NonZero::new(2).expect("count"),
                                            fields.into_iter(),
                                        )),
                                        Ok(Err(value::reply::Failure::Control(
                                            cartel_redis::Error::Request(request::Error::Items {
                                                declared: 2,
                                                actual: 1,
                                            })
                                        )))
                                    ));
                                    assert!(matches!(
                                        runtime.block_on(redis.hashes().set_iter(
                                            b"key",
                                            NonZero::<usize>::MIN,
                                            Changed {
                                                measured: &[(b"abcdefgh", b"abcdefgh")],
                                                emitted: &[(b"a", b"b"), (b"a", b"b")],
                                            },
                                        )),
                                        Ok(Err(value::reply::Failure::Control(
                                            cartel_redis::Error::Request(request::Error::Items {
                                                declared: 1,
                                                actual: 2,
                                            })
                                        )))
                                    ));
                                    let hash = {
                                        let field = String::from("field");
                                        let value = String::from("value");
                                        redis.hashes().set_iter(
                                            b"key",
                                            NonZero::<usize>::MIN,
                                            [(field.as_bytes(), value.as_bytes())].into_iter(),
                                        )
                                    };
                                    assert!(matches!(runtime.block_on(hash), Ok(Ok(1))));
                                    assert!(matches!(
                                        runtime.block_on(
                                            redis.hashes().set(b"key", b"field", b"value")
                                        ),
                                        Ok(Ok(false))
                                    ));
                                    let delete = {
                                        let keys = [String::from("one"), String::from("two")];
                                        redis.basic().del_iter(
                                            NonZero::new(keys.len()).expect("count"),
                                            keys.iter().map(String::as_bytes),
                                        )
                                    };
                                    assert!(matches!(runtime.block_on(delete), Ok(Ok(2))));
                                    let add = {
                                        let key = String::from("set");
                                        let members = [String::from("one"), String::from("two")];
                                        redis.sets().add_iter(
                                            key.as_bytes(),
                                            NonZero::new(members.len()).expect("count"),
                                            members.iter().map(String::as_bytes),
                                        )
                                    };
                                    assert!(matches!(runtime.block_on(add), Ok(Ok(2))));

                                    let ack = {
                                        let key = String::from("key");
                                        let group = String::from("group");
                                        let ids = [String::from("1-0"), String::from("2-0")];
                                        redis.streams().ack(
                                            key.as_bytes(),
                                            group.as_bytes(),
                                            Slice::from_pair(&ids),
                                        )
                                    };
                                    assert!(matches!(runtime.block_on(ack), Ok(Ok(2))));
                                    let empty = runtime
                                        .block_on(
                                            redis
                                                .scripts()
                                                .eval::<value::Bytes<'_, '_, '_>, &[u8], _>(
                                                    b"return 17",
                                                    &[],
                                                    &[b"ok".as_slice()],
                                                ),
                                        )
                                        .expect("empty response completed")
                                        .ok()
                                        .expect("empty bulk");
                                    let clone = empty.try_clone().expect("empty clone");
                                    assert!(matches!(
                                        runtime.block_on(redis.scripts().eval::<i64, &[u8], _>(
                                            b"return 17",
                                            &[],
                                            &[b"ok".as_slice()],
                                        )),
                                        Ok(Ok(17))
                                    ));
                                    assert!(empty.is_empty() && clone.is_empty());
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

#[test]
fn bulk_boundary() {
    let call = Owned {
        args: [vec![b'x'; 300 * 1024]],
    };
    let need = call.wire_len().expect("test operation must succeed");
    assert!(need > 256 * 1024);

    for capacity in [need, need - 1] {
        TcpScript::with(
            move |socket| {
                if capacity == need {
                    let mut got = vec![0_u8; need];
                    socket.read_exact(&mut got).expect("bulk EVAL request");
                    assert!(
                        got[need - 2 - 300 * 1024..need - 2]
                            .iter()
                            .all(|byte| *byte == b'x')
                    );
                    socket.write_all(b":17\r\n").expect("integer response");
                } else {
                    let mut byte = [0_u8; 1];
                    assert_eq!(socket.read(&mut byte).expect("connection close"), 0);
                }
            },
            |server| {
                let addr = server.addr();
                {
                    let prepared = topology::try_new::<Env>(
                        try_bulk_config(capacity).expect("bulk configuration"),
                        addr,
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
                                        let request = if capacity == need {
                                            use common::session::deadline::release_after_poll;
                                            use dope_fiber::abi::Task;
                                            let prepared = call.prepare().expect("prepared EVAL");
                                            let held = redis.scripts().eval_prepared::<i64, _, _>(
                                                call.prepare().expect("held EVAL"),
                                            );
                                            let waiting = Box::pin(Task::new(
                                                redis
                                                    .scripts()
                                                    .try_reserve_bulk(prepared.wire_len())
                                                    .expect("bulk capacity"),
                                            ));
                                            let reserved = runtime
                                                .block_on(release_after_poll(waiting, held))
                                                .expect("reservation drive")
                                                .ok()
                                                .expect("command return wakes reservation");
                                            let waiting = Box::pin(Task::new(
                                                redis
                                                    .scripts()
                                                    .try_reserve_bulk(prepared.wire_len())
                                                    .expect("bulk capacity"),
                                            ));
                                            let reserved = runtime
                                                .block_on(release_after_poll(waiting, reserved))
                                                .expect("reservation drive")
                                                .ok()
                                                .expect("reservation return wakes reservation");
                                            reserved.eval_prepared::<i64, _, _>(prepared)
                                        } else {
                                            redis.scripts().eval_call::<i64, _>(&call)
                                        };
                                        let result = runtime.block_on(request);
                                        if capacity == need {
                                            assert!(matches!(result, Ok(Ok(17))));
                                        } else {
                                            assert!(
                                                matches!(
                                                    result,
                                                    Ok(Err(value::reply::Failure::Control(
                                                        cartel_redis::Error::Request(
                                                            request::Error::Capacity(cause)
                                                        )
                                                    ))) if {
                                                        let (actual, cap) = cause.into_parts();
                                                        actual.get() == need && cap == need - 1
                                                    }
                                                ),
                                                "oversized EVAL must fail before submission"
                                            );
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
}

#[test]
fn bulk_reservation_retains_reply() {
    use dope_fiber::abi::{PollFn, Task};
    use dope_fiber::context::PollResult;
    use std::task::Poll;

    for complete in [true, false] {
        let call = Owned {
            args: [vec![b'x'; 16 * 1024 * 1024 - 64]],
        };
        let need = NonZero::new(call.wire_len().expect("wire length")).expect("nonempty EVAL");
        TcpScript::with(
            move |peer| retained_bulk(peer, need, complete),
            |server| {
                let prepared = topology::try_new::<Env>(
                    try_retained_bulk_config(need.get()).expect("retained bulk configuration"),
                    server.addr(),
                    Plan::new(),
                )
                .expect("topology fixture");
                let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
                let mut report = Completion::new();
                prepared.with_local(
                    |worker| {
                        worker.lend(|node, stage| {
                            node.install(stage, |host, stage| {
                                stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                                    let redis = runtime.root_client();
                                    let held = runtime
                                        .block_on(redis.basic().get(b"held"))
                                        .expect("source drive")
                                        .ok()
                                        .expect("source reply")
                                        .expect("source bytes");
                                    let mut read = Box::pin(Task::new(redis.basic().get(b"next")));
                                    assert!(
                                        runtime
                                            .block_on(dope_test::fibers::TEST.poll_once(read.as_mut()))
                                            .expect("publish next read")
                                            .is_none()
                                    );
                                    dope_test::fibers::TEST
                                        .try_pause(&mut runtime, Duration::from_millis(20))
                                        .expect("retain blocked reply");
                                    let mut send =
                                        Box::pin(Task::new(redis.scripts().eval_call::<i64, _>(&call)));
                                    drop(call);
                                    let mut reserve = Box::pin(Task::new(
                                        redis
                                            .scripts()
                                            .try_reserve_bulk(need)
                                            .expect("bulk capacity"),
                                    ));
                                    let kept = &held;
                                    let mut first = true;
                                    let running = &mut send;
                                    let reserved = runtime
                                        .block_on(PollFn::admitted(move |admit| {
                                            let (next, mut cx) = admit.poll(read.as_mut());
                                            match next {
                                                Poll::Pending => {}
                                                Poll::Ready(Ok(_)) => {
                                                    panic!("read completed")
                                                }
                                                Poll::Ready(Err(error)) => panic!(
                                                    "read: {:?}",
                                                    value::source::Error::diagnostic(&error)
                                                ),
                                            }
                                            match cx.as_mut().try_poll(running.as_mut()) {
                                                PollResult::Exhausted => return Poll::Pending,
                                                PollResult::Polled(sent) => {
                                                    assert!(sent.is_pending());
                                                }
                                            }
                                            let result = match cx.try_poll(reserve.as_mut()) {
                                                PollResult::Exhausted => return Poll::Pending,
                                                PollResult::Polled(result) => result,
                                            };
                                            if std::mem::take(&mut first) {
                                                assert!(result.is_pending());
                                            }
                                            if result.is_ready() {
                                                assert_eq!(kept.as_ref(), &[b'x'; 41]);
                                            }
                                            result
                                        }))
                                        .expect("retained reservation drive")
                                        .ok()
                                        .expect("send returns bulk frame");
                                    drop(reserved);
                                    drop(held);
                                    let error = runtime.block_on(send.as_mut())
                                        .expect("queued EVAL drive").err().expect("old suffix rejected");
                                    assert!(
                                        matches!(&error, value::reply::Failure::Control(cartel_redis::Error::ResponseFront(cartel_core::reply::order::Front::Empty)) if complete)
                                        || matches!(&error, value::reply::Failure::Control(cartel_redis::Error::UnexpectedResponse) if !complete),
                                        "{:?}", value::source::Error::diagnostic(&error)
                                    );
                                })
                            })
                        })
                    },
                    recovery::Policy::new(|event| reports.push(event), recover),
                    |result| assert!(matches!(result, Ok(Ok(Ok(())))), "Redis scope"),
                );
                reports.events().expect("reclaim observations");
                server
                    .finish()
                    .expect("Redis peer")
                    .expect("retained bulk protocol");
            },
        )
        .expect("Redis listener");
    }
}

#[test]
fn partial_reply_keeps_published_cohort() {
    use dope_fiber::abi::Task;
    use dope_test::fibers;

    const REQUEST: &[u8] = b"*2\r\n$4\r\nINCR\r\n$3\r\nkey\r\n";
    TcpScript::with(
        |peer| {
            peer.read_wait(Some(GUARD))?;
            let mut request = [0; REQUEST.len()];
            for _ in 0..2 {
                peer.read_exact(&mut request)?;
                assert_eq!(request, REQUEST);
            }
            peer.write_all(b":1\r\n:2")?;
            peer.read_exact(&mut request)?;
            assert_eq!(request, REQUEST);
            peer.write_all(b"\r\n:3\r\n:4\r\n")?;
            common::session::deadline::wait_closed(peer)
        },
        |server| {
            let prepared = topology::try_new::<Env>(
                common::command::list::try_config(3).expect("cohort configuration"),
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
                                assert!(matches!(
                                    runtime.block_on(redis.wait_active()),
                                    Ok(Ok(()))
                                ));
                                let mut first = Box::pin(Task::new(redis.basic().incr(b"key")));
                                let mut second = Box::pin(Task::new(redis.basic().incr(b"key")));
                                assert!(
                                    runtime
                                        .block_on(fibers::TEST.poll_once(first.as_mut()))
                                        .expect("publish first request")
                                        .is_none()
                                );
                                assert!(
                                    runtime
                                        .block_on(fibers::TEST.poll_once(second.as_mut()))
                                        .expect("publish second request")
                                        .is_none()
                                );
                                assert!(matches!(runtime.block_on(first.as_mut()), Ok(Ok(1))));
                                let mut third = Box::pin(Task::new(redis.basic().incr(b"key")));
                                assert!(
                                    runtime
                                        .block_on(fibers::TEST.poll_once(third.as_mut()))
                                        .expect("publish later request")
                                        .is_none()
                                );
                                assert!(matches!(runtime.block_on(second.as_mut()), Ok(Ok(2))));
                                assert!(matches!(runtime.block_on(third.as_mut()), Ok(Ok(3))));
                                assert!(matches!(
                                    runtime.block_on(redis.basic().incr(b"key")),
                                    Ok(Err(value::reply::Failure::Control(
                                        cartel_redis::Error::ResponseFront(
                                            cartel_core::reply::order::Front::Empty
                                        )
                                    )))
                                ));
                            })
                        })
                    })
                },
                recovery::Policy::new(|event| reports.push(event), recover),
                |result| assert!(matches!(result, Ok(Ok(Ok(())))), "Redis scope"),
            );
            reports.events().expect("reclaim observations");
            server
                .finish()
                .expect("Redis peer")
                .expect("cohort protocol");
        },
    )
    .expect("Redis listener");
}

#[test]
fn split_is_one_bulk() {
    TcpScript::with(
        move |socket| {
            let mut got =
                [0; KEY_COUNTS[2].1.len() + KEY_COUNTS[2].0 * KEY_BULK.len() + SPLIT_BULK.len()];
            for (count, header) in KEY_COUNTS {
                let got = &mut got[..header.len() + count * KEY_BULK.len() + SPLIT_BULK.len()];
                assert_eq!(
                    scripts::wire_len(
                        SPLIT.script(),
                        count,
                        iter::repeat_n(1, count),
                        1,
                        iter::once(SPLIT.args[0].len),
                    ),
                    Ok(got.len())
                );
                socket.read_exact(got).expect("EVAL request");
                let (actual, rest) = got.split_at(header.len());
                assert_eq!(actual, header);
                let (keys, argument) = rest.split_at(count * KEY_BULK.len());
                assert!(keys.chunks_exact(KEY_BULK.len()).all(|key| key == KEY_BULK));
                assert_eq!(argument, SPLIT_BULK);
                socket.write_all(b":17\r\n").expect("integer response");
            }
            let lengths = [0, 9, 10, 99, 100].map(|len| {
                let expected = format!(
                    "*4\r\n$4\r\nEVAL\r\n$9\r\nreturn 17\r\n$1\r\n0\r\n${len}\r\n{}\r\n",
                    "x".repeat(len)
                );
                assert_eq!(
                    scripts::wire_len(SPLIT.script(), 0, iter::empty(), 1, iter::once(len)),
                    Ok(expected.len())
                );
                expected
            });
            let arity = format!(
                "*10\r\n$4\r\nEVAL\r\n$9\r\nreturn 17\r\n$1\r\n0\r\n{}",
                "$0\r\n\r\n".repeat(7)
            );
            assert_eq!(
                scripts::wire_len(SPLIT.script(), 0, iter::empty(), 7, iter::repeat_n(0, 7)),
                Ok(arity.len())
            );
            for expected in lengths
                .iter()
                .map(String::as_bytes)
                .chain(iter::once(arity.as_bytes()))
            {
                let mut got = vec![0_u8; expected.len()];
                socket.read_exact(&mut got).expect("EVAL request");
                assert_eq!(got, expected);
                socket.write_all(b":17\r\n").expect("integer response");
            }
        },
        |server| {
            let addr = server.addr();

            {
                let prepared = topology::try_new::<Env>(
                    try_config().expect("test configuration"),
                    addr,
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
                                    let keys = [b"k".as_slice(); 10];
                                    for (count, header) in KEY_COUNTS {
                                        let call = Split {
                                            keys: &keys[..count],
                                            ..SPLIT
                                        };
                                        assert!(matches!(call.wire_len(), Ok(len)
                                            if len == header.len() + count * KEY_BULK.len() + SPLIT_BULK.len()));
                                        assert!(matches!(
                                            runtime.block_on(
                                                redis.scripts().eval_call::<i64, _>(&call)
                                            ),
                                            Ok(Ok(17))
                                        ));
                                    }
                                    for len in [0, 9, 10, 99, 100] {
                                        let call = Owned {
                                            args: [vec![b'x'; len]],
                                        };
                                        assert!(matches!(
                                            runtime.block_on(
                                                redis.scripts().eval_call::<i64, _>(&call)
                                            ),
                                            Ok(Ok(17))
                                        ));
                                    }
                                    assert!(matches!(
                                        runtime.block_on(redis.scripts().eval::<i64, &[u8], _>(
                                            b"return 17",
                                            &[],
                                            &[b"".as_slice(); 7],
                                        )),
                                        Ok(Ok(17))
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
            server.finish().expect("finish Redis peer");
        },
    )
    .expect("spawn Redis peer");
}
