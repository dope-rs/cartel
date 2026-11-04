use cartel_redis::api::Client as _;
use cartel_redis::value::source::{Reader as _, Reply as _};
use dope::core::driver::life::recovery;
use dope::runtime::executor::session::Completion;
use std::{iter, num::NonZero, time::Duration};

use cartel_redis::{
    client::{ops::scripts::Invocation as _, settings::init::Plan},
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
            let mut delete = [0; DELETE.len()];
            socket.read_exact(&mut delete).expect("valid DEL request");
            assert_eq!(delete, DELETE);
            socket.write_all(b":2\r\n").expect("DEL response");
            let mut add = [0; ADD.len()];
            socket.read_exact(&mut add).expect("valid SADD request");
            assert_eq!(add, ADD);
            socket.write_all(b":2\r\n").expect("SADD response");
            let mut got = [0; EXPECTED.len()];
            socket.read_exact(&mut got).expect("valid EVAL request");
            assert_eq!(got, EXPECTED);
            socket.write_all(b":17\r\n").expect("integer response");
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

                                    assert!(
                                        matches!(
                                            runtime.block_on(
                                                redis.scripts().eval::<i64, &[u8], _>(
                                                    b"return 17",
                                                    &[],
                                                    &[b"ok".as_slice()],
                                                )
                                            ),
                                            Ok(Ok(17))
                                        ),
                                        "recycled frame must remain reusable"
                                    );
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
        keys: [],
        args: [vec![b'x'; 300 * 1024]],
    };
    let need = call.wire_len().expect("test operation must succeed");
    assert!(need > 256 * 1024);

    TcpScript::with(
        move |socket| {
            let mut got = vec![0_u8; need];
            socket.read_exact(&mut got).expect("bulk EVAL request");
            assert!(
                got[need - 2 - 300 * 1024..need - 2]
                    .iter()
                    .all(|byte| *byte == b'x')
            );
            socket.write_all(b":17\r\n").expect("integer response");
        },
        |server| {
            let addr = server.addr();

            {
                let prepared = topology::try_new::<Env>(
                    try_bulk_config(need).expect("bulk configuration"),
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
                                    assert!(matches!(
                                        runtime
                                            .block_on(redis.scripts().eval_call::<i64, _>(&call)),
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

    TcpScript::with(
        move |socket| {
            let mut byte = [0_u8; 1];
            assert_eq!(socket.read(&mut byte).expect("connection close"), 0);
        },
        |server| {
            let addr = server.addr();
            {
                let prepared = topology::try_new::<Env>(
                    try_bulk_config(need - 1).expect("bulk configuration"),
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

                assert!(matches!(runtime.block_on(redis.wait_active()), Ok(Ok(()))));
                assert!(matches!(
                    runtime.block_on(redis.scripts().eval_call::<i64, _>(&call)),
                    Ok(Err(value::reply::Failure::Control(cartel_redis::Error::Request(
                        cartel_redis::request::Error::Capacity(cause)
                    )))) if {
                        let (actual, cap) = cause.into_parts();
                        actual.get() == need && cap == need - 1
                    }
                ), "oversized EVAL must fail before submission");

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
fn split_is_one_bulk() {
    const EXPECTED: &[u8] =
        b"*4\r\n$4\r\nEVAL\r\n$9\r\nreturn 17\r\n$1\r\n0\r\n$12\r\nfirst\0second\r\n";

    let call = SPLIT;
    assert!(matches!(call.wire_len(), Ok(len) if len == EXPECTED.len()));

    TcpScript::with(
        move |socket| {
            let lengths = [0, 9, 10, 99, 100].map(|len| {
                format!(
                    "*4\r\n$4\r\nEVAL\r\n$9\r\nreturn 17\r\n$1\r\n0\r\n${len}\r\n{}\r\n",
                    "x".repeat(len)
                )
            });
            let arity = format!(
                "*10\r\n$4\r\nEVAL\r\n$9\r\nreturn 17\r\n$1\r\n0\r\n{}",
                "$0\r\n\r\n".repeat(7)
            );
            for expected in iter::once(EXPECTED)
                .chain(lengths.iter().map(String::as_bytes))
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
                                    assert!(matches!(
                                        runtime
                                            .block_on(redis.scripts().eval_call::<i64, _>(&call)),
                                        Ok(Ok(17))
                                    ));
                                    for len in [0, 9, 10, 99, 100] {
                                        let call = Owned {
                                            keys: [],
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
