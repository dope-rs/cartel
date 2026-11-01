use cartel_redis::api::Client as _;
use cartel_redis::value::source::{Bulks as _, Bytes as _, Scan as _};
use dope::core::driver::life::recovery;
use std::io::{Read as _, Write as _};

use cartel_redis::{client::settings::init::Plan, value::reply};
use common::{
    command::{list, unsigned},
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
    harness::{GUARD, peer::TcpScript},
    reclaim::Reports,
};

pub mod common;

#[test]
fn unsigned_domain() {
    TcpScript::with(|socket| {
            for (request, reply) in unsigned::SCRIPT {
                let mut received = vec![0; request.len()];
                socket.read_exact(&mut received).expect("Redis request");
                assert_eq!(received, request);
                socket.write_all(reply).expect("Redis response");
            }
        }, |server| {
        let prepared = topology::try_new::<Bundle<Tcp, Identity, Throughput>>(
            list::try_config(1).expect("Redis configuration"),
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
                        assert!(matches!(
                            runtime.block_on(redis.keyspace().scan(0, None, None)),
                            Ok(Ok(scan)) if scan.cursor() == u64::MAX && scan.keys().is_empty()
                        ));
                        assert!(matches!(
                            runtime.block_on(redis.keyspace().scan(0, None, None)),
                            Ok(Err(reply::Failure::Decode(reply::Decode::NegativeCursor(-1))))
                        ));
                        assert!(matches!(
                            runtime.block_on(redis.scripts().eval::<u64, &[u8], &[u8]>(b"0", &[], &[])),
                            Ok(Ok(u64::MAX))
                        ));
                        assert!(matches!(
                            runtime.block_on(redis.scripts().eval::<Option<u64>, &[u8], &[u8]>(b"0", &[], &[])),
                            Ok(Ok(Some(u64::MAX)))
                        ));
                        assert!(matches!(
                            runtime.block_on(redis.scripts().eval::<u64, &[u8], &[u8]>(b"0", &[], &[])),
                            Ok(Err(reply::Failure::Number(reply::Number::Integer {
                                value,
                                cause: reply::Invalid::Overflow { offset: 19 },
                            }))) if value.as_slice() == b"18446744073709551616"
                        ));
                        assert!(matches!(
                            runtime.block_on(redis.scripts().eval::<Option<u64>, &[u8], &[u8]>(b"0", &[], &[])),
                            Ok(Err(reply::Failure::Decode(reply::Decode::NegativeInteger(-1))))
                        ));
                        assert!(matches!(
                            runtime.block_on(redis.scripts().eval::<i64, &[u8], &[u8]>(b"0", &[], &[])),
                            Ok(Ok(i64::MIN))
                        ));
                        for by in [i64::MIN, i64::MAX] {
                            assert!(matches!(
                                runtime.block_on(redis.basic().incr_by(b"key", by)),
                                Ok(Ok(value)) if value == by
                            ));
                        }
                        assert!(matches!(
                            runtime.block_on(redis.keyspace().scan(u64::MAX, None, None)),
                            Ok(Ok(scan)) if scan.cursor() == 0 && scan.keys().is_empty()
                        ));
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
