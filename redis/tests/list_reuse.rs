use cartel_redis::api::Client as _;
use cartel_redis::value::source::{Bulks as _, PairRejected as _, Pairs as _};
use dope::core::driver::life::recovery;
use std::{convert::Infallible, ops::ControlFlow, time::Duration};

use cartel_redis::client::settings::init::Plan;
use common::{command::list::*, recover, topology};
use dope::{
    core::driver::route::Id,
    manifold::{app::env::Bundle, timing::Throughput},
    net::{tcp::Tcp, wire::Identity},
    runtime::executor::session::Completion,
};
use dope_fiber::BlockOn as _;
use dope_test::{
    alloc::Tracker,
    harness::{GUARD, peer::TcpScript},
    reclaim::Reports,
};

type Env = Bundle<Tcp, Identity, Throughput>;

pub mod common;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn pair_survives_reuse() {
    assert_eq!(REPLIES[0].len(), REPLIES[1].len());
    TcpScript::with(
        |socket| {
            socket
                .read_wait(Some(Duration::from_secs(1)))
                .expect("read timeout");
            for reply in REPLIES {
                let mut request = [0; ENTRIES.len()];
                socket.read_exact(&mut request).expect("Redis request");
                assert_eq!(request, ENTRIES);
                socket.write_all(reply).expect("Redis response");
            }
        },
        |server| {
            let address = server.addr();
            {
                let prepared = topology::try_new::<Env>(
                    try_config(2).expect("list configuration"),
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

                                    assert!(
                                        matches!(runtime.block_on(redis.wait_active()), Ok(Ok(()))),
                                        "connection"
                                    );
                                    let pairs = runtime.block_on(redis.hashes().entries(b"hash"));
                                    let pairs = pairs.expect("runtime").ok().expect("first reply");
                                    let mut origin = None;
                                    let (rejected, counts) = Tracker::usage(|| {
                                        pairs
                                            .try_check(|field, value| {
                                                if field == b"b" {
                                                    assert_eq!(value, b"2");
                                                    origin = Some((field.as_ptr(), value.as_ptr()));
                                                    Err(7_u32)
                                                } else {
                                                    Ok(())
                                                }
                                            })
                                            .err()
                                            .expect("second pair must fail")
                                    });
                                    assert_eq!(counts, (0, 0));
                                    assert_eq!(rejected.index(), 1);
                                    assert_eq!(*rejected.error(), 7);
                                    let origin = origin.expect("rejected source pointers");
                                    let pairs = runtime.block_on(redis.hashes().entries(b"hash"));
                                    let pairs = pairs
                                        .expect("runtime")
                                        .ok()
                                        .expect("parser credits must have been released");
                                    let ((), counts) = Tracker::usage(|| {
                                        assert_eq!(
                                            pairs.iter().next(),
                                            Some((&b"x"[..], &b"3"[..]))
                                        );
                                        let (field, value) = rejected.values();
                                        assert_eq!((field, value), (&b"b"[..], &b"2"[..]));
                                        assert_eq!((field.as_ptr(), value.as_ptr()), origin);
                                    });
                                    assert_eq!(counts, (0, 0));
                                    assert!(pairs.try_check::<Infallible>(|_, _| Ok(())).is_ok());
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
fn drains_before_drop() {
    TcpScript::with(
        |socket| {
            socket
                .read_wait(Some(Duration::from_secs(1)))
                .expect("read timeout");
            for first in DRAINS {
                for case in [first, List::Pairs] {
                    let mut request = [0; ENTRIES.len()];
                    let request = &mut request[..case.request().len()];
                    socket.read_exact(request).expect("Redis request");
                    assert_eq!(request, case.request());
                    socket.write_all(case.reply()).expect("Redis response");
                }
            }
        },
        |server| {
            let prepared = topology::try_new::<Env>(
                try_config(1).expect("list configuration"),
                server.addr(),
                Plan::new(),
            )
            .expect("Redis topology");
            let mut report = Completion::new();
            let (prepared, report, timeout) = (prepared, &mut report, GUARD);
            prepared.with_local(
                |worker| {
                    worker.lend(|node, stage| {
                        node.install(stage, |host, stage| {
                            stage.with_quiesce(report, host, timeout, |mut runtime| {
                                let redis = runtime.root_client();
                                assert!(matches!(
                                    runtime.block_on(redis.wait_active()),
                                    Ok(Ok(()))
                                ));
                                for case in DRAINS {
                                    if matches!(case, List::Pairs) {
                                        let values = runtime
                                            .block_on(redis.hashes().entries(b"hash"))
                                            .expect("pair operation")
                                            .ok()
                                            .expect("pair reply");
                                        let after = AfterDrain(|| {
                                            assert!(matches!(
                                                runtime.block_on(redis.hashes().entries(b"hash")),
                                                Ok(Ok(_))
                                            ));
                                        });
                                        assert!(
                                            values
                                                .try_drain::<(), ()>(move |_, _| {
                                                    let _ = &after;
                                                    Ok(())
                                                })
                                                .is_ok()
                                        );
                                    } else {
                                        let values = runtime
                                            .block_on(redis.sets().members(b"set"))
                                            .expect("array operation")
                                            .ok()
                                            .expect("array reply");
                                        let after = AfterDrain(|| {
                                            assert!(matches!(
                                                runtime.block_on(redis.hashes().entries(b"hash")),
                                                Ok(Ok(_))
                                            ));
                                        });
                                        let result = if matches!(case, List::ArrayVisitAt) {
                                            values.try_drain_at(move |_, _| {
                                                let _ = &after;
                                                ControlFlow::<()>::Continue(())
                                            })
                                        } else {
                                            values.try_drain(move |_| {
                                                let _ = &after;
                                                ControlFlow::<()>::Continue(())
                                            })
                                        };
                                        assert!(result.is_ok());
                                    }
                                }
                            })
                        })
                    })
                },
                recovery::Policy::new(drop, recover),
                |outcome| assert!(matches!(outcome, Ok(Ok(Ok(()))))),
            );
            server.finish().expect("Redis peer completion");
        },
    )
    .expect("Redis peer");
}
