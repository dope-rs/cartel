use cartel_redis::{
    api::{Client as _, Connection as _},
    client::settings::init::Plan,
};
use common::{counted::Counted, session, topology};
use dope::{
    core::driver::{life::recovery, route::Id},
    manifold::{app::env::Bundle, timing::Throughput},
    net::{tcp::Tcp, wire::Identity},
    runtime::executor::session::Completion,
};
use dope_fiber::{abi::Join, task::time::sleep::Sleep};
use dope_test::{
    fibers,
    harness::{GUARD, peer::AcceptScript},
    reclaim::Reports,
};
use o3::base::time::Positive;
use std::{cell::Cell, time::Duration};

pub mod common;

#[test]
fn blocked_lease_waiters_finish_each_notification_round() {
    AcceptScript::with(
        |peer| {
            let socket = peer.accept()?;
            session::held::pong_once(&socket)?;
            assert_eq!(socket.read(&mut [0])?, 0);
            Ok::<_, session::held::Error>(())
        },
        |server| {
            let config = session::config(Positive::from_secs::<1>()).expect("Redis configuration");
            let prepared = topology::try_new::<Bundle<Tcp, Identity, Throughput>>(
                config, server.addr(), Plan::new(),
            ).expect("Redis topology");
            let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
            let mut report = Completion::new();
            prepared.with_local(
                |worker| worker.lend(|node, stage| node.install(stage, |host, stage| {
                    let timer = stage.driver().driver_ref().timer();
                    stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                        let redis = runtime.root_client();
                        let mut held = fibers::TEST.try_run(&mut runtime, redis.connection())
                            .expect("acquire runtime").ok().expect("held connection");
                        let polls = [Cell::new(0), Cell::new(0)];
                        let first = Counted::new(dope_fiber::fiber!('_ => async move {
                            drop(redis.connection().await.ok().expect("first connection"));
                        }), &polls[0]);
                        let second = Counted::new(dope_fiber::fiber!('_ => async move {
                            drop(redis.connection().await.ok().expect("second connection"));
                        }), &polls[1]);
                        let observed = &polls;
                        let release = dope_fiber::fiber!('_ => async move {
                            assert!(held.ping().await.is_ok());
                            Sleep::new(timer, Duration::from_millis(20)).expect("settle timer").await;
                            let before = observed.each_ref().map(Cell::get);
                            Sleep::new(timer, Duration::from_millis(20)).expect("observe timer").await;
                            for (polls, before) in observed.iter().zip(before) {
                                assert!(polls.get() <= before + 2, "blocked waiter kept polling");
                            }
                            drop(held);
                        });
                        fibers::TEST.try_run(&mut runtime, Join::new(Join::new(first, second), release))
                            .expect("wake after connection release");
                    })
                })),
                recovery::Policy::new(|event| reports.push(event), common::recover),
                |result| assert!(matches!(result, Ok(Ok(Ok(()))))),
            );
            assert!(reports.events().expect("reclamation events").is_empty());
            assert!(matches!(server.finish(), Ok(Ok(()))));
        },
    ).expect("Redis peer");
}
