use cartel_pg::{
    api::traits::{Client as _, Notify as _, Status as _, Unchecked as _},
    port::{self, config},
};
use common::{
    counted::Counted,
    port::Queries,
    postgres::{notice, startup::Stream},
    topology,
};
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
use std::{cell::Cell, time::Duration};

pub mod common;

#[test]
fn notification_waiters_finish_each_wake_round() {
    AcceptScript::with(
        |peer| {
            let mut socket = peer.accept()?;
            Stream::try_start(&mut socket, None)?;
            notice::try_batch(&socket, &notice::SEED[..1])?;
            notice::try_batch(&socket, &notice::SEED[1..])?;
            assert_eq!(socket.read(&mut [0])?, 0);
            Ok::<_, common::postgres::Error>(())
        },
        |server| {
            let capacity = config::Config::new(config::Capacities {
                connections: 1, inflight: 2, admission_waiters: 3,
                requests: config::Request { frames: 2, frame_bytes: 512 },
                responses: config::Response { items: 2, bytes: 2_048, frame_bytes: 1_024 },
                notifications: config::Notification { items: 3, bytes: 128 },
            }).expect("PG configuration");
            let prepared = topology::Prepare::try_new::<Bundle<Tcp, Identity, Throughput>>(
                port::Port::<Queries>::plan(Queries::try_config().expect("PG login"), capacity),
                server.addr(), Duration::from_millis(1), Duration::from_secs(1),
            ).expect("PG topology");
            let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
            let mut report = Completion::new();
            prepared.with_local(
                |worker| worker.lend(|node, stage| node.install(stage, |host, stage| {
                    let timer = stage.driver().driver_ref().timer();
                    stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                        let client = runtime.root_client();
                        assert!(matches!(fibers::TEST.try_run(&mut runtime, client.status().wait_ready()), Ok(Ok(()))));
                        let polls = [Cell::new(0), Cell::new(0), Cell::new(0)];
                        let first = Counted::new(client.notify().next(), &polls[0]);
                        let second = Counted::new(client.notify().next(), &polls[1]);
                        let third = Counted::new(client.notify().next(), &polls[2]);
                        let observed = &polls;
                        let notify = dope_fiber::fiber!('_ => async move {
                            assert!(client.unchecked().command(notice::COMMAND).await.is_ok());
                            Sleep::new(timer, Duration::from_millis(20)).expect("settle timer").await;
                            let before = observed.each_ref().map(Cell::get);
                            Sleep::new(timer, Duration::from_millis(20)).expect("observe timer").await;
                            for (polls, before) in observed.iter().zip(before) {
                                assert!(polls.get() <= before + 2, "empty notification waiter kept polling");
                            }
                            assert!(client.unchecked().command(notice::COMMAND).await.is_ok());
                        });
                        let ((first, second), (third, ())) = fibers::TEST.try_run(
                            &mut runtime, Join::new(Join::new(first, second), Join::new(third, notify)),
                        ).expect("subsequent notifications wake remaining waiters");
                        assert!(first.is_ok() && second.is_ok() && third.is_ok());
                    })
                })),
                recovery::Policy::new(|event| reports.push(event), common::recover),
                |result| assert!(matches!(result, Ok(Ok(Ok(()))))),
            );
            assert!(reports.events().expect("reclamation events").is_empty());
            assert!(matches!(server.finish(), Ok(Ok(()))));
        },
    ).expect("PG peer");
}
