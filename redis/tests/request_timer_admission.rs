use cartel_redis::{
    Error, api::Commands as _, client::settings::init::Plan, request, value::reply::Failure,
};
use dope::{
    core::driver::{life::recovery, route::Id, schedule::clock::timer::Exhausted},
    manifold::{app::env::Bundle, timing::Throughput},
    net::{tcp::Tcp, wire::Identity},
    runtime::executor::session::Completion,
};
use dope_fiber::BlockOn as _;
use dope_test::{harness::GUARD, reclaim::Reports};
use o3::base::time::Positive;
use std::{net::SocketAddr, num::NonZero};

pub mod common;

#[test]
fn encoding_failure_precedes_timer_exhaustion() {
    let config = common::session::config(Positive::from_secs::<1>()).expect("Redis configuration");
    let prepared = common::topology::try_new::<Bundle<Tcp, Identity, Throughput>>(
        config,
        SocketAddr::from(([127, 0, 0, 1], 0)),
        Plan::new(),
    )
    .expect("Redis topology");
    let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
    let mut report = Completion::new();
    prepared.with_local(
        |worker| {
            worker.lend(|node, stage| {
                node.install(stage, |host, stage| {
                    let timer = stage.driver().driver_ref().timer();
                    stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                        let redis = runtime.root_client();
                        let mut alarms = Vec::new();
                        while let Ok(alarm) = timer.try_alarm() {
                            alarms.push(alarm);
                        }
                        let invalid = redis.hashes().set_iter(
                            b"key",
                            NonZero::new(2).expect("declared count"),
                            [(b"field", b"value")].into_iter(),
                        );
                        assert!(matches!(
                            runtime.block_on(invalid),
                            Ok(Err(Failure::Control(Error::Request(
                                request::Error::Items {
                                    declared: 2,
                                    actual: 1,
                                }
                            ))))
                        ));
                        assert!(matches!(
                            runtime.block_on(redis.server().ping()),
                            Ok(Err(Failure::Control(Error::Deadline(Exhausted::Slots))))
                        ));
                    })
                })
            })
        },
        recovery::Policy::new(|event| reports.push(event), common::recover),
        |result| assert!(matches!(result, Ok(Ok(Ok(()))))),
    );
    assert!(reports.events().expect("reclamation events").is_empty());
}
