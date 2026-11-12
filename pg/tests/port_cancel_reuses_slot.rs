use crate::common::postgres::accept::queued_cancel_reuse;
use cartel_pg::api::traits::{Begin as _, Client as _, Status as _, Transaction as _};
use cartel_pg::{api, api::tx::control::error::Kind, port};
use common::{port::*, topology};
use dope::core::driver::life::recovery;
use dope::runtime::executor::session::Completion;
use dope::{
    core::driver::route::Id, manifold::app::env::Bundle, manifold::timing::Throughput,
    net::tcp::Tcp, net::wire::Identity,
};
use dope_fiber::{BlockOn as _, abi::Join};
use dope_test::fibers;
use dope_test::harness::peer;
use dope_test::{harness::GUARD, reclaim::Reports};
use std::time::Duration;

type Env = Bundle<Tcp, Identity, Throughput>;

pub mod common;

#[test]
fn cancel_reuses_slot() {
    peer::AcceptScript::with(queued_cancel_reuse, |server| {
        let config = Queries::try_cancel().expect("cancel config");
        let ((), events) = {
            let prepared = topology::try_new::<_, Env>(
                port::Port::<Queries>::plan(
                    Queries::try_config().expect("database config"),
                    config,
                ),
                server.addr(),
                Duration::from_millis(1),
                Duration::from_millis(1_024),
            )
            .expect("topology fixture completed");
            let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
            let mut report = Completion::new();
            let scoped = prepared.with_local(
                |worker| {
                    worker.lend(|node, stage| {
                        node.install(stage, |host, stage| {
                            stage.with_quiesce(&mut report, host, GUARD, |mut runtime| {
                                let client: api::Client<
                                    api::Context<'_, '_, '_, '_, '_, '_, '_, _, _>,
                                > = runtime.root_client();

                                assert!(
                                    matches!(
                                        runtime.block_on(client.status().wait_ready()),
                                        Ok(Ok(()))
                                    ),
                                    "PostgreSQL connection ready"
                                );
                                let tx = runtime
                                    .block_on(client.transaction().begin())
                                    .expect("runtime")
                                    .ok()
                                    .expect("tx");
                                let token = tx.cancel_token().expect("backend key data");
                                assert!(
                                    matches!(
                                        runtime.block_on(fibers::TEST.poll_once(token.cancel())),
                                        Ok(None)
                                    ),
                                    "first cancel must be dropped from the queued state"
                                );
                                let (first, second) = runtime
                                    .block_on(Join::new(token.cancel(), token.cancel()))
                                    .expect("cancellation runtime");
                                assert!(matches!(first, Ok(())), "reused cancel slot");
                                assert!(
                                    second.as_ref().is_err_and(|error| matches!(
                                        error.kind(),
                                        Kind::Capacity { target, slots }
                                            if target == token.target() && slots.get() == 1
                                    )),
                                    "the active cancellation retains the only buffer"
                                );
                                assert!(
                                    matches!(runtime.block_on(tx.rollback()), Ok(Ok(()))),
                                    "rollback"
                                );
                            })
                        })
                    })
                },
                recovery::Policy::new(|event| reports.push(event), common::recover),
                |outcome| outcome.map_err(common::entry_error).expect("application entry"),
            );

            let result = scoped.expect("application installation completed");
            (result.expect("application session completed"), reports)
        };
        assert!(
            events.events().expect("native reclamation").is_empty(),
            "unexpected events PostgreSQL operations: {events:?}"
        );
        server
            .finish()
            .expect("cancel peer")
            .expect("cancel protocol");
    })
    .expect("cancel listener");
}
