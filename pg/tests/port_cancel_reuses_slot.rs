use crate::common::postgres::accept::queued_cancel_reuse;
use cartel_pg::api::traits::{Begin as _, Client as _, Status as _, Transaction as _};
use cartel_pg::{api, api::tx::control::error::Kind, port};
use common::{port::*, topology};
use dope::manifold::timing::Throughput;
use dope_fiber::{BlockOn as _, abi::Join};
use dope_test::fibers;
use dope_test::harness::peer;

pub mod common;

#[test]
fn cancel_reuses_slot() {
    peer::AcceptScript::with(queued_cancel_reuse, |server| {
        let config = Queries::try_cancel().expect("cancel config");
        {
            let plan = port::Port::<Queries>::plan(
                Queries::try_config().expect("database config"),
                config,
            );
            topology::run(plan, server.addr(), (1, 1_024), Throughput, |mut runtime| {
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
            });
        }
        server
            .finish()
            .expect("cancel peer")
            .expect("cancel protocol");
    })
    .expect("cancel listener");
}
