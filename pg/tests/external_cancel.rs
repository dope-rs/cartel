use crate::common::external::runtime::Plan;
use crate::{
    common::external::own::Convert as _, common::external::user::Lookup,
    common::external::user::Write,
};
use cartel_pg::api::traits::Connection as _;
use cartel_pg::api::traits::{
    Begin as _, Client as _, Status as _, Transaction as _, Unchecked as _,
};
use cartel_pg::api::tx::control::error::Kind;
use cartel_pg::port;
use common::env::{self, Env};
use common::external::{Observer, own};
use common::{
    external::runtime::{check, limits::Limits},
    port::Queries,
    recover,
};
use dope::{
    core::driver::{life::recovery, route::Id},
    manifold::{app::env::Bundle, service, timing::Throughput},
    net::{tcp::Tcp, wire::Identity},
    runtime::{client::Pair, executor::session::Completion, topology},
};
use dope_fiber::BlockOn as _;
use dope_fiber::abi::Join;
use dope_test::fibers;
use dope_test::{harness::GUARD, reclaim::Reports};
use std::{env::var_os, num::NonZero, time::Duration};

pub mod common;

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn quarantines_transaction() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_run(
            |rt| {
                let client = rt.client();
                let mut tx = rt
                    .runtime
                    .block_on(client.transaction().begin())?
                    .ok()
                    .expect("tx begins");
                assert!(
                    matches!(
                        rt.runtime.block_on(Write::rename(&tx, 1, "outer".into()))?,
                        Ok(())
                    ),
                    "outer update succeeds"
                );
                {
                    let name = cartel_pg::Identifier::try_from(&mut String::from("drop_me"))
                        .expect("identifier storage must allocate");
                    let point = rt
                        .runtime
                        .block_on(tx.savepoint(name))?
                        .ok()
                        .expect("save opens");
                    assert!(
                        matches!(
                            rt.runtime
                                .block_on(Write::rename(&point, 1, "inner".into()))?,
                            Ok(())
                        ),
                        "inner update succeeds"
                    );
                }
                tx.cancel();
                ::core::assert_eq!(
                    rt.runtime
                        .block_on(Lookup::by_id(client, 1))?
                        .try_own()
                        .ok()
                        .expect("successful typed query")
                        .name,
                    "alice"
                );
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn bounded_cancel_slots() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    for reused in [false, true] {
        let address = input.address();
        let settings = Limits::new(NonZero::<usize>::MIN)
            .try_config::<()>()
            .expect("PostgreSQL limits");
        let snapshot = || {
            service::Snapshot::from_one(
                service::Revision::new(1),
                service::Endpoint::new(address, address),
            )
        };
        let (_, main) = port::Port::<Queries>::plan(
            input.try_config().expect("PostgreSQL config"),
            settings.clone(),
        )
        .assemble::<Bundle<Tcp, Identity, Throughput>>(
            snapshot(),
            Duration::from_millis(500),
            Duration::from_secs(512),
        )
        .expect("main plan")
        .into_parts();
        let (_, observer) = port::Port::<Observer, 1>::plan(
            input.try_config().expect("PostgreSQL config"),
            settings,
        )
        .assemble::<Bundle<Tcp, Identity, Throughput>>(
            snapshot(),
            Duration::from_millis(500),
            Duration::from_secs(512),
        )
        .expect("observer plan")
        .into_parts();
        let prepared = topology::Plan::new((), (main, observer))
            .for_install::<Throughput>()
            .expect("paired topology");
        let mut reports = Reports::new(prepared.layout(), [Id::fixed::<0>(), Id::fixed::<1>()]);
        let mut done = Completion::new();
        prepared
            .with_local(
                |worker| {
                    worker.lend(|(main, observer), stage| {
                        main.install(stage, |main, stage| {
                            observer.install(stage, |observer, stage| {
                                stage.with_quiesce(
                                    &mut done,
                                    Pair::new(main, observer),
                                    GUARD,
                                    |mut runtime| {
                                        let (client, observer) = runtime.root_client();
                                        runtime
                                            .block_on(client.status().wait_ready())?
                                            .ok()
                                            .expect("PostgreSQL ready");
                                        let tx = runtime
                                            .block_on(client.transaction().begin())?
                                            .ok()
                                            .expect("tx begins");
                                        let token = tx.cancel_token().expect("backend key data");
                                        if reused {
                                            assert!(
                                                runtime
                                                    .block_on(fibers::TEST.poll_once(token.cancel()))?
                                                    .is_none(),
                                                "cancel must first reserve and remain pending"
                                            );
                                        } else {
                                            assert!(token.pid() != 0);
                                        }
                                        let query = tx.query().command("SELECT pg_sleep(30)");
                                        let pid = token.pid();
                                        let request = &token;
                                        let cancel = dope_fiber::fiber!('_ => async move {
                                            let observed = Observer::observe(observer, pid).await;
                                            if reused {
                                                assert!(
                                                    matches!(request.cancel().await, Ok(())),
                                                    "the released bounded cancel slot must be reusable"
                                                );
                                            } else {
                                                let (first, second) = Join::new(
                                                    request.cancel(), request.cancel(),
                                                ).await;
                                                assert!(matches!(first, Ok(())), "cancel request delivery");
                                                assert!(
                                                    second.as_ref().is_err_and(|error| matches!(
                                                        error.kind(), Kind::Capacity { target, slots }
                                                            if target == request.target() && slots.get() == 1
                                                    )),
                                                    "the first cancellation retains the only buffer"
                                                );
                                            }
                                            observed
                                        });
                                        let (query, observed) =
                                            runtime.block_on(Join::new(query, cancel))?;
                                        let rollback = runtime.block_on(tx.rollback())?;
                                        let observed = observed
                                            .expect("observer timer")
                                            .expect("observer deadline");
                                        match &observed {
                                            Ok(()) => {}
                                            Err(own::Error::Query(error)) => {
                                                assert!(observed.is_ok(), "observer query: {:?}", error.kind());
                                            }
                                            Err(own::Error::Decode(error)) => {
                                                assert!(observed.is_ok(), "observer row: {error:?}");
                                            }
                                        }
                                        assert!(
                                            query.as_ref().is_err_and(|error| error
                                                .as_server()
                                                .map(|error| error.fields().code().as_str()) == Some("57014")),
                                            "query must be cancelled"
                                        );
                                        assert!(matches!(rollback, Ok(())), "tx rolls back");
                                        Ok::<(), dope::runtime::executor::drive::Error>(())
                                    },
                                )
                            })
                        })
                    })
                },
                recovery::Policy::new(|event| reports.push(event), recover),
                |outcome| outcome.map_err(common::entry_error).expect("application entry"),
            )
            .expect("main install")
            .expect("observer install")
            .expect("paired session")
            .expect("application driver");
        assert!(reports.events().expect("native reclamation").is_empty());
    }
}
