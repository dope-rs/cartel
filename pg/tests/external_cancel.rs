use crate::common::external::runtime::Plan;
use crate::{
    common::external::own::Convert as _, common::external::user::Lookup,
    common::external::user::Write,
};
use cartel_pg::api::traits::{
    Begin as _, Client as _, Savepoint as _, Transaction as _, Unchecked as _,
};
use cartel_pg::api::tx::control::error::Kind;
use common::env::{self, Env};
use common::external::{Observer, own};
use common::{external::runtime::check, recover};
use dope_fiber::BlockOn as _;
use dope_fiber::abi::Join;
use dope_test::fibers;
use std::env::var_os;
use std::thread;

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
                        rt.runtime
                            .block_on(Write::rename(tx.query(), 1, "outer".into()))?,
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
                                .block_on(Write::rename(point.query(), 1, "inner".into()))?,
                            Ok(())
                        ),
                        "inner update succeeds"
                    );
                }
                tx.cancel();
                ::core::assert_eq!(
                    rt.runtime
                        .block_on(Lookup::by_id(client.query(), 1))?
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
fn bounded() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_run(|rt| {
            let client = rt.client();
            let tx = rt.runtime
                .block_on(client.transaction().begin())?
                .ok()
                .expect("tx begins");
            let token = tx.cancel_token().expect("backend key data");
            ::core::assert!(token.pid() != 0);

            let query = tx.unchecked().command("SELECT pg_sleep(30)");
            let pid = token.pid();
            let observer = thread::spawn(move || Observer::observe(input, pid, |observed| {
                let result = observed.expect("observer timer").expect("observer deadline");
                match &result {
                    Ok(()) => {}
                    Err(own::Error::Query(error)) => {
                        assert!(result.is_ok(), "observer query: {:?}", error.kind());
                    }
                    Err(own::Error::Decode(error)) => {
                        assert!(result.is_ok(), "observer row: {error:?}");
                    }
                }
            }, recover, check).expect("observer runtime"));
            let observed = fibers::TEST.within(dope_fiber::fiber!('_ => async move {
                while !observer.is_finished() {
                    fibers::Yield::new().await;
                }
                observer.join()
            }));
            let request = &token;
            let cancel = dope_fiber::fiber!('_ => async move {
                observed.await
                    .expect("observer wait timer")
                    .expect("observer wait deadline")
                    .expect("observer query");
                Join::new(request.cancel(), request.cancel()).await
            });
            let (query, (first, second)) = rt.runtime.block_on(Join::new(query, cancel))?;

            assert!(query.as_ref().is_err_and(|error| error.as_server().map(|error| error.fields().code().as_str()) == Some("57014")), "query must be cancelled");
            ::core::assert!(matches!(first, Ok(())), "cancel request delivery");
            assert!(second.as_ref().is_err_and(|error| matches!(error.kind(), Kind::Capacity { target, slots } if target == token.target() && slots.get() == 1)), "the first cancellation retains the only buffer");
            assert!(
                matches!(rt.runtime.block_on(tx.rollback())?, Ok(())),
                "tx rolls back"
            );
            Ok(())
        }, recover, check).expect("PostgreSQL runtime");
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn reuses_slot() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_run(
            |rt| {
                let client = rt.client();
                let tx = rt
                    .runtime
                    .block_on(client.transaction().begin())?
                    .ok()
                    .expect("tx begins");
                let token = tx.cancel_token().expect("backend key data");

                ::core::assert!(
                    rt.runtime
                        .block_on(fibers::TEST.poll_once(token.cancel()))?
                        .is_none(),
                    "cancel must first reserve and remain pending"
                );

                let query = tx.unchecked().command("SELECT pg_sleep(30)");
                let pid = token.pid();
                let observer = thread::spawn(move || {
                    Observer::observe(
                        input,
                        pid,
                        |observed| {
                            let result = observed
                                .expect("observer timer")
                                .expect("observer deadline");
                            match &result {
                                Ok(()) => {}
                                Err(own::Error::Query(error)) => {
                                    assert!(result.is_ok(), "observer query: {:?}", error.kind());
                                }
                                Err(own::Error::Decode(error)) => {
                                    assert!(result.is_ok(), "observer row: {error:?}");
                                }
                            }
                        },
                        recover,
                        check,
                    )
                    .expect("observer runtime")
                });
                let observed = fibers::TEST.within(dope_fiber::fiber!('_ => async move {
                    while !observer.is_finished() {
                        fibers::Yield::new().await;
                    }
                    observer.join()
                }));
                let request = &token;
                let cancel = dope_fiber::fiber!('_ => async move {
                    observed.await
                        .expect("observer wait timer")
                        .expect("observer wait deadline")
                        .expect("observer query");
                    request.cancel().await
                });
                let (query, cancel) = rt.runtime.block_on(Join::new(query, cancel))?;
                assert!(
                    query.as_ref().is_err_and(|error| error
                        .as_server()
                        .map(|error| error.fields().code().as_str())
                        == Some("57014")),
                    "replacement cancel must reach PostgreSQL"
                );
                ::core::assert!(
                    matches!(cancel, Ok(())),
                    "the released bounded cancel slot must be reusable"
                );
                assert!(
                    matches!(rt.runtime.block_on(tx.rollback())?, Ok(())),
                    "tx rolls back"
                );
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}
