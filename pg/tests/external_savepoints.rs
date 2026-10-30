use crate::common::external::runtime::Plan;
use crate::{
    common::external::own, common::external::own::Convert as _,
    common::external::reports::Aggregate, common::external::user::Lookup,
    common::external::user::Write,
};
use cartel_pg::api::traits::{
    Begin as _, Client as _, Savepoint as _, Transaction as _, Unchecked as _,
};
use cartel_pg::{Identifier, api::sql, api::tx::Isolation};
use common::env::{self, Env};
use common::{external::runtime::check, recover};
use dope_fiber::BlockOn as _;
use std::env::var_os;
use std::time::Duration;

pub mod common;

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn cancels_close() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_run(|rt| {
            let client = rt.client();
            let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                let mut tx = client
                    .transaction()
                    .begin()
                    .await
                    .map_err(own::Error::Query)?;
                let save = tx
                    .savepoint(Identifier::try_from(&mut String::from("cancel_close")).expect("identifier storage"))
                    .await
                    .map_err(own::Error::Query)?;
                save.cancel();
                ::core::assert_eq!(
                    Lookup::by_id(client.query(), 1)
                        .await
                        .try_own()?
                        .name,
                    "alice"
                );
                tx.cancel();
                Ok::<(), own::Error<'_>>(())
            }))?;
            assert!(
                result.is_ok(),
                "cancelled save closes must release the route"
            );
            Ok(())
        }, recover, check).expect("PostgreSQL runtime");
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn drop_rolls_back() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_run(
            |rt| {
                let client = rt.client();
                let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                    {
                        let tx = client
                            .transaction()
                            .begin()
                            .await
                            .map_err(own::Error::Query)?;
                        Write::rename(tx.query(), 1, "drop-rollback".into())
                            .await
                            .try_own()?;
                    }
                    let u = Lookup::by_id(client.query(), 1)
                        .await
                        .try_own()?;
                    ::core::assert_eq!(u.name, "alice");
                    Ok::<(), own::Error<'_>>(())
                }))?;
                assert!(result.is_ok(), "dropped transactions must roll back");
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn isolation() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_run(
            |rt| {
                let client = rt.client();
                let result = (|| -> Result<(), own::Error<'_>> {
                    for level in [
                        Isolation::ReadCommitted,
                        Isolation::RepeatableRead,
                        Isolation::Serializable,
                    ] {
                        let tx = rt
                            .runtime
                            .block_on(client.transaction().isolation(level).begin())
                            .expect("runtime drive")
                            .map_err(own::Error::Query)?;
                        ::core::assert_eq!(
                            rt.runtime
                                .block_on(Lookup::by_id(tx.query(), 1))
                                .expect("runtime drive")
                                .try_own()?
                                .id,
                            1
                        );
                        rt.runtime
                            .block_on(tx.commit())
                            .expect("runtime drive")
                            .map_err(own::Error::Query)?;
                    }
                    let tx = rt
                        .runtime
                        .block_on(
                            client
                                .transaction()
                                .isolation(Isolation::Serializable)
                                .read_only()
                                .deferrable()
                                .begin(),
                        )
                        .expect("runtime drive")
                        .map_err(own::Error::Query)?;
                    ::core::assert_eq!(
                        rt.runtime
                            .block_on(Aggregate::count_all(tx.query()))
                            .expect("runtime drive")
                            .try_own()?,
                        5
                    );
                    rt.runtime
                        .block_on(tx.commit())
                        .expect("runtime drive")
                        .map_err(own::Error::Query)?;
                    Ok(())
                })();
                assert!(result.is_ok(), "tx isolation modes must commit");
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn timeout() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_run(
            |rt| {
                let client = rt.client();
                let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
            let tx = client
                .transaction()
                .timeout(
                    sql::Timeout::new(Duration::from_secs(10)).expect("positive statement timeout"),
                )
                .begin()
                .await
                .map_err(own::Error::Query)?;
            ::core::assert_eq!(
                Aggregate::count_all(tx.query())
                    .await
                    .try_own()?,
                5
            );
            tx.commit().await.map_err(own::Error::Query)?;
            Ok::<(), own::Error<'_>>(())
        }))?;
                assert!(result.is_ok(), "tx statement timeout must permit the query");
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn savepoint_end() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_run(|rt| {
            let client = rt.client();
            let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                let mut tx = client
                    .transaction()
                    .begin()
                    .await
                    .map_err(own::Error::Query)?;
                Write::rename(tx.query(), 1, "lvl-0".into())
                    .await
                    .try_own()?;

                let sp = tx
                    .savepoint(Identifier::try_from(&mut String::from("a\"quoted")).expect("identifier storage"))
                    .await
                    .map_err(own::Error::Query)?;
                Write::rename(sp.query(), 1, "lvl-1".into())
                    .await
                    .try_own()?;
                sp.release().await.map_err(own::Error::Query)?;
                ::core::assert_eq!(
                    Lookup::by_id(tx.query(), 1)
                        .await
                        .try_own()?
                        .name,
                    "lvl-1"
                );

                let sp2 = tx
                    .savepoint(Identifier::try_from(&mut String::from("b\"quoted")).expect("identifier storage"))
                    .await
                    .map_err(own::Error::Query)?;
                Write::rename(sp2.query(), 1, "lvl-2".into())
                    .await
                    .try_own()?;
                ::core::assert_eq!(
                    Lookup::by_id(sp2.query(), 1)
                        .await
                        .try_own()?
                        .name,
                    "lvl-2"
                );
                sp2.rollback().await.map_err(own::Error::Query)?;
                ::core::assert_eq!(
                    Lookup::by_id(tx.query(), 1)
                        .await
                        .try_own()?
                        .name,
                    "lvl-1"
                );

                let mut sp_a = tx
                    .savepoint(Identifier::try_from(&mut String::from("aa")).expect("identifier storage"))
                    .await
                    .map_err(own::Error::Query)?;
                Write::rename(sp_a.query(), 1, "lvl-3a".into())
                    .await
                    .try_own()?;
                let sp_b = sp_a
                    .savepoint(Identifier::try_from(&mut String::from("bb\"quoted")).expect("identifier storage"))
                    .await
                    .map_err(own::Error::Query)?;
                Write::rename(sp_b.query(), 1, "lvl-3b".into())
                    .await
                    .try_own()?;
                sp_b.rollback().await.map_err(own::Error::Query)?;
                ::core::assert_eq!(
                    Lookup::by_id(sp_a.query(), 1)
                        .await
                        .try_own()?
                        .name,
                    "lvl-3a"
                );
                sp_a.release().await.map_err(own::Error::Query)?;

                tx.commit().await.map_err(own::Error::Query)?;
                ::core::assert_eq!(
                    Lookup::by_id(client.query(), 1)
                        .await
                        .try_own()?
                        .name,
                    "lvl-3a"
                );
                Ok::<(), own::Error<'_>>(())
            }))?;
            assert!(result.is_ok(), "nested save ends must preserve values");
            Ok(())
        }, recover, check).expect("PostgreSQL runtime");
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn releases_server() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_run(|rt| {
            let client = rt.client();
            let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                let mut tx = client
                    .transaction()
                    .begin()
                    .await
                    .map_err(own::Error::Query)?;
                let save = tx
                    .savepoint(Identifier::try_from(&mut String::from("released_after_rollback")).expect("identifier storage"))
                    .await
                    .map_err(own::Error::Query)?;
                save.rollback().await.map_err(own::Error::Query)?;

                let error = tx
                    .unchecked()
                    .command("RELEASE SAVEPOINT \"released_after_rollback\"")
                    .await;
                ::core::assert_eq!(
                    error.as_ref().err().and_then(|error| error.as_server())
                        .map(|error| error.fields().code().as_str()),
                    Some("3B001")
                );
                tx.rollback().await.map_err(own::Error::Query)?;
                Ok::<(), own::Error<'_>>(())
            }))?;
            assert!(result.is_ok(), "rollback must release the server save");
            Ok(())
        }, recover, check).expect("PostgreSQL runtime");
}
