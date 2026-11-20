use crate::common::external::runtime::fixtures;
use crate::common::external::runtime::limits::Limits;
use crate::common::external::{
    own,
    own::Convert as _,
    user::{Lookup, LookupById, User, Write, WriteAddOne},
};
use cartel_pg::api::traits::{
    Begin as _, Client as _, Connection as _, Query as _, Transaction as _,
};
use cartel_pg::{
    Identifier,
    api::{
        results::group::{Outcome, Sync},
        tx::FinishError,
    },
    port::config,
    reply,
};
use common::env;
use dope_fiber::BlockOn as _;
use dope_test::fibers;
use std::convert::Infallible;
use std::env::var_os;

pub mod common;

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn groups_survive_finish() {
    use std::num::NonZero;

    fixtures::run(
        env::NAMES.map(var_os),
        Limits {
            groups: Some(config::Groups {
                active: NonZero::<usize>::MIN,
                records: NonZero::new(2).expect("diagnostic records"),
                bytes: NonZero::new(1024).expect("diagnostic bytes"),
            }),
            ..Limits::default()
        },
        |rt| {
            let client = rt.client();
            let rows = rt
                .runtime
                .block_on(client.run_group::<LookupById, _>([1, 2]))?
                .ok()
                .expect("group rows");
            let tx = rt
                .runtime
                .block_on(client.transaction().begin())?
                .ok()
                .expect("transaction");
            let outcome = rt
                .runtime
                .block_on(tx.query().run_group::<WriteAddOne, _>(User::conflicting()))?;
            let error = rt
                .runtime
                .block_on(tx.finish(outcome))?
                .err()
                .expect("group failure");
            let failure = match error {
                FinishError::Operation(failure) => Ok(failure),
                FinishError::Commit(error)
                | FinishError::Rollback {
                    rollback: error, ..
                } => Err(error),
            };
            let failure = failure
                .map_err(|error| format!("{:?}", common::port::Reply::from(&error)))
                .expect("rolled back group failure");
            assert_eq!(failure.sync(), Sync::Complete);
            assert!(matches!(
                failure.outcomes(),
                [Outcome::Completed(row), Outcome::Server, Outcome::Skipped]
                    if row.decode().expect("completed insert").name == "group-first"
            ));
            assert_eq!(
                failure
                    .diagnostic(1)
                    .expect("duplicate diagnostic")
                    .code()
                    .as_str(),
                "23505"
            );
            assert_eq!(rows[0].decode().expect("retained first row").name, "alice");
            assert_eq!(rows[1].decode().expect("retained second row").name, "bob");
            assert!(
                rt.runtime
                    .block_on(Lookup::maybe_by_id(client, 101))?
                    .ok()
                    .expect("rolled back lookup")
                    .is_none()
            );
            Ok(())
        },
    );
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn commit_reports_rollback() {
    fixtures::run(env::NAMES.map(var_os), Default::default(), |rt| {
        let client = rt.client();
        let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
            let tx = client.transaction().begin().await.map_err(own::Error::Query)?;
            let pid = tx.cancel_token().expect("backend key").pid();
            Write::add(&tx, 101, "rolled-back".into(), 30, 1.0, true)
                .await
                .try_own()?;
            let error = Write::add(&tx, 1, "duplicate".into(), 30, 1.0, true).await;
            ::core::assert_eq!(
                error.as_ref().err().and_then(|error| error.as_server())
                    .map(|error| error.fields().code().as_str()),
                Some("23505")
            );
            ::core::assert!(matches!(
                tx.commit().await,
                Err(reply::Error::Driver(cartel_pg::Error::CommitRolledBack))
            ));
            ::core::assert!(
                Lookup::maybe_by_id(client, 101).await.try_own()?.is_none()
            );
            let next = client.transaction().begin().await.map_err(own::Error::Query)?;
            ::core::assert_eq!(next.cancel_token().expect("backend key").pid(), pid);
            next.rollback().await.try_own()?;
            Ok::<(), own::Error<'_>>(())
        }))?;
        assert!(
            result.is_ok(),
            "rolled-back commit must preserve connection reuse"
        );
        Ok(())
    });
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn explicit_end() {
    fixtures::run(env::NAMES.map(var_os), Default::default(), |rt| {
        let client = rt.client();
        let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
            let tx = client
                .transaction()
                .begin()
                .await
                .map_err(own::Error::Query)?;
            Write::rename(&tx, 1, "explicit-saved".into())
                .await
                .try_own()?;
            let saved = Lookup::by_id(&tx, 1)
                .await
                .try_own()?;
            let name = cartel_pg::api::traits::Transaction::finish(
                tx,
                Ok::<_, Infallible>(saved.name.as_str()),
            )
            .await
            .ok()
            .expect("successful transaction finish preserves its value");
            ::core::assert_eq!(name, "explicit-saved");
            ::core::assert_eq!(
                Lookup::by_id(client, 1)
                    .await
                    .try_own()?
                    .name,
                "explicit-saved"
            );

            let tx = client
                .transaction()
                .begin()
                .await
                .map_err(own::Error::Query)?;
            Write::rename(&tx, 1, "explicit-rolled-back".into())
                .await
                .try_own()?;
            let operation = String::from("operation failed");
            ::core::assert!(matches!(
                tx.finish::<(), _>(Err(operation.as_str())).await,
                Err(FinishError::Operation(error)) if error == operation
            ));
            ::core::assert_eq!(
                Lookup::by_id(client, 1)
                    .await
                    .try_own()?
                    .name,
                "explicit-saved"
            );
            Ok::<(), own::Error<'_>>(())
        }))?;
        assert!(result.is_ok(), "explicit tx ends must preserve values");
        Ok(())
    });
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn ends() {
    fixtures::run(env::NAMES.map(var_os), Default::default(), |rt| {
        let client = rt.client();
        let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
            {
                let tx = client
                    .transaction()
                    .begin()
                    .await
                    .map_err(own::Error::Query)?;
                Write::rename(&tx, 1, "tx-renamed".into())
                    .await
                    .try_own()?;
                ::core::assert_eq!(
                    Lookup::by_id(&tx, 1)
                        .await
                        .try_own()?
                        .name,
                    "tx-renamed"
                );
                tx.rollback().await.map_err(own::Error::Query)?;
            }
            ::core::assert_eq!(
                Lookup::by_id(client, 1)
                    .await
                    .try_own()?
                    .name,
                "alice"
            );

            {
                let tx = client
                    .transaction()
                    .begin()
                    .await
                    .map_err(own::Error::Query)?;
                Write::rename(&tx, 1, "tx-saved".into())
                    .await
                    .try_own()?;
                tx.commit().await.map_err(own::Error::Query)?;
            }
            ::core::assert_eq!(
                Lookup::by_id(client, 1)
                    .await
                    .try_own()?
                    .name,
                "tx-saved"
            );
            Ok::<(), own::Error<'_>>(())
        }))?;
        assert!(result.is_ok(), "tx ends must preserve values");
        Ok(())
    });
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn routes_pin() {
    fixtures::run(env::NAMES.map(var_os), Default::default(), |rt| {
        let client = rt.client();
        let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
            let tx = client
                .transaction()
                .begin()
                .await
                .map_err(own::Error::Query)?;
            Write::rename(&tx, 1, "tx-only".into())
                .await
                .try_own()?;
            let outside = Write::rename(client, 1, "outside".into()).await;
            ::core::assert!(outside.as_ref().is_err_and(|error| ::core::matches!(
                error,
                reply::Error::Driver(cartel_pg::Error::Backpressure { .. })
            )));
            tx.rollback()
                .await
                .try_own()?;
            Write::rename(client, 1, "outside".into())
                .await
                .try_own()?;
            ::core::assert_eq!(
                Lookup::by_id(client, 1)
                    .await
                    .try_own()?
                    .name,
                "outside"
            );
            Ok::<(), own::Error<'_>>(())
        }))?;
        assert!(result.is_ok(), "tx routing must preserve pinning");
        Ok(())
    });
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn cancels_tx() {
    fixtures::run(env::NAMES.map(var_os), Default::default(), |rt| {
        let client = rt.client();
        let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
            ::core::assert!(
                fibers::TEST
                    .poll_once(client.transaction().begin())
                    .await
                    .is_none()
            );
            ::core::assert_eq!(
                Lookup::by_id(client, 1)
                    .await
                    .try_own()?
                    .name,
                "alice"
            );

            let tx = client
                .transaction()
                .begin()
                .await
                .map_err(own::Error::Query)?;
            Write::rename(&tx, 1, "cancelled-finalizer".into())
                .await
                .try_own()?;
            drop(tx.finish::<(), Infallible>(Ok(())));
            ::core::assert_eq!(
                Lookup::by_id(client, 1)
                    .await
                    .try_own()?
                    .name,
                "alice"
            );
            Ok::<(), own::Error<'_>>(())
        }))?;
        assert!(result.is_ok(), "cancelled transactions must roll back");
        Ok(())
    });
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn cancels_open() {
    fixtures::run(env::NAMES.map(var_os), Default::default(), |rt| {
        let client = rt.client();
        let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                let mut tx = client
                    .transaction()
                    .begin()
                    .await
                    .map_err(own::Error::Query)?;
                ::core::assert!(
                    fibers::TEST
                        .poll_once(tx.savepoint(Identifier::try_from(&mut String::from("cancel_open")).expect("identifier storage")))
                        .await
                        .is_none()
                );
                ::core::assert_eq!(
                    Lookup::by_id(client, 1)
                        .await
                        .try_own()?
                        .name,
                    "alice"
                );
                drop(tx);
                Ok::<(), own::Error<'_>>(())
            }))?;
        assert!(
            result.is_ok(),
            "cancelled save opens must release the route"
        );
        Ok(())
    });
}
