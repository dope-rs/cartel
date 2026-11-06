use crate::common::external::runtime::Session;
use crate::{
    common::external::runtime::fixtures::try_suffix,
    common::{external::Db, port::Queries},
};
use cartel_pg::api::traits::{Client as _, Status as _, Unchecked as _};
use common::env::{self, Env};
use common::{external::runtime::check, recover};
use dope_fiber::BlockOn as _;
use dope_test::fibers;
use std::env::var_os;
use std::num::NonZero;

pub mod common;

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn deferred_commit_fails() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Session::<Queries>::new(
        input.address(),
        input.try_config().expect("PostgreSQL credentials"),
        NonZero::<usize>::MIN,
        "public".to_owned(),
        false,
    )
    .try_run(|rt| {
        let client = rt.client();
        let ready = rt.runtime.block_on(client.status().wait_ready())?;
        assert!(
            ready.is_ok(),
            "connection ready: {:?}",
            ready.as_ref().err().map(cartel_pg::reply::Error::kind),
        );
        let created = rt.runtime.block_on(client.unchecked().command(
            "CREATE TEMP TABLE cartel_pg_deferred (id integer UNIQUE DEFERRABLE INITIALLY DEFERRED)",
        ))?;
        assert!(
            created.is_ok(),
            "temporary table: {:?}",
            created.as_ref().err().map(cartel_pg::reply::Error::kind),
        );

        let conflict = rt.runtime.block_on(client.unchecked().command(
            "INSERT INTO cartel_pg_deferred VALUES (1), (1)",
        ))?;
        assert_eq!(
            conflict
                .as_ref()
                .err()
                .and_then(|error| error.as_server())
                .map(|error| error.fields().code().as_str()),
            Some("23505"),
            "Sync must report its deferred constraint failure: {:?}",
            conflict.as_ref().err().map(cartel_pg::reply::Error::kind),
        );
        drop(conflict);

        let reused = rt.runtime.block_on(client.unchecked().command(
            "INSERT INTO cartel_pg_deferred VALUES (1)",
        ))?;
        assert!(
            reused.is_ok(),
            "the same session remains usable: {:?}",
            reused.as_ref().err().map(cartel_pg::reply::Error::kind),
        );
        Ok(())
    }, recover, check).expect("PostgreSQL runtime");
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn failure_stops_retry() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    let schema = ::std::format!(
        "cartel_pg_missing_{}",
        try_suffix().expect("fixture suffix")
    );
    Session::<Db>::new(
        input.address(),
        input.try_config().expect("PostgreSQL credentials"),
        NonZero::<usize>::MIN.saturating_add(1),
        schema,
        false,
    )
    .try_run(
        |rt| {
            let client = rt.client();
            let ready = fibers::TEST
                .try_run(&mut rt.runtime, client.status().wait_ready())
                .expect("PostgreSQL startup driver");
            assert!(
                client.status().is_failed(),
                "pool with missing schema must reach is_failed(): {:?}",
                ready.as_ref().err().map(common::port::Reply::from),
            );
            Ok(())
        },
        recover,
        check,
    )
    .expect("PostgreSQL runtime");
}
