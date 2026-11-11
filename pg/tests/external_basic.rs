use cartel_pg::api::traits::Stream as _;
use common::env::{self, Env};
use common::{external::runtime::check, recover};
use dope_fiber::BlockOn as _;
use std::env::var_os;
use std::{thread, time::Duration};

use crate::common::external::runtime::Plan;
use cartel_pg::reply;
use o3::base::time::Positive;

use crate::common::external::{
    own,
    own::Convert as _,
    user::{Lookup, Write},
};

pub mod common;

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn dispatch_kinds() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_run(
            |rt| {
                let client = rt.client();
                let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                    let u = Lookup::by_id(client, 1)
                        .await
                        .try_own()?;
                    ::core::assert_eq!(u.id, 1);
                    ::core::assert_eq!(u.name, "alice");
                    ::core::assert_eq!(u.age, 30);
                    ::core::assert!((u.score - 9.5).abs() < 1e-9);
                    ::core::assert!(u.active);
                    ::core::assert_eq!(u.nickname.as_deref(), Some("al"));
                    ::core::assert!(u.avatar.is_none());

                    ::core::assert!(::core::matches!(
                        Lookup::by_id(client, 999).await,
                        Err(reply::Error::Driver(cartel_pg::Error::NotFound))
                    ));

                    ::core::assert!(
                        Lookup::maybe_by_id(client, 999)
                            .await
                            .try_own()?
                            .is_none()
                    );
                    ::core::assert!(
                        Lookup::maybe_by_id(client, 2)
                            .await
                            .try_own()?
                            .is_some()
                    );

                    let all = Lookup::all_rows(client)
                        .await
                        .try_own()?;
                    ::core::assert_eq!(all.len(), 5);

                    let names = Lookup::names_min_id(client, 3)
                        .await
                        .try_own()?;
                    ::core::assert_eq!(names, ::std::vec!["carol", "dave", "한국어 √"]);

                    let pairs = Lookup::id_names(client)
                        .await
                        .try_own()?;
                    ::core::assert_eq!(pairs.len(), 5);
                    ::core::assert_eq!(pairs[0], (1, "alice".to_string()));

                    ::core::assert_eq!(
                        Lookup::id_plus_one(client, 3)
                            .await
                            .try_own()?,
                        4
                    );
                    Ok::<(), own::Error<'_>>(())
                }))?;
                assert!(result.is_ok(), "query dispatch must preserve rows");
                let mut stream = Lookup::all_stream(client);
                let mut streamed = Vec::new();
                let result = (|| -> Result<(), own::Error<'_>> {
                    while let Some(user) = rt
                        .runtime
                        .block_on(stream.next_row())
                        .expect("runtime drive")
                        .map_err(own::Error::Query)?
                    {
                        streamed.push(user.into_owned().map_err(own::Error::Decode)?);
                    }
                    Ok(())
                })();
                assert!(result.is_ok(), "seeded user stream rows must decode");
                ::core::assert_eq!(streamed.len(), 5);
                ::core::assert_eq!(streamed[0].id, 1);
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn row_stream_deadline() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_window(
            Positive::from_millis::<100>(),
            |rt| {
                let client = rt.client();
                let mut stream = Lookup::all_stream(client);
                let first = rt.runtime.block_on(stream.next_row())?;
                ::core::assert!(matches!(first, Ok(Some(_))), "first row must arrive");
                thread::sleep(Duration::from_millis(150));
                ::core::assert!(
                    ::core::matches!(
                        rt.runtime.block_on(stream.next_row())?,
                        Err(reply::Error::Driver(cartel_pg::Error::RequestTimeout(timeout)))
                            if timeout.phase() == cartel_pg::RequestPhase::Response
                    ),
                    "stream deadline must not rearm per row"
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
fn type_roundtrips_basic() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_run(
            |rt| {
                let client = rt.client();
                let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                    let row = Lookup::blob(client, 3)
                        .await
                        .try_own()?;
                    ::core::assert_eq!(row, ::std::vec![Some(::std::vec![0xcau8, 0xfe])]);
                    let row = Lookup::blob(client, 4)
                        .await
                        .try_own()?;
                    ::core::assert_eq!(
                        row,
                        ::std::vec![Some(::std::vec![0xdeu8, 0xad, 0xbe, 0xef])]
                    );
                    let row = Lookup::blob(client, 1)
                        .await
                        .try_own()?;
                    ::core::assert_eq!(row, ::std::vec![None]);
                    ::core::assert_eq!(
                        Lookup::unicode_name(client)
                            .await
                            .try_own()?,
                        "한국어 √"
                    );
                    Ok::<(), own::Error<'_>>(())
                }))?;
                assert!(result.is_ok(), "basic values must roundtrip");
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn update_and_delete() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_run(
            |rt| {
                let client = rt.client();
                let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                    Write::rename(client, 2, "robert".into())
                        .await
                        .try_own()?;
                    ::core::assert_eq!(
                        Lookup::by_id(client, 2)
                            .await
                            .try_own()?
                            .name,
                        "robert"
                    );

                    Write::set_fields(client, 2, "bobby".into(), 26, 7.5)
                        .await
                        .try_own()?;
                    let u = Lookup::by_id(client, 2)
                        .await
                        .try_own()?;
                    ::core::assert_eq!(u.name, "bobby");
                    ::core::assert_eq!(u.age, 26);
                    ::core::assert!((u.score - 7.5).abs() < 1e-9);

                    Write::remove(client, 4)
                        .await
                        .try_own()?;
                    ::core::assert!(
                        Lookup::maybe_by_id(client, 4)
                            .await
                            .try_own()?
                            .is_none()
                    );

                    Write::clear_age(client, 1)
                        .await
                        .try_own()?;
                    ::core::assert_eq!(
                        Lookup::by_id(client, 1)
                            .await
                            .try_own()?
                            .age,
                        0
                    );
                    Ok::<(), own::Error<'_>>(())
                }))?;
                assert!(result.is_ok(), "updates and deletes must match");
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn insert_variants() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_run(
            |rt| {
                let client = rt.client();
                let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                    Write::add(client, 100, "ned".into(), 50, 9.0, true)
                        .await
                        .try_own()?;
                    ::core::assert_eq!(
                        Lookup::by_id(client, 100)
                            .await
                            .try_own()?
                            .name,
                        "ned"
                    );

                    let rows = Write::add_all(client, 101, "olive".into(), 0, 0.0, true)
                        .await
                        .try_own()?;
                    ::core::assert_eq!(rows.len(), 1);
                    ::core::assert_eq!(rows[0].id, 101);
                    ::core::assert_eq!(rows[0].name, "olive");

                    let row = Write::add_one(client, 102, "pete".into(), 0, 0.0, true)
                        .await
                        .try_own()?;
                    ::core::assert_eq!(row.id, 102);

                    let row = Write::add_first(client, 103, "quinn".into(), 0, 0.0, true)
                        .await
                        .try_own()?;
                    ::core::assert_eq!(
                        row.expect("insert returning first must yield the inserted row")
                            .id,
                        103
                    );

                    Write::copy_min_age(client, 30, 1000)
                        .await
                        .try_own()?;
                    let mut names = Vec::new();
                    if let Some(user) = Lookup::maybe_by_id(client, 1001)
                        .await
                        .try_own()?
                    {
                        names.push(user.name);
                    }
                    if let Some(user) = Lookup::maybe_by_id(client, 1002)
                        .await
                        .try_own()?
                    {
                        names.push(user.name);
                    }
                    if let Some(user) = Lookup::maybe_by_id(client, 1003)
                        .await
                        .try_own()?
                    {
                        names.push(user.name);
                    }
                    if let Some(user) = Lookup::maybe_by_id(client, 1004)
                        .await
                        .try_own()?
                    {
                        names.push(user.name);
                    }
                    if let Some(user) = Lookup::maybe_by_id(client, 1005)
                        .await
                        .try_own()?
                    {
                        names.push(user.name);
                    }
                    ::core::assert!(names.contains(&"alice".to_string()));
                    ::core::assert!(names.contains(&"carol".to_string()));

                    Write::add_literal(client, 200, "ulysses".into())
                        .await
                        .try_own()?;
                    let u = Lookup::by_id(client, 200)
                        .await
                        .try_own()?;
                    ::core::assert_eq!(u.age, 7);
                    ::core::assert!((u.score - 1.25).abs() < 1e-9);
                    Ok::<(), own::Error<'_>>(())
                }))?;
                assert!(result.is_ok(), "insert variants must preserve values");
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn insert_on_conflict() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_run(
            |rt| {
                let client = rt.client();
                let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                    Write::add_or_skip(client, 1, "ignored".into(), 0, 0.0, true)
                        .await
                        .try_own()?;
                    ::core::assert_eq!(
                        Lookup::by_id(client, 1)
                            .await
                            .try_own()?
                            .name,
                        "alice"
                    );

                    Write::upsert_name(
                        client,
                        1,
                        "ignored".into(),
                        "ALICE".into(),
                        0,
                        0.0,
                        true,
                    )
                    .await
                    .try_own()?;
                    ::core::assert_eq!(
                        Lookup::by_id(client, 1)
                            .await
                            .try_own()?
                            .name,
                        "ALICE"
                    );

                    Write::upsert_name(
                        client,
                        222,
                        "new".into(),
                        "new-upd".into(),
                        0,
                        0.0,
                        true,
                    )
                    .await
                    .try_own()?;
                    ::core::assert_eq!(
                        Lookup::by_id(client, 222)
                            .await
                            .try_own()?
                            .name,
                        "new"
                    );

                    Write::add_id_skip(client, 1, "still-ignored".into(), 0, 0.0, true)
                        .await
                        .try_own()?;
                    ::core::assert_eq!(
                        Lookup::by_id(client, 1)
                            .await
                            .try_own()?
                            .name,
                        "ALICE"
                    );
                    Ok::<(), own::Error<'_>>(())
                }))?;
                assert!(result.is_ok(), "conflict policies must preserve values");
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn returning_clauses() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_run(
            |rt| {
                let client = rt.client();
                let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                    let u = Write::rename_row(client, 1, "AAA".into())
                        .await
                        .try_own()?;
                    ::core::assert_eq!(u.name, "AAA");

                    let u = Write::take(client, 2)
                        .await
                        .try_own()?
                        .expect("deleting seeded user 2 must return that row");
                    ::core::assert_eq!(u.name, "bob");

                    let removed = Write::take_idle(client)
                        .await
                        .try_own()?;
                    ::core::assert_eq!(removed.len(), 1);
                    ::core::assert_eq!(removed[0].id, 3);
                    Ok::<(), own::Error<'_>>(())
                }))?;
                assert!(result.is_ok(), "returning clauses must preserve rows");
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}
