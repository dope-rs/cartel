use crate::common::{assert_reclaimed, assert_retirement, plan};
use crate::{
    common::database::IntegrationDatabase, common::query::dispatch::keyword_value,
    common::query::dispatch::maybe_user_by_id, common::query::dispatch::user_by_id,
};
use cartel_sqlite::runtime::{self, Runtime};
use common::{query::mutation::*, schema};
use dope_core::driver::life::recovery::Policy;
use dope_test::fibers;
use std::sync::OnceLock;

pub mod common;

static RUNTIME: OnceLock<Result<Runtime, runtime::Error>> = OnceLock::new();

#[test]
fn update_and_delete() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: no SQLite call can pass this binary's initializer before it returns.
        // Only Cartel uses installed SQLite; Core handles and allocations remain
        // exclusively owned by their originating worker.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    {
        let (limits, memory) = schema::try_settings()?;
        let plan =
            plan::Plan::<IntegrationDatabase>::try_new(schema::open(runtime)?, limits, memory)?;
        let result = plan.try_run(
            |driver, local| {
                fibers::TEST
                    .try_run(
                        driver,
                        dope_fiber::fiber!('_ => async move {

                        let mut conn = local;
                        rename_user(&mut conn, 2, "robert").await?;
                        ::core::assert_eq!(user_by_id(&mut conn, 2).await?.name, "robert");

                        set_fields(&mut conn, 2, "bobby", 26, 7.5).await?;
                        let u = user_by_id(&mut conn, 2).await?;
                        ::core::assert_eq!(u.name, "bobby");
                        ::core::assert_eq!(u.age, 26);
                        ::core::assert_eq!(u.score, 7.5);

                        delete_user(&mut conn, 4).await?;
                        ::core::assert!(maybe_user_by_id(&mut conn, 4).await?.is_none());

                        insert_keyword(&mut conn, 8).await?;
                        ::core::assert_eq!(keyword_value(&mut conn, 8).await?, Some(8));
                        rewrite_keyword(&mut conn, 8, 9).await?;
                        ::core::assert_eq!(keyword_value(&mut conn, 9).await?, Some(9));
                        upsert_keyword(&mut conn, 9, 10).await?;
                        ::core::assert_eq!(keyword_value(&mut conn, 10).await?, Some(10));

                                Ok::<(), cartel_sqlite::Error>(())
                            }),
                    )
                    .map_err(plan::Error::Run)?
                    .map_err(plan::Error::Call)
            },
            Policy::new(assert_reclaimed, assert_retirement),
            |result| result.expect("SQLite scope"),
        );
        result.expect("SQLite integration scope")
    }
}

#[test]
fn insert_variants() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: no SQLite call can pass this binary's initializer before it returns.
        // Only Cartel uses installed SQLite; Core handles and allocations remain
        // exclusively owned by their originating worker.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    {
        let (limits, memory) = schema::try_settings()?;
        let plan =
            plan::Plan::<IntegrationDatabase>::try_new(schema::open(runtime)?, limits, memory)?;
        let result = plan.try_run(
            |driver, local| {
                fibers::TEST
                    .try_run(
                        driver,
                        dope_fiber::fiber!('_ => async move {

                        let mut conn = local;
                        insert_user(&mut conn, 100, "ned", 50, 9.0, true).await?;
                        let u = user_by_id(&mut conn, 100).await?;
                        ::core::assert_eq!(u.name, "ned");

                        let rows = insert_all(&mut conn, 101, "olive", 0, 0.0, true).await?;
                        ::core::assert_eq!(rows.len(), 1);
                        ::core::assert_eq!(rows[0].id, 101);
                        ::core::assert_eq!(rows[0].name, "olive");

                        let row = insert_one(&mut conn, 102, "pete", 0, 0.0, true).await?;
                        ::core::assert_eq!(row.id, 102);

                        let row = insert_first(&mut conn, 103, "quinn", 0, 0.0, true).await?;
                        let row = (row).expect("insert returned no row");
                        ::core::assert_eq!(row.id, 103);

                        copy_older(&mut conn, 30, 1000).await?;
                        let mut names = Vec::new();
                        for id in 1001..=1005 {
                            if let Some(user) = maybe_user_by_id(&mut conn, id).await? {
                                names.push(user.name.to_owned());
                            }
                        }
                        ::core::assert!(names.contains(&"alice".to_string()));
                        ::core::assert!(names.contains(&"carol".to_string()));

                                Ok::<(), cartel_sqlite::Error>(())
                            }),
                    )
                    .map_err(plan::Error::Run)?
                    .map_err(plan::Error::Call)
            },
            Policy::new(assert_reclaimed, assert_retirement),
            |result| result.expect("SQLite scope"),
        );
        result.expect("SQLite integration scope")
    }
}

#[test]
fn insert_on_conflict() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: no SQLite call can pass this binary's initializer before it returns.
        // Only Cartel uses installed SQLite; Core handles and allocations remain
        // exclusively owned by their originating worker.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    {
        let (limits, memory) = schema::try_settings()?;
        let plan =
            plan::Plan::<IntegrationDatabase>::try_new(schema::open(runtime)?, limits, memory)?;
        let result = plan.try_run(
            |driver, local| {
                fibers::TEST
                    .try_run(
                        driver,
                        dope_fiber::fiber!('_ => async move {

                        let mut conn = local;
                        insert_or_ignore(&mut conn, 1, "ignored", 0, 0.0, true).await?;
                        ::core::assert_eq!(user_by_id(&mut conn, 1).await?.name, "alice");

                        upsert_name(&mut conn, 1, "ignored", "ALICE", 0, 0.0, true).await?;
                        ::core::assert_eq!(user_by_id(&mut conn, 1).await?.name, "ALICE");

                        upsert_name(&mut conn, 200, "new", "new-upd", 0, 0.0, true).await?;
                        ::core::assert_eq!(user_by_id(&mut conn, 200).await?.name, "new");

                        insert_or_keep(&mut conn, 1, "still-ignored", 0, 0.0, true).await?;
                        ::core::assert_eq!(user_by_id(&mut conn, 1).await?.name, "ALICE");

                        ::core::assert_eq!(upsert_correlated(&mut conn, 2).await?.value, 1);

                                Ok::<(), cartel_sqlite::Error>(())
                            }),
                    )
                    .map_err(plan::Error::Run)?
                    .map_err(plan::Error::Call)
            },
            Policy::new(assert_reclaimed, assert_retirement),
            |result| result.expect("SQLite scope"),
        );
        result.expect("SQLite integration scope")
    }
}

#[test]
fn returning_clauses() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: no SQLite call can pass this binary's initializer before it returns.
        // Only Cartel uses installed SQLite; Core handles and allocations remain
        // exclusively owned by their originating worker.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    {
        let (limits, memory) = schema::try_settings()?;
        let plan =
            plan::Plan::<IntegrationDatabase>::try_new(schema::open(runtime)?, limits, memory)?;
        let result = plan.try_run(
            |driver, local| {
                fibers::TEST.try_run(driver, dope_fiber::fiber!('_ => async move {

        let mut conn = local;
        let u = update_one(&mut conn, 1, "AAA").await?;
        ::core::assert_eq!(u.name, "AAA");

        let u = (delete_one(&mut conn, 2).await?).expect("delete returned no matching user");
        ::core::assert_eq!(u.name, "bob");

        let removed = delete_inactive(&mut conn).await?;
        ::core::assert_eq!(removed.len(), 1);
        ::core::assert_eq!(removed[0].id, 3);

                Ok::<(), cartel_sqlite::Error>(())
            })).map_err(plan::Error::Run)?.map_err(plan::Error::Call)
            },
            Policy::new(assert_reclaimed, assert_retirement),
            |result| result.expect("SQLite scope"),
        );
        result.expect("SQLite integration scope")
    }
}
