use crate::common::database::IntegrationDatabase;
use crate::common::{assert_reclaimed, assert_retirement, plan};
use cartel_sqlite::runtime::{self, Runtime};
use common::{query::relation::*, schema};
use dope_core::driver::life::recovery::Policy;
use dope_test::fibers;
use std::sync::OnceLock;

pub mod common;

static RUNTIME: OnceLock<Result<Runtime, runtime::Error>> = OnceLock::new();

#[test]
fn joins() -> Result<(), plan::Error> {
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
                        let rows = user_with_posts(&mut conn).await?;
                        ::core::assert_eq!(rows.len(), 4);

                        let n = user_left_join_posts(&mut conn).await?;
                        ::core::assert!(n >= 5);

                        let rows3 = user_post_comment(&mut conn).await?;
                        ::core::assert_eq!(rows3.len(), 3);

                        let rows4 = user_post_comment_tag(&mut conn).await?;
                        ::core::assert!(!rows4.is_empty());

                        ::core::assert_eq!(count_user_posts(&mut conn).await?, 4);

                        let titles = post_titles_by_user(&mut conn, 1).await?;
                        ::core::assert_eq!(titles.as_slice(), &["hello", "second"]);

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
fn set_ops() -> Result<(), plan::Error> {
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
                        let r = users_union(&mut conn).await?;
                        ::core::assert_eq!(r.len(), 3);
                        let ids: Vec<i64> = r.iter().map(|u| u.id).collect();
                        ::core::assert_eq!(ids, vec![1, 4, 5]);

                        let r = users_union_all(&mut conn).await?;
                        ::core::assert_eq!(r.len(), 2);

                        let r = users_intersect(&mut conn).await?;
                        ::core::assert_eq!(r.len(), 3);

                        let r = users_except(&mut conn).await?;
                        ::core::assert_eq!(r.len(), 4);

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
fn ctes() -> Result<(), plan::Error> {
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
                        let r = cte_active_users(&mut conn).await?;
                        let ids: Vec<i64> = r.iter().map(|u| u.id).collect();
                        ::core::assert_eq!(ids, vec![1, 2, 5]);

                        let r = cte_union(&mut conn, 22).await?;
                        let ids: Vec<i64> = r.iter().map(|u| u.id).collect();
                        ::core::assert!(ids.contains(&2));
                        ::core::assert!(ids.contains(&4));
                        ::core::assert!(ids.contains(&5));

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
fn subqueries() -> Result<(), plan::Error> {
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
        let r = users_with_any_post(&mut conn).await?;
        let ids: Vec<i64> = r.iter().map(|u| u.id).collect();
        ::core::assert_eq!(ids, vec![1, 2, 3]);

        let r = users_without_post(&mut conn).await?;
        let ids: Vec<i64> = r.iter().map(|u| u.id).collect();
        ::core::assert_eq!(ids, vec![4, 5]);

        let counts = user_post_counts(&mut conn).await?;
        ::core::assert_eq!(counts.as_slice(), &[(1, 2), (2, 1), (3, 1), (4, 0), (5, 0)]);

                Ok::<(), cartel_sqlite::Error>(())
            })).map_err(plan::Error::Run)?.map_err(plan::Error::Call)
            },
            Policy::new(assert_reclaimed, assert_retirement),
            |result| result.expect("SQLite scope"),
        );
        result.expect("SQLite integration scope")
    }
}
