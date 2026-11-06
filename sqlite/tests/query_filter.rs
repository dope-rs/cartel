use crate::common::database::IntegrationDatabase;
use crate::common::{assert_reclaimed, assert_retirement, plan};
use cartel_sqlite::runtime::{self, Runtime};
use cartel_sqlite::{Catalog as _, Error};
use common::{query::filter::*, schema};
use dope_core::driver::life::recovery::Policy;
use dope_test::fibers;

pub mod common;

#[test]
fn predicates() -> Result<(), plan::Error> {
    // SAFETY: this binary has one test; only Cartel uses installed SQLite.
    // Core handles and allocations remain exclusively owned by their originating worker.
    let runtime = Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
        .map_err(plan::Error::Runtime)?;
    {
        let (limits, memory) = schema::try_settings()?;
        let plan =
            plan::Plan::<IntegrationDatabase>::try_new(schema::open(runtime)?, limits, memory)?;
        let result =
            plan.try_run(
                |driver, local| {
                    fibers::TEST.try_run(driver, dope_fiber::fiber!('_ => async move {

        let mut conn = local;
        ::core::assert_eq!(users_eq_age(&mut conn, 25).await?.len(), 1);
        ::core::assert_eq!(users_ne_age(&mut conn, 25).await?.len(), 4);
        ::core::assert_eq!(users_gt_age(&mut conn, 30).await?.len(), 1);
        ::core::assert_eq!(users_lt_age(&mut conn, 30).await?.len(), 3);
        ::core::assert_eq!(users_ge_age(&mut conn, 30).await?.len(), 2);
        ::core::assert_eq!(users_le_age(&mut conn, 30).await?.len(), 4);
        ::core::assert_eq!(users_like(&mut conn, "a%").await?.len(), 1);
        ::core::assert_eq!(users_not_like(&mut conn, "a%").await?.len(), 4);
        ::core::assert_eq!(users_glob(&mut conn, "[ab]*").await?.len(), 2);
        ::core::assert_eq!(users_nickname_some(&mut conn).await?.len(), 3);
        ::core::assert_eq!(users_nickname_none(&mut conn).await?.len(), 2);
        ::core::assert_eq!(users_and(&mut conn, 25, true).await?.len(), 3);
        ::core::assert_eq!(users_or(&mut conn, 22, 40).await?.len(), 2);
        ::core::assert_eq!(users_nested(&mut conn, 25, 35, true).await?.len(), 4);
        ::core::assert_eq!(users_not_via_ne(&mut conn, true).await?.len(), 1);

                Ok::<(), cartel_sqlite::Error>(())
            })).map_err(plan::Error::Run)?.map_err(plan::Error::Call)
                },
                Policy::new(assert_reclaimed, assert_retirement),
                |result| result.expect("SQLite scope"),
            );
        result.expect("SQLite integration scope")?;
    }
    ordering_and_limits(runtime)?;
    aggregates(runtime)
}

fn ordering_and_limits(runtime: Runtime) -> Result<(), plan::Error> {
    let sql = format!(
        "SELECT {} FROM \"users\" AS q0_0 ORDER BY q0_0.\"id\" LIMIT -1 OFFSET ?1",
        schema::User::__CARTEL_SELECT_COLS
    );
    assert!(
        IntegrationDatabase::STATEMENTS
            .iter()
            .any(|entry| entry.sql() == sql)
    );
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
                        let ids = users_ordered_desc(&mut conn).await?;
                        ::core::assert_eq!(ids.as_slice(), &[3, 1, 5, 2, 4]);
                        ::core::assert_eq!(users_limited(&mut conn).await?.as_slice(), &[1, 2]);
                        ::core::assert_eq!(users_offset(&mut conn).await?.as_slice(), &[3, 4]);
                        let ages = distinct_ages(&mut conn).await?;
                        ::core::assert_eq!(ages.as_slice(), &[22, 25, 29, 30, 40]);
                        ::core::assert_eq!(users_limit_param(&mut conn, 3).await?.len(), 3);
                        ::core::assert_eq!(users_offset_param(&mut conn, 2).await?.len(), 3);
                        ::core::assert!(::core::matches!(
                            users_limit_param(&mut conn, -1).await,
                            Err(Error::NegativeLimit { value: -1 })
                        ));
                        ::core::assert!(::core::matches!(
                            users_offset_param(&mut conn, -1).await,
                            Err(Error::NegativeOffset { value: -1 })
                        ));

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

fn aggregates(runtime: Runtime) -> Result<(), plan::Error> {
    {
        let (limits, memory) = schema::try_settings()?;
        let plan =
            plan::Plan::<IntegrationDatabase>::try_new(schema::open(runtime)?, limits, memory)?;
        let result = plan.try_run(
            |driver, local| {
                fibers::TEST.try_run(driver, dope_fiber::fiber!('_ => async move {

        let mut conn = local;
        ::core::assert_eq!(count_all(&mut conn).await?, 5);
        ::core::assert_eq!(count_active(&mut conn, true).await?, 4);
        ::core::assert_eq!(sum_ages(&mut conn).await?, Some(30 + 25 + 40 + 22 + 29));
        let avg = (avg_age(&mut conn).await?).expect("users were empty");
        ::core::assert!((avg - (30.0 + 25.0 + 40.0 + 22.0 + 29.0) / 5.0).abs() < 1e-9);
        ::core::assert_eq!(min_age(&mut conn).await?, Some(22));
        let m = (max_score(&mut conn).await?).expect("users were empty");
        ::core::assert!((m - 9.5).abs() < 1e-9);
        ::core::assert_eq!(
            widgets_count_per_bucket(&mut conn).await?.as_slice(),
            &[(1, 2), (2, 2), (3, 1)]
        );
        let sums = widgets_sum_per_bucket(&mut conn).await?;
        ::core::assert_eq!(
            sums.as_slice(),
            &[(1, Some(30)), (2, Some(70)), (3, Some(50))]
        );
        ::core::assert_eq!(
            widgets_having(&mut conn, 2).await?.as_slice(),
            &[(1, 2), (2, 2)]
        );

                Ok::<(), cartel_sqlite::Error>(())
            })).map_err(plan::Error::Run)?.map_err(plan::Error::Call)
            },
            Policy::new(assert_reclaimed, assert_retirement),
            |result| result.expect("SQLite scope"),
        );
        result.expect("SQLite integration scope")
    }
}
