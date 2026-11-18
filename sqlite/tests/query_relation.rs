use crate::common::plan;
use cartel_sqlite::runtime::{self, Runtime};
use common::{query::relation::*, schema};
use dope_test::fibers;

pub mod common;

#[test]
fn joins() -> Result<(), plan::Error> {
    // SAFETY: this binary has one test; only Cartel uses installed SQLite.
    // Core handles and allocations remain exclusively owned by their originating worker.
    let runtime = Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
        .map_err(plan::Error::Runtime)?;
    schema::run(runtime, |driver, mut conn| {
        fibers::TEST.try_run(
            driver,
            dope_fiber::fiber!('_ => async move {
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
                ::core::assert_eq!(titles, &["hello", "second"]);
                Ok(())
            }),
        )
    })?;
    set_ops(runtime)?;
    ctes(runtime)?;
    subqueries(runtime)
}

fn set_ops(runtime: Runtime) -> Result<(), plan::Error> {
    schema::run(runtime, |driver, mut conn| {
        fibers::TEST.try_run(
            driver,
            dope_fiber::fiber!('_ => async move {
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
                Ok(())
            }),
        )
    })
}

fn ctes(runtime: Runtime) -> Result<(), plan::Error> {
    schema::run(runtime, |driver, mut conn| {
        fibers::TEST.try_run(
            driver,
            dope_fiber::fiber!('_ => async move {
                let r = cte_active_users(&mut conn).await?;
                let ids: Vec<i64> = r.iter().map(|u| u.id).collect();
                ::core::assert_eq!(ids, vec![1, 2, 5]);

                let r = cte_union(&mut conn, 22).await?;
                ::core::assert!(r.iter().map(|u| u.id).eq([1, 2, 4, 5]));
                Ok(())
            }),
        )
    })
}

fn subqueries(runtime: Runtime) -> Result<(), plan::Error> {
    schema::run(runtime, |driver, mut conn| {
        fibers::TEST.try_run(driver, dope_fiber::fiber!('_ => async move {
            let r = users_with_any_post(&mut conn).await?;
            let ids: Vec<i64> = r.iter().map(|u| u.id).collect();
            ::core::assert_eq!(ids, vec![1, 2, 3]);

            let r = users_without_post(&mut conn).await?;
            let ids: Vec<i64> = r.iter().map(|u| u.id).collect();
            ::core::assert_eq!(ids, vec![4, 5]);

            let counts = user_post_counts(&mut conn).await?;
            ::core::assert_eq!(
                counts,
                &[(1, 2, Some(11)), (2, 1, Some(12)), (3, 1, Some(13)), (4, 0, None), (5, 0, None)]
            );
            Ok(())
        }))
    })
}
