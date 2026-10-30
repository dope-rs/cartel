use crate::common::external::runtime::Plan;
use crate::{
    common::external::own, common::external::own::Convert as _, common::external::reports::Combine,
    common::external::row::Post, common::external::row::Reports, common::external::search::Pattern,
    common::external::user::Compare,
};
use cartel_pg::api::traits::Client as _;
use common::env::{self, Env};
use common::{external::runtime::check, recover};
use dope_fiber::BlockOn as _;
use std::env::var_os;

pub mod common;

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn predicates() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_run(
            |rt| {
                let client = rt.client();
                let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                    assert_eq!(
                        Compare::eq_age(client.query(), 25)
                            .await
                            .try_own()?
                            .len(),
                        1
                    );
                    assert_eq!(
                        Compare::ne_age(client.query(), 25)
                            .await
                            .try_own()?
                            .len(),
                        4
                    );
                    assert_eq!(
                        Compare::gt_age(client.query(), 30)
                            .await
                            .try_own()?
                            .len(),
                        1
                    );
                    assert_eq!(
                        Compare::lt_age(client.query(), 30)
                            .await
                            .try_own()?
                            .len(),
                        3
                    );
                    assert_eq!(
                        Compare::ge_age(client.query(), 30)
                            .await
                            .try_own()?
                            .len(),
                        2
                    );
                    assert_eq!(
                        Compare::le_age(client.query(), 30)
                            .await
                            .try_own()?
                            .len(),
                        4
                    );
                    assert_eq!(
                        Pattern::like(client.query(), "a%".into())
                            .await
                            .try_own()?
                            .len(),
                        1
                    );
                    assert_eq!(
                        Pattern::not_like(client.query(), "a%".into())
                            .await
                            .try_own()?
                            .len(),
                        4
                    );
                    assert_eq!(
                        Pattern::ilike(client.query(), "A%".into())
                            .await
                            .try_own()?
                            .len(),
                        1
                    );
                    assert_eq!(
                        Pattern::not_ilike(client.query(), "A%".into())
                            .await
                            .try_own()?
                            .len(),
                        4
                    );
                    assert_eq!(
                        Pattern::regex(client.query(), "^[ab]".into())
                            .await
                            .try_own()?
                            .len(),
                        2
                    );
                    assert_eq!(
                        Pattern::regex_i(client.query(), "^[AB]".into())
                            .await
                            .try_own()?
                            .len(),
                        2
                    );
                    assert_eq!(
                        Pattern::not_regex(client.query(), "^[ab]".into())
                            .await
                            .try_own()?
                            .len(),
                        3
                    );
                    assert_eq!(
                        Pattern::not_regex_i(client.query(), "^[AB]".into())
                            .await
                            .try_own()?
                            .len(),
                        3
                    );
                    assert_eq!(
                        Compare::with_nick(client.query())
                            .await
                            .try_own()?
                            .len(),
                        3
                    );
                    assert_eq!(
                        Compare::no_nick(client.query())
                            .await
                            .try_own()?
                            .len(),
                        2
                    );
                    assert_eq!(
                        Compare::active_from(client.query(), 25, true)
                            .await
                            .try_own()?
                            .len(),
                        3
                    );
                    assert_eq!(
                        Compare::age_or(client.query(), 22, 40)
                            .await
                            .try_own()?
                            .len(),
                        2
                    );
                    assert_eq!(
                        Compare::nested(client.query(), 25, 35, true)
                            .await
                            .try_own()?
                            .len(),
                        4
                    );
                    assert_eq!(
                        Compare::not_via_ne(client.query(), true)
                            .await
                            .try_own()?
                            .len(),
                        1
                    );

                    let ids = vec![1i64, 3, 5];
                    let r = Compare::in_ids(client.query(), &ids)
                        .await
                        .try_own()?;
                    assert_eq!(
                        r.iter().map(|u| u.id).collect::<Vec<_>>(),
                        vec![1, 3, 5]
                    );
                    let r = Compare::not_in_ids(client.query(), &ids)
                        .await
                        .try_own()?;
                    assert_eq!(
                        r.iter().map(|u| u.id).collect::<Vec<_>>(),
                        vec![2, 4]
                    );
                    Ok::<(), own::Error<'_>>(())
                }))?;
                assert!(result.is_ok(), "query predicates must select expected rows");
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn joins() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_run(
            |rt| {
                let client = rt.client();
                let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                    let rows = Reports::inner_posts(client.query())
                        .await
                        .try_own()?;
                    assert_eq!(rows.len(), 4);

                    let n = Reports::left_n(client.query())
                        .await
                        .try_own()?;
                    assert!(n >= 5);

                    let rows3 = Reports::comments(client.query())
                        .await
                        .try_own()?;
                    assert_eq!(rows3.len(), 3);

                    let rows4 = Reports::comment_tags(client.query())
                        .await
                        .try_own()?;
                    assert!(!rows4.is_empty());

                    assert_eq!(
                        Reports::post_n(client.query())
                            .await
                            .try_own()?,
                        4
                    );

                    let titles = Post::titles(client.query(), 1)
                        .await
                        .try_own()?;
                    assert_eq!(titles, vec!["hello", "second"]);
                    Ok::<(), own::Error<'_>>(())
                }))?;
                assert!(result.is_ok(), "joins must preserve matching rows");
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn lateral_joins() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_run(
            |rt| {
                let client = rt.client();
                let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                    let rows = Reports::top_posts(client.query())
                        .await
                        .try_own()?;
                    assert_eq!(rows.len(), 3);
                    for (u, p) in &rows {
                        assert_eq!(p.author_id, u.id);
                    }
                    let n = Reports::left_top_n(client.query())
                        .await
                        .try_own()?;
                    assert!(n >= 5);
                    Ok::<(), own::Error<'_>>(())
                }))?;
                assert!(result.is_ok(), "nested projections must preserve rows");
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn set_ops() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_run(
            |rt| {
                let client = rt.client();
                let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                    let r = Combine::union(client.query())
                        .await
                        .try_own()?;
                    assert_eq!(r.len(), 3);
                    let ids: Vec<i64> = r.iter().map(|u| u.id).collect();
                    assert_eq!(ids, vec![1, 4, 5]);

                    let r = Combine::union_all(client.query())
                        .await
                        .try_own()?;
                    assert_eq!(r.len(), 2);

                    let r = Combine::intersect(client.query())
                        .await
                        .try_own()?;
                    assert_eq!(r.len(), 3);
                    let r = Combine::overlap_all(client.query())
                        .await
                        .try_own()?;
                    assert_eq!(r.len(), 3);

                    let r = Combine::except(client.query())
                        .await
                        .try_own()?;
                    assert_eq!(r.len(), 4);
                    let r = Combine::except_all(client.query())
                        .await
                        .try_own()?;
                    assert_eq!(r.len(), 4);
                    Ok::<(), own::Error<'_>>(())
                }))?;
                assert!(result.is_ok(), "query set operations must match");
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn ctes() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_run(
            |rt| {
                let client = rt.client();
                let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                    let r = Combine::cte_active(client.query())
                        .await
                        .try_own()?;
                    let ids: Vec<i64> = r.iter().map(|u| u.id).collect();
                    assert_eq!(ids, vec![1, 2, 5]);

                    let r = Combine::cte_union(client.query(), 22)
                        .await
                        .try_own()?;
                    let ids: Vec<i64> = r.iter().map(|u| u.id).collect();
                    assert!(ids.contains(&2));
                    assert!(ids.contains(&4));
                    assert!(ids.contains(&5));

                    let r = Combine::nested_cte(client.query())
                        .await
                        .try_own()?;
                    let ids: Vec<i64> = r.iter().map(|u| u.id).collect();
                    assert_eq!(ids, vec![1, 2, 5]);
                    Ok::<(), own::Error<'_>>(())
                }))?;
                assert!(result.is_ok(), "common table expressions must match");
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn subqueries() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_run(
            |rt| {
                let client = rt.client();
                let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                    let r = Reports::with_posts(client.query())
                        .await
                        .try_own()?;
                    let ids: Vec<i64> = r.iter().map(|u| u.id).collect();
                    assert_eq!(ids, vec![1, 2, 3]);

                    let r = Reports::without_post(client.query())
                        .await
                        .try_own()?;
                    let ids: Vec<i64> = r.iter().map(|u| u.id).collect();
                    assert_eq!(ids, vec![4, 5]);

                    let counts = Reports::post_counts(client.query())
                        .await
                        .try_own()?;
                    assert_eq!(counts, vec![(1, 2), (2, 1), (3, 1), (4, 0), (5, 0)]);
                    assert_eq!(
                        Reports::self_later(client.query()).await.try_own()?,
                        [1, 2, 3, 4],
                    );
                    assert_eq!(
                        Reports::self_counts(client.query()).await.try_own()?,
                        [(1, 0), (2, 1), (3, 2), (4, 3), (5, 4)],
                    );
                    assert_eq!(
                        Reports::correlated(client.query(), 2).await.try_own()?.value,
                        1,
                    );
                    Ok::<(), own::Error<'_>>(())
                }))?;
                assert!(result.is_ok(), "subqueries must match");
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}
