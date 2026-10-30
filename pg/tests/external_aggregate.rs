use cartel_pg::api::traits::Client as _;
use common::{external::runtime::check, recover};
use dope_fiber::BlockOn as _;
use std::env::var_os;

use crate::common::{
    env::{self, Env},
    external::{own, own::Convert as _, reports::Aggregate, row::Widget, runtime::Plan},
};

pub mod common;

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn window_functions() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_run(
            |rt| {
                let client = rt.client();
                let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                    let rows = Aggregate::row_numbers(client.query())
                        .await
                        .try_own()?;
                    ::core::assert_eq!(rows.len(), 5);
                    let rns: Vec<i64> = rows.iter().map(|(_, n)| *n).collect();
                    ::core::assert_eq!(rns, ::std::vec![1, 2, 3, 4, 5]);

                    let ll = Aggregate::lag_lead_ids(client.query())
                        .await
                        .try_own()?;
                    ::core::assert_eq!(ll.len(), 5);
                    ::core::assert_eq!(ll[0].1, None);
                    ::core::assert_eq!(ll[1].1, Some(1));
                    ::core::assert_eq!(ll[4].2, None);

                    let rs = Widget::running_n(client.query())
                        .await
                        .try_own()?;
                    ::core::assert_eq!(rs.len(), 5);
                    let totals: Vec<i64> = rs.iter().map(|(_, t)| *t).collect();
                    ::core::assert_eq!(totals, ::std::vec![1, 2, 3, 4, 5]);
                    Ok::<(), own::Error<'_>>(())
                }))?;
                assert!(result.is_ok(), "window queries must match");
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn aggregates() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_run(
            |rt| {
                let client = rt.client();
                let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                    assert_eq!(
                        Aggregate::count_all(client.query())
                            .await
                            .try_own()?,
                        5
                    );
                    assert_eq!(
                        Aggregate::count_active(client.query(), true)
                            .await
                            .try_own()?,
                        4
                    );
                    assert_eq!(
                        Aggregate::sum_ages(client.query())
                            .await
                            .try_own()?,
                        Some(30 + 25 + 40 + 22 + 29)
                    );
                    let s = Aggregate::sum_doubled(client.query(), 0)
                        .await
                        .try_own()?;
                    assert_eq!(s, Some((30 + 25 + 40 + 22 + 29) * 2));
                    let avg = Aggregate::avg_score(client.query())
                        .await
                        .try_own()?
                        .expect("users are nonempty");
                    assert!((avg - (9.5 + 7.0 + 8.2 + 6.1 + 5.0) / 5.0).abs() < 1e-6);
                    assert_eq!(
                        Aggregate::min_age(client.query())
                            .await
                            .try_own()?,
                        Some(22)
                    );
                    let m = Aggregate::max_score(client.query())
                        .await
                        .try_own()?
                        .expect("users are nonempty");
                    assert!((m - 9.5).abs() < 1e-9);
                    assert_eq!(
                        Widget::bucket_n(client.query())
                            .await
                            .try_own()?,
                        vec![(1, 2), (2, 2), (3, 1)]
                    );
                    let maxes = Widget::bucket_max(client.query())
                        .await
                        .try_own()?;
                    assert_eq!(
                        maxes,
                        vec![(1, Some(20)), (2, Some(40)), (3, Some(50))]
                    );
                    assert_eq!(
                        Widget::having(client.query(), 2)
                            .await
                            .try_own()?,
                        vec![(1, 2), (2, 2)]
                    );
                    let widgets = Widget::all_rows(client.query())
                        .await
                        .map_err(own::Error::Query)?
                        .validate()
                        .map_err(own::Error::Decode)?;
                    let widgets = widgets
                        .iter()
                        .map(|widget| (widget.id, widget.bucket, widget.value))
                        .collect::<Vec<_>>();
                    assert_eq!(
                        widgets,
                        vec![(1, 1, 10), (2, 1, 20), (3, 2, 30), (4, 2, 40), (5, 3, 50)]
                    );
                    Ok::<(), own::Error<'_>>(())
                }))?;
                assert!(result.is_ok(), "query aggregates must match");
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}
