use cartel_pg::{protocol::Reason, schema::TypedQuery as _};
use common::{external::runtime::check, recover};
use dope_fiber::BlockOn as _;
use std::env::var_os;

use crate::common::{
    env::{self, Env},
    external::{
        own,
        own::Convert as _,
        reports::{Aggregate, AggregateRejectId},
        row::Widget,
        runtime::Plan,
    },
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
                    let rows = Aggregate::row_numbers(client)
                        .await
                        .try_own()?;
                    ::core::assert_eq!(rows.len(), 5);
                    let rns: Vec<i64> = rows.iter().map(|(_, n)| *n).collect();
                    ::core::assert_eq!(rns, ::std::vec![1, 2, 3, 4, 5]);

                    let ll = Aggregate::lag_lead_ids(client)
                        .await
                        .try_own()?;
                    ::core::assert_eq!(ll.len(), 5);
                    ::core::assert_eq!(ll[0].1, None);
                    ::core::assert_eq!(ll[1].1, Some(1));
                    ::core::assert_eq!(ll[4].2, None);

                    let rs = Widget::running_n(client)
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
                        Aggregate::count_all(client)
                            .await
                            .try_own()?,
                        5
                    );
                    let mut pool = dope_fiber::task::batch::Pool::<4>::acquire().await
                        .expect("aggregate task credits");
                    let values = Aggregate::count_active_each(
                        client,
                        &mut pool,
                        [true, false, true, false],
                    ).await.expect("aggregate tasks");
                    for (value, expected) in values.zip([4, 1, 4, 1]) {
                        assert_eq!(value.try_own()?, expected);
                    }
                    assert_eq!(
                        Aggregate::sum_ages(client)
                            .await
                            .try_own()?,
                        Some(Some(30 + 25 + 40 + 22 + 29))
                    );
                    let cases = [
                        (0, 1, 0, Some(Some((30 + 25 + 40 + 22 + 29) * 2))),
                        (51, 1, 0, Some(None)),
                        (0, 0, 0, None),
                        (0, 1, 1, None),
                    ];
                    let values = Aggregate::sum_doubled_each(
                        client,
                        &mut pool,
                        cases.map(|(min, limit, offset, _)| (min, limit, offset)),
                    ).await.expect("aggregate tasks");
                    for (value, (_, _, _, expected)) in values.zip(cases) {
                        let value = value.map_err(own::Error::Query)?;
                        assert_eq!(
                            value.as_ref().map(|row| row.decode()).transpose()
                                .map_err(own::Error::Decode)?.map(|value| value.map(|value| value.id)),
                            expected
                        );
                        let value = value.map(|row| row.into_owned()).transpose()
                            .map_err(own::Error::Decode)?;
                        assert_eq!(value.map(|row| row.map(|value| value.id)), expected);
                    }
                    assert_eq!(AggregateRejectId::RESULT_FORMAT_CODES, &[0]);
                    for (min, null) in [(0, false), (6, true)] {
                        let row = Aggregate::reject_id(client, min)
                            .await
                            .map_err(own::Error::Query)?
                            .expect("aggregate row");
                        for result in [row.decode(), row.into_owned()] {
                            assert!(match result {
                                Ok(None) => null,
                                Err(cartel_pg::Error::Protocol(Reason::ColumnNull {
                                    column: 0,
                                })) => !null,
                                _ => false,
                            });
                        }
                    }
                    let avg = Aggregate::avg_score(client)
                        .await
                        .try_own()?
                        .expect("aggregate row")
                        .expect("users are nonempty");
                    assert!((avg - (9.5 + 7.0 + 8.2 + 6.1 + 5.0) / 5.0).abs() < 1e-6);
                    assert_eq!(
                        Aggregate::min_age(client)
                            .await
                            .try_own()?,
                        Some(Some(22))
                    );
                    let m = Aggregate::max_score(client)
                        .await
                        .try_own()?
                        .expect("aggregate row")
                        .expect("users are nonempty");
                    assert!((m - 9.5).abs() < 1e-9);
                    assert_eq!(
                        Widget::bucket_n(client)
                            .await
                            .try_own()?,
                        vec![(1, 2), (2, 2), (3, 1)]
                    );
                    let maxes = Widget::bucket_max(client)
                        .await
                        .try_own()?;
                    assert_eq!(
                        maxes,
                        vec![(1, Some(20)), (2, Some(40)), (3, Some(50))]
                    );
                    assert_eq!(
                        Widget::having(client, 2)
                            .await
                            .try_own()?,
                        vec![(1, 2), (2, 2)]
                    );
                    let widgets = Widget::all_rows(client)
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
