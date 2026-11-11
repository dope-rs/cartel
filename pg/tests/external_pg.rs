use crate::common::external::runtime::Plan;
use crate::{
    common::external::own, common::external::own::Convert as _,
    common::external::reports::Aggregate, common::external::row::AllTypes,
    common::external::row::Doc, common::external::row::LtreeNode, common::external::row::Score,
    common::external::row::WidgetSlim, common::external::search::Order,
    common::external::user::Lookup,
};
use cartel_pg::api::traits::{Begin as _, Client as _, Transaction as _, Unchecked as _};
use common::env::{self, Env};
use common::{external::runtime::check, recover};
use dope_fiber::BlockOn as _;
use std::env::var_os;

pub mod common;

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn full_text_search() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_run(
            |rt| {
                let client = rt.client();
                let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                    let r = Doc::fts_match(client, "quick".into())
                        .await
                        .try_own()?;
                    ::core::assert_eq!(r, ::std::vec![1, 2]);
                    let r = Doc::fts_raw(client, "rust & cartel".into())
                        .await
                        .try_own()?;
                    ::core::assert_eq!(r, ::std::vec![2]);
                    let r = Doc::ts_rank_q(client, "quick".into(), "quick".into())
                        .await
                        .try_own()?;
                    ::core::assert_eq!(r.len(), 2);
                    for (_, rank) in &r {
                        ::core::assert!(*rank >= 0.0);
                    }
                    Ok::<(), own::Error<'_>>(())
                }))?;
                assert!(result.is_ok(), "full text queries must match");
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn array_ops() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_run(
            |rt| {
                let client = rt.client();
                let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                    client
                        .command("DELETE FROM cartel_pg_types")
                        .await
                        .map_err(own::Error::Query)?;
                    client
                        .command(
                            "INSERT INTO cartel_pg_types
                (id, v_i16, v_i32, v_i64, v_f32, v_f64, v_bool, v_string, v_bytes,
                 v_uuid, v_ts, v_date, v_ltree,
                 arr_i32, arr_i64, arr_str)
             VALUES
                (10, 0, 0, 0, 0.0, 0.0, TRUE, 'a', E'\\\\x',
                 '11111111-1111-1111-1111-111111111111'::uuid,
                 NOW(), CURRENT_DATE, 'r'::ltree,
                 ARRAY[1,2,3]::int4[], ARRAY[]::int8[], ARRAY['rust','sql']::text[]),
                (11, 0, 0, 0, 0.0, 0.0, TRUE, 'b', E'\\\\x',
                 '22222222-2222-2222-2222-222222222222'::uuid,
                 NOW(), CURRENT_DATE, 'r'::ltree,
                 ARRAY[2,3,4]::int4[], ARRAY[]::int8[], ARRAY['sql','tree']::text[]),
                (12, 0, 0, 0, 0.0, 0.0, TRUE, 'c', E'\\\\x',
                 '33333333-3333-3333-3333-333333333333'::uuid,
                 NOW(), CURRENT_DATE, 'r'::ltree,
                 ARRAY[7,8,9]::int4[], ARRAY[]::int8[], ARRAY['cake']::text[])",
                        )
                        .await
                        .map_err(own::Error::Query)?;

                    let rhs: &[i32] = &[2, 3];
                    let r = AllTypes::ints_contain(client, rhs)
                        .await
                        .try_own()?;
                    ::core::assert_eq!(r, ::std::vec![10, 11]);

                    let rhs: &[i32] = &[2, 99];
                    let r = AllTypes::ints_contain(client, rhs)
                        .await
                        .try_own()?;
                    ::core::assert!(r.is_empty());

                    let rhs: &[i32] = &[3, 4];
                    let r = AllTypes::ints_overlap(client, rhs)
                        .await
                        .try_own()?;
                    ::core::assert_eq!(r, ::std::vec![10, 11]);

                    let rhs: &[i32] = &[99, 100];
                    let r = AllTypes::ints_overlap(client, rhs)
                        .await
                        .try_own()?;
                    ::core::assert!(r.is_empty());

                    let rhs: &[&str] = &["sql"];
                    let r = AllTypes::strs_contain(client, rhs)
                        .await
                        .try_own()?;
                    ::core::assert_eq!(r, ::std::vec![10, 11]);

                    let rhs: &[&str] = &["rust", "missing"];
                    let r = AllTypes::strs_contain(client, rhs)
                        .await
                        .try_own()?;
                    ::core::assert!(r.is_empty());

                    let rhs: &[&str] = &["cake", "missing"];
                    let r = AllTypes::strs_overlap(client, rhs)
                        .await
                        .try_own()?;
                    ::core::assert_eq!(r, ::std::vec![12]);

                    let r = AllTypes::ints_len(client)
                        .await
                        .try_own()?;
                    ::core::assert_eq!(r, ::std::vec![(10, 3), (11, 3), (12, 3)]);

                    let r = AllTypes::strs_len(client)
                        .await
                        .try_own()?;
                    ::core::assert_eq!(r, ::std::vec![(10, 2), (11, 2), (12, 1)]);
                    Ok::<(), own::Error<'_>>(())
                }))?;
                assert!(result.is_ok(), "array operators must match");
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn ltree_ops() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_run(
            |rt| {
                let client = rt.client();
                let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                    let path = cartel_pg::Ltree::try_from(&mut String::from("top.a"))
                        .expect("ltree storage must allocate");
                    let r = LtreeNode::descendants(client, path)
                        .await
                        .try_own()?;
                    ::core::assert_eq!(r, ::std::vec![2, 3]);
                    let path = cartel_pg::Ltree::try_from(&mut String::from("top.a.b"))
                        .expect("ltree storage must allocate");
                    let r = LtreeNode::ancestors(client, path)
                        .await
                        .try_own()?;
                    ::core::assert_eq!(r, ::std::vec![1, 2, 3]);
                    Ok::<(), own::Error<'_>>(())
                }))?;
                assert!(result.is_ok(), "ltree operators must match");
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn array_sizes() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_run(
            |rt| {
                let client = rt.client();
                let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                    let ids = [1i32, 2, 3];
                    let c = Aggregate::array_int4(client, &ids)
                        .await
                        .try_own()?;
                    ::core::assert_eq!(c, 5);
                    let c = Aggregate::cardinality(client, &ids)
                        .await
                        .try_own()?;
                    ::core::assert_eq!(c, 5);
                    Ok::<(), own::Error<'_>>(())
                }))?;
                assert!(result.is_ok(), "array cardinalities must match");
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn row_lock_clauses() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_run(
            |rt| {
                let client = rt.client();
                let result = (|| -> Result<(), own::Error<'_>> {
                    let tx = rt
                        .runtime
                        .block_on(client.transaction().begin())
                        .expect("runtime drive")
                        .map_err(own::Error::Query)?;
                    let rows = rt
                        .runtime
                        .block_on(Order::locked(&tx))
                        .expect("runtime drive")
                        .try_own()?;
                    ::core::assert_eq!(rows, ::std::vec![1, 2]);
                    let rows = rt
                        .runtime
                        .block_on(Order::share_lock(&tx))
                        .expect("runtime drive")
                        .try_own()?;
                    ::core::assert_eq!(rows, ::std::vec![1, 2]);
                    rt.runtime
                        .block_on(tx.commit())
                        .expect("runtime drive")
                        .map_err(own::Error::Query)?;
                    Ok(())
                })();
                assert!(result.is_ok(), "row lock clauses must commit");
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn insert_each_bulk() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_run(
            |rt| {
                let client = rt.client();
                let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                    let ids = [500i64, 501, 502];
                    let buckets = [10i32, 11, 12];
                    let values = [100i64, 200, 300];
                    WidgetSlim::insert_bulk(client, &ids, &buckets, &values)
                        .await
                        .try_own()?;
                    let n = WidgetSlim::count_500(client)
                        .await
                        .try_own()?;
                    ::core::assert_eq!(n, 3);
                    let rows = WidgetSlim::rows_500(client)
                        .await
                        .map_err(own::Error::Query)?
                        .validate()
                        .map_err(own::Error::Decode)?;
                    let rows = rows
                        .iter()
                        .map(|row| (row.id, row.bucket, row.value))
                        .collect::<Vec<_>>();
                    ::core::assert_eq!(
                        rows,
                        ::std::vec![(500, 10, 100), (501, 11, 200), (502, 12, 300)]
                    );
                    Ok::<(), own::Error<'_>>(())
                }))?;
                assert!(result.is_ok(), "bulk inserts must preserve rows");
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn bulk_update_filter_each() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_run(
            |rt| {
                let client = rt.client();
                let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                    let tx = client
                        .transaction()
                        .begin()
                        .await
                        .map_err(own::Error::Query)?;
                    let ids = [1i32, 3, 5];
                    let numbers = [111i32, 333, 555];
                    Score::bulk_update(&tx, &ids, &numbers)
                        .await
                        .try_own()?;
                    let score = Score::by_id(&tx, 1)
                        .await
                        .map_err(own::Error::Query)?;
                    let score = score.decode().map_err(own::Error::Decode)?;
                    ::core::assert_eq!((score.id, score.randomnumber), (1, 111));
                    ::core::assert_eq!(
                        Score::by_id(&tx, 1)
                            .await
                            .try_own()?
                            .randomnumber,
                        111
                    );
                    ::core::assert_eq!(
                        Score::by_id(&tx, 2)
                            .await
                            .try_own()?
                            .randomnumber,
                        200
                    );
                    ::core::assert_eq!(
                        Score::by_id(&tx, 3)
                            .await
                            .try_own()?
                            .randomnumber,
                        333
                    );
                    ::core::assert_eq!(
                        Score::by_id(&tx, 4)
                            .await
                            .try_own()?
                            .randomnumber,
                        400
                    );
                    ::core::assert_eq!(
                        Score::by_id(&tx, 5)
                            .await
                            .try_own()?
                            .randomnumber,
                        555
                    );
                    tx.commit()
                        .await
                        .map_err(own::Error::Query)?;
                    ::core::assert_eq!(
                        Score::by_id(client, 3)
                            .await
                            .try_own()?
                            .randomnumber,
                        333
                    );
                    Ok::<(), own::Error<'_>>(())
                }))?;
                assert!(result.is_ok(), "bulk updates must preserve selected values");
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn raw_typed_query() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_run(
            |rt| {
                let client = rt.client();
                let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                    let v = Lookup::id_literal(client, 1)
                        .await
                        .try_own()?;
                    ::core::assert_eq!(v, 1);
                    Ok::<(), own::Error<'_>>(())
                }))?;
                assert!(result.is_ok(), "raw typed query must decode");
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}
