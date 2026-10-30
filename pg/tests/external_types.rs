use crate::common::external::Seed as _;
use crate::common::external::runtime::Plan;
use crate::{
    common::external::own, common::external::own::Convert as _, common::external::row::AllTypes,
};
use cartel_pg::api::traits::{Client as _, Unchecked as _};
use common::env::{self, Env};
use common::{external::runtime::check, recover};
use dope_fiber::BlockOn as _;
use std::env::var_os;

use cartel_pg::{Date, Timestamp, Uuid};

pub mod common;

#[test]
#[ignore = "requires CARTEL_PG_TEST and PostgreSQL"]
fn type_roundtrips_full() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_run(
            |rt| {
                let client = rt.client();
                let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                    client
                        .unchecked()
                        .command("DELETE FROM cartel_pg_types")
                        .await
                        .map_err(own::Error::Query)?;
                    client
                        .unchecked()
                        .command(
                            "INSERT INTO cartel_pg_types
                (id, v_i16, v_i32, v_i64, v_f32, v_f64, v_bool, v_string, v_bytes,
                 v_uuid, v_ts, v_date, v_ltree,
                 o_i32, o_i64, o_bool, o_string, o_bytes, o_uuid,
                 arr_i32, arr_i64, arr_str)
             VALUES
                (1, 12345, 123456789, 9876543210, 1.5::real, 2.5::double precision, TRUE,
                 'rust √', E'\\\\xcafebabe',
                 '10101010-1010-1010-1010-101010101010'::uuid,
                 '2001-09-09 01:46:39'::timestamp,
                 DATE '2024-10-04',
                 'a.b.c'::ltree,
                 42, 43, TRUE, 'opt', E'\\\\xdead',
                 '20202020-2020-2020-2020-202020202020'::uuid,
                 ARRAY[1,2,3]::int4[], ARRAY[10,20,30]::int8[], ARRAY['x','y']::text[])",
                        )
                        .await
                        .map_err(own::Error::Query)?;
                    client
                        .unchecked()
                        .command(
                            "INSERT INTO cartel_pg_types
                (id, v_i16, v_i32, v_i64, v_f32, v_f64, v_bool, v_string, v_bytes,
                 v_uuid, v_ts, v_date, v_ltree,
                 o_i32, o_i64, o_bool, o_string, o_bytes, o_uuid,
                 arr_i32, arr_i64, arr_str)
             VALUES
                (2, 0, 0, 0, 0.0::real, 0.0::double precision, FALSE,
                 'n', E'\\\\x',
                 '00000000-0000-0000-0000-000000000000'::uuid,
                 '2001-09-09 01:46:39'::timestamp,
                 DATE '2024-10-04',
                 'z'::ltree,
                 NULL, NULL, NULL, NULL, NULL, NULL,
                 ARRAY[]::int4[], ARRAY[]::int8[], ARRAY[]::text[])",
                        )
                        .await
                        .map_err(own::Error::Query)?;

                    let row = AllTypes::by_id(client.query(), 1)
                        .await
                        .try_own()?;
                    ::core::assert_eq!(row.0.id, 1);
                    ::core::assert_eq!(row.0.v_i16, 12345);
                    ::core::assert_eq!(row.0.v_i32, 123456789);
                    ::core::assert_eq!(row.0.v_i64, 9876543210);
                    ::core::assert!((row.0.v_f32 - 1.5).abs() < 1e-6);
                    ::core::assert!((row.0.v_f64 - 2.5).abs() < 1e-9);
                    ::core::assert!(row.0.v_bool);
                    ::core::assert_eq!(row.0.v_string, "rust √");
                    ::core::assert_eq!(row.1.v_bytes, ::std::vec![0xca, 0xfe, 0xba, 0xbe]);
                    ::core::assert!(row.1.v_uuid == Uuid::from_bytes([0x10; 16]));
                    ::core::assert_eq!(row.1.v_ltree.as_ref(), "a.b.c");
                    ::core::assert_eq!(row.2.o_i32, Some(42));
                    ::core::assert_eq!(row.2.o_i64, Some(43));
                    ::core::assert_eq!(row.2.o_bool, Some(true));
                    ::core::assert_eq!(row.2.o_string.as_deref(), Some("opt"));
                    ::core::assert_eq!(row.2.o_bytes, Some(::std::vec![0xde, 0xad]));
                    ::core::assert!(row.2.o_uuid == Some(Uuid::from_bytes([0x20; 16])));

                    let row2 = AllTypes::by_id(client.query(), 2)
                        .await
                        .try_own()?;
                    ::core::assert!(row2.2.o_i32.is_none());
                    ::core::assert!(row2.2.o_i64.is_none());
                    ::core::assert!(row2.2.o_bool.is_none());
                    ::core::assert!(row2.2.o_string.is_none());
                    ::core::assert!(row2.2.o_bytes.is_none());
                    ::core::assert!(row2.2.o_uuid.is_none());

                    let arrs = AllTypes::arrays_by_id(client.query(), 1)
                        .await
                        .try_own()?;
                    ::core::assert_eq!(arrs.0, ::std::vec![1, 2, 3]);
                    ::core::assert_eq!(arrs.1, ::std::vec![10, 20, 30]);
                    ::core::assert_eq!(arrs.2, ::std::vec!["x".to_string(), "y".to_string()]);
                    Ok::<(), own::Error<'_>>(())
                }))?;
                assert!(
                    result.is_ok(),
                    "all field types must roundtrip: {:?}",
                    result.as_ref().err().map(|error| match error {
                        own::Error::Query(error) => Ok(error.kind()),
                        own::Error::Decode(error) => Err(error),
                    })
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
fn type_param_binding_each() {
    let input = Env::try_load(env::NAMES.map(var_os)).expect("PostgreSQL environment");
    Plan::try_new(&input)
        .expect("external PostgreSQL fixture")
        .try_run(
            |rt| {
                let client = rt.client();
                let result = rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                client
                    .unchecked()
                    .command("DELETE FROM cartel_pg_types")
                    .await
                    .map_err(own::Error::Query)?;
                client.seed_types()
                    .await
                    .map_err(own::Error::Query)?;
                let uuid = Uuid::from_bytes([0x31; 16]);
                let ts = Timestamp(100i64);
                let date = Date(1000i32);
                let ltree = cartel_pg::Ltree::try_from(&mut String::from("a.b.c"))
                    .expect("ltree storage must allocate");
                AllTypes::set_ints(client.query(), 42, 321i16, 654i32, 987i64, false)
                    .await
                    .try_own()?;
                let update = {
                    let text = String::from("updated");
                    AllTypes::set_scalars(client.query(), 42, 1.25f32, 6.5f64, &text)
                };
                update.await.try_own()?;
                AllTypes::set_bytes(client.query(), 42, ::std::vec![0xaa, 0xbb]).await.try_own()?;
                AllTypes::set_temporal(client.query(), 42, uuid, ts, date, ltree)
                    .await
                    .try_own()?;
                let row = AllTypes::by_id(client.query(), 42)
                    .await
                    .try_own()?;
                ::core::assert_eq!(row.0.v_i16, 321);
                ::core::assert_eq!(row.0.v_i32, 654);
                ::core::assert_eq!(row.0.v_i64, 987);
                ::core::assert!((row.0.v_f32 - 1.25).abs() < 1e-6);
                ::core::assert!((row.0.v_f64 - 6.5).abs() < 1e-9);
                ::core::assert!(!row.0.v_bool);
                ::core::assert_eq!(row.0.v_string, "updated");
                ::core::assert_eq!(row.1.v_bytes, ::std::vec![0xaa, 0xbb]);
                ::core::assert!(row.1.v_uuid == uuid);
                ::core::assert_eq!(row.1.v_ts.0, 100);
                ::core::assert_eq!(row.1.v_date.0, 1000);
                ::core::assert!(row.1.v_ltree == "a.b.c");
                Ok::<(), own::Error<'_>>(())
            }))?;
                assert!(
                    result.is_ok(),
                    "all parameter types must bind: {:?}",
                    result.as_ref().err().map(|error| match error {
                        own::Error::Query(error) => Ok(error.kind()),
                        own::Error::Decode(error) => Err(error),
                    })
                );
                Ok(())
            },
            recover,
            check,
        )
        .expect("PostgreSQL runtime");
}
