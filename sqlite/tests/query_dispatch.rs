use crate::common::{assert_reclaimed, assert_retirement, plan};
use crate::{common::database::IntegrationDatabase, common::schema::Keyword};
use cartel_sqlite::Error;
use cartel_sqlite::row;
use cartel_sqlite::runtime::{self, Runtime};
use common::{query::dispatch::*, schema};
use dope_core::driver::life::recovery::Policy;
use dope_test::fibers;
use std::sync::OnceLock;

pub mod common;

static RUNTIME: OnceLock<Result<Runtime, runtime::Error>> = OnceLock::new();

#[test]
fn dispatch_kinds() -> Result<(), plan::Error> {
    let runtime = (*RUNTIME.get_or_init(|| {
        // SAFETY: no SQLite call can pass this binary's initializer before it returns.
        // Only Cartel uses installed SQLite; Core handles and allocations remain
        // exclusively owned by their originating worker.
        Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
    }))
    .map_err(plan::Error::Runtime)?;
    ::core::assert_eq!(Keyword::<i64>::__CARTEL_TABLE, "\"select\"");
    ::core::assert_eq!(Keyword::<i64>::__CARTEL_SELECT_COLS, "\"type\"");
    {
        let (limits, memory) = schema::try_settings()?;
        let plan =
            plan::Plan::<IntegrationDatabase>::try_new(schema::open(runtime)?, limits, memory)?;
        let result =
            plan.try_run(
                |driver, local| {
                    fibers::TEST.try_run(driver, dope_fiber::fiber!('_ => async move {

        let mut conn = local;
        let u = user_by_id(&mut conn, 1).await?;
        ::core::assert_eq!(u.id, 1);
        ::core::assert_eq!(u.name, "alice");
        ::core::assert_eq!(u.age, 30);
        ::core::assert_eq!(u.score, 9.5);
        ::core::assert!(u.active);
        ::core::assert_eq!(u.nickname, Some("al"));
        ::core::assert!(u.avatar.is_none());

        ::core::assert!(::core::matches!(
            user_by_id(&mut conn, 999).await,
            Err(Error::QueryReturnedNoRows)
        ));

        ::core::assert!(::core::matches!(
            conn.query()
                .run_no_rows(IntegrationDatabase::MULTIPLE_ROWS, ())
                .await,
            Err(Error::UnexpectedRows {
                expected: 0,
                minimum: 1
            })
        ));

        let first = conn
            .query()
            .run_one::<i64, _, _>(IntegrationDatabase::MULTIPLE_ROWS, ())
            .await?;
        ::core::assert_eq!(first, 1);

        let first = conn
            .query()
            .run_one::<String, _, _>(IntegrationDatabase::MULTIPLE_TEXT_ROWS, ())
            .await?;
        ::core::assert_eq!(first, "first");

        let error = (conn
            .query()
            .run_first::<i64, _, _>(IntegrationDatabase::ROWS_OVER_LIMIT, ())
            .await)
            .expect_err("scan exceeded its row capacity");
        assert!(matches!(error, Error::ScanRows { capacity: 1_024 }));

        ::core::assert!(maybe_user_by_id(&mut conn, 999).await?.is_none());
        ::core::assert!(maybe_user_by_id(&mut conn, 2).await?.is_some());

        let all = all_users(&mut conn).await?;
        ::core::assert_eq!(all.len(), 5);

        let names = user_names_min_id(&mut conn, 3).await?;
        ::core::assert_eq!(names.as_slice(), &["carol", "dave", "한국어 √"]);

        let pairs = user_id_name_pairs(&mut conn).await?;
        ::core::assert_eq!(pairs.len(), 5);
        ::core::assert_eq!(pairs[0], (1, "alice"));

        ::core::assert_eq!(user_id_plus_one(&mut conn, 3).await?, 4);
        ::core::assert_eq!(keyword_value(&mut conn, 7).await?, Some(7));

                Ok::<(), cartel_sqlite::Error>(())
            })).map_err(plan::Error::Run)?.map_err(plan::Error::Call)
                },
                Policy::new(assert_reclaimed, assert_retirement),
                |result| result.expect("SQLite scope"),
            );
        result.expect("SQLite integration scope")
    }
}

#[test]
fn type_roundtrips() -> Result<(), plan::Error> {
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
        let row = user_blob(&mut conn, 3).await?;
        ::core::assert_eq!(row.as_slice(), &[Some(&[0xcau8, 0xfe][..])]);
        let row = user_blob(&mut conn, 4).await?;
        ::core::assert_eq!(row.as_slice(), &[Some(&[0xdeu8, 0xad, 0xbe, 0xef][..])]);
        let row = user_blob(&mut conn, 1).await?;
        ::core::assert_eq!(row.as_slice(), &[None]);
        ::core::assert_eq!(unicode_user_name(&mut conn).await?, "한국어 √");

                Ok::<(), cartel_sqlite::Error>(())
            })).map_err(plan::Error::Run)?.map_err(plan::Error::Call)
            },
            Policy::new(assert_reclaimed, assert_retirement),
            |result| result.expect("SQLite scope"),
        );
        result.expect("SQLite integration scope")
    }
}

#[test]
fn raw_values_keep_classes() -> Result<(), plan::Error> {
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
        let values = conn
            .query()
            .run_one::<(Option<String>, String, Vec<u8>, String), _, _>(
                IntegrationDatabase::VALUE_CLASSES,
                (),
            )
            .await?;
        ::core::assert_eq!(values.0, None);
        ::core::assert_eq!(values.1, "");
        ::core::assert_eq!(values.2, &[]);
        ::core::assert_eq!(values.3, "한국어 √");

        let composites = conn
            .query()
            .run_one::<(Option<(i64, String)>, i64, Option<(Option<i64>, String)>, i64), _, _>(
                IntegrationDatabase::NULL_COMPOSITES,
                (),
            )
            .await?;
        ::core::assert_eq!(composites, (None, 23, Some((None, "mixed")), 47));

        let error = (conn
            .query()
            .run_one::<(Option<(i64, Option<i64>, i64)>, Option<(Option<i64>, String)>, i64), _, _>(
                IntegrationDatabase::NULL_COMPOSITES,
                (),
            )
            .await)
            .expect_err("NULL prefix decoded as an integer");
        assert!(matches!(
            error,
            Error::ColumnType {
                column: 0,
                expected: row::Class::Integer,
                actual: row::Kind::Null
            }
        ));
        let prefixes = conn
            .query()
            .run_one::<(Option<(Option<i64>, Option<i64>, i64)>, Option<(Option<i64>, String)>, i64), _, _>(
                IntegrationDatabase::NULL_COMPOSITES,
                (),
            )
            .await?;
        ::core::assert_eq!(prefixes, (Some((None, None, 23)), Some((None, "mixed")), 47));

        let numbers = conn
            .query()
            .run_one::<(i64, f64, bool), _, _>(IntegrationDatabase::NUMERIC_CLASSES, ())
            .await?;
        ::core::assert_eq!(numbers, (-7, 1.25, true));

        let error = (conn
            .query()
            .run_one::<Vec<u8>, _, _>(IntegrationDatabase::MULTIPLE_TEXT_ROWS, ())
            .await)
            .expect_err("text column decoded as a blob");
        assert!(matches!(
            error,
            Error::ColumnType {
                column: 0,
                expected: row::Class::Blob,
                actual: row::Kind::Text
            }
        ));
        let error = (conn
            .query()
            .run_one::<String, _, _>(IntegrationDatabase::INVALID_TEXT, ())
            .await)
            .expect_err("invalid UTF-8 column decoded");
        assert!(matches!(
            error,
            Error::ColumnUtf8 {
                column: 0,
                error,
            } if error.valid_up_to() == 0 && error.error_len() == Some(1)
        ));

                Ok::<(), cartel_sqlite::Error>(())
            })).map_err(plan::Error::Run)?.map_err(plan::Error::Call)
            },
            Policy::new(assert_reclaimed, assert_retirement),
            |result| result.expect("SQLite scope"),
        );
        result.expect("SQLite integration scope")
    }
}
