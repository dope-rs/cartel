use cartel_sqlite::{
    Action, Error, OpenOptions, Progress,
    runtime::{self, Runtime},
};
use common::{
    assert_reclaimed, assert_retirement,
    engine::layout::{COMMANDS, LayoutDatabase, try_limits, try_memory},
    plan,
};
use dope_core::driver::life::recovery::Policy;
use dope_test::fibers;
use std::num::NonZero;

pub mod common;

#[test]
fn control_budget_preserves_transaction() -> Result<(), plan::Error> {
    // SAFETY: this test exclusively owns SQLite and its installed allocator.
    let runtime = Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
        .map_err(plan::Error::Runtime)?;
    let database = OpenOptions::new()
        .open_memory(runtime)
        .map_err(plan::Error::Client)?
        .commands(COMMANDS)
        .map_err(plan::Error::Client)?;
    let mut limits = try_limits()?;
    limits.work.call.progress = Progress::new(NonZero::<u64>::MIN);
    plan::Plan::<LayoutDatabase>::try_new(database, limits, try_memory()?)?.try_run(
        |driver, mut conn| {
            fibers::TEST
                .try_run(
                    driver,
                    dope_fiber::fiber!('_ => async move {
                        for _ in 0..1_000 {
                            let result = match conn.transaction().await {
                                Ok(mut tx) => {
                                    let result = match tx.savepoint().await {
                                        Ok(mut save) => match save.rollback().await {
                                            Ok(()) => save.commit().await,
                                            Err(error) => Err(error),
                                        },
                                        Err(error) => Err(error),
                                    };
                                    match result {
                                        Ok(()) => tx.rollback().await,
                                        Err(error) => {
                                            if let Error::WorkLimit { action, code } = &error {
                                                assert!(matches!(
                                                    tx.savepoint().await,
                                                    Err(Error::TransactionAborted {
                                                        action: aborted_action,
                                                        code: aborted_code,
                                                    }) if aborted_action == *action && aborted_code == *code
                                                ));
                                            }
                                            Err(error)
                                        }
                                    }
                                }
                                Err(error) => Err(error),
                            };
                            if let Err(error) = result {
                                assert!(
                                    matches!(error, Error::WorkLimit { action: Action::Step, code }
                                        if code.primary() == libsqlite3_sys::SQLITE_INTERRUPT),
                                    "transaction budget: {error:?}"
                                );
                            }
                        }
                        assert_eq!(
                            conn.query()
                                .run_one::<i64, _, _>(LayoutDatabase::SELECT_ITEM, ())
                                .await?,
                            1
                        );
                        Ok::<(), Error>(())
                    }),
                )
                .map_err(plan::Error::Run)?
                .map_err(plan::Error::Call)
        },
        Policy::new(assert_reclaimed, assert_retirement),
        |result| result.expect("transaction control cleanup"),
    )
    .expect("transaction control installation")
}
