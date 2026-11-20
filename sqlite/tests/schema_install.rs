use cartel_sqlite::{Action, Error, open, runtime};
use common::catalog::{Stable, open as file, try_limits, try_memory};
use common::plan::{self, SchemaAttempt as Attempt, SchemaError};
use dope_core::driver::life::recovery::Policy;
use dope_manifold::timing::Throughput;
use dope_runtime::{executor::session, topology::InstallError};
use dope_test::{fibers, harness::support::file::Directory};
use std::path::Path;

pub mod common;

#[test]
fn catalog_and_schema_commit_together() -> Result<(), plan::Error> {
    // SAFETY: this binary installs SQLite once before opening any connection.
    let runtime = runtime::Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
        .map_err(plan::Error::Runtime)?;
    let directory = Directory::try_with(
        Path::new(env!("CARGO_TARGET_TMPDIR")),
        "cartel-schema-install",
        |removed| removed.expect("directory cleanup"),
    )
    .expect("directory");
    let path = directory.path().join("store.db");
    for attempt in [
        Attempt::MissingTable,
        Attempt::Refusal,
        Attempt::EarlyCommit,
        Attempt::CommitFailure,
        Attempt::Success,
    ] {
        let plan = file(runtime, &path)?
            .plan::<Stable>(try_limits()?, try_memory()?)
            .map_err(plan::Error::Plan)?
            .for_profile::<Throughput>()
            .map_err(plan::Error::Claim)?;
        plan.try_scoped(
            |worker| {
                let mut report = open::Completion::new();
                match worker.install_with(|plan, stage| {
                    plan.install_immediate(stage, &mut report, |schema| attempt.initialize(schema))
                }) {
                    Ok(live) => {
                        assert!(matches!(attempt, Attempt::Success));
                        live.with_app(&mut session::Completion::new(), |mut app| {
                            let mut conn = app.root_client().expect("committed connection");
                            let query = conn.query().run_one::<i64, _, _>(Stable::COUNT, ());
                            let count = fibers::TEST
                                .try_run(&mut app, query)
                                .expect("drive count")
                                .expect("committed row");
                            assert_eq!(count, 1);
                        })
                        .expect("session");
                    }
                    Err(InstallError::Install(SchemaError::Rejected(n))) => {
                        assert!(matches!(attempt, Attempt::Refusal));
                        assert_eq!(n, 3);
                    }
                    Err(InstallError::Install(SchemaError::Open(open::Error::Step(
                        Error::Sqlite { action, code },
                    )))) => {
                        let expected = match attempt {
                            Attempt::MissingTable => {
                                (Action::Prepare, libsqlite3_sys::SQLITE_ERROR)
                            }
                            Attempt::EarlyCommit => (Action::Prepare, libsqlite3_sys::SQLITE_AUTH),
                            Attempt::CommitFailure => {
                                (Action::Step, libsqlite3_sys::SQLITE_CONSTRAINT_FOREIGNKEY)
                            }
                            _ => panic!("unexpected SQLite failure: {action:?} {code:?}"),
                        };
                        assert_eq!((action, code.get()), expected);
                    }
                    Err(error) => panic!("installation failed: {error:?}"),
                }
                report.take().expect("native cleanup");
            },
            Policy::new(common::assert_reclaimed, common::assert_retirement),
            |outcome| {
                outcome
                    .as_ref()
                    .map_err(|error| error.cause())
                    .expect("driver scope");
            },
        );
    }
    let count = plan::Plan::<Stable>::try_new(file(runtime, &path)?, try_limits()?, try_memory()?)?
        .try_count(
            Stable::COUNT,
            Policy::new(common::assert_reclaimed, common::assert_retirement),
            |outcome| outcome.expect("driver scope").expect("reopen"),
        )?;
    assert_eq!(count, 1);
    Ok(())
}
