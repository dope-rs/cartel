use crate::common::plan;
use cartel_sqlite::runtime::{self, Runtime};
use cartel_sqlite::{Error, open};
use common::{assert_reclaimed, assert_retirement, engine::smoke::*};
use dope_core::driver::life::recovery::Policy;
use dope_manifold::timing::Throughput;
use dope_test::fibers;

pub mod common;

#[test]
fn smoke_contracts() -> Result<(), plan::Error> {
    // SAFETY: this binary has one test; installation precedes every SQLite call.
    // Only Cartel uses installed SQLite; Core handles and allocations remain
    // exclusively owned by their originating worker until release.
    let runtime = Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
        .map_err(plan::Error::Runtime)?;
    repeat_open_is_rejected(runtime)?;
    exclusive_reusable_lease(runtime)?;
    round_trip(runtime)
}

fn repeat_open_is_rejected(runtime: Runtime) -> Result<(), plan::Error> {
    let limits = try_limits()?;
    let memory = try_memory()?;
    let (unused, second) = database(runtime)?
        .plan::<SmokeDatabase>(limits, memory)
        .map_err(plan::Error::Plan)?
        .into_parts();
    drop(unused);
    database(runtime)?
        .plan::<SmokeDatabase>(limits, memory)
        .map_err(plan::Error::Plan)?
        .for_profile::<Throughput>()
        .map_err(plan::Error::Claim)?
        .try_create(
            (),
            |worker| {
                let mut cleanup = open::Completion::new();
                assert!(matches!(
                    worker.install_with(|first, stage| {
                        drop(first.install(stage, &mut cleanup));
                        second.install(stage, &mut cleanup)
                    }),
                    Err(dope_runtime::topology::InstallError::Install(
                        cartel_sqlite::open::Error::AlreadyStarted
                    ))
                ));
                assert!(cleanup.take().is_ok(), "unpolled open cleanup");
            },
            Policy::new(assert_reclaimed, assert_retirement),
            |outcome| {
                outcome
                    .as_ref()
                    .map_err(|error| error.cause())
                    .expect("SQLite scope");
            },
        );
    Ok(())
}

fn exclusive_reusable_lease(runtime: Runtime) -> Result<(), plan::Error> {
    let result =
        plan::Plan::<SmokeDatabase>::try_new(database(runtime)?, try_limits()?, try_memory()?)?
            .try_app(
                |driver| {
                    {
                        let mut first =
                            driver.client(|ready| ready).map_err(plan::Error::Client)?;
                        let user = fibers::TEST
                            .try_run(driver, user_by_id(&mut first, 1))
                            .map_err(plan::Error::Run)?
                            .map_err(plan::Error::Call)?;
                        assert_eq!(user.name, "alice");
                        let occupied = driver.client(|ready| ready);
                        assert!(matches!(occupied, Err(Error::LocalConnectionBorrowed)));
                    }

                    let mut next = driver.client(|ready| ready).map_err(plan::Error::Client)?;
                    let user = fibers::TEST
                        .try_run(driver, user_by_id(&mut next, 2))
                        .map_err(plan::Error::Run)?
                        .map_err(plan::Error::Call)?;
                    assert_eq!(user.name, "bob");
                    Ok(())
                },
                Policy::new(assert_reclaimed, assert_retirement),
                |result| result.expect("SQLite scope"),
            );
    result.expect("SQLite application scope failed")
}

fn round_trip(runtime: Runtime) -> Result<(), plan::Error> {
    let result =
        plan::Plan::<SmokeDatabase>::try_new(database(runtime)?, try_limits()?, try_memory()?)?
            .try_run(
                |driver, mut conn| {
                    let user = fibers::TEST
                        .try_run(driver, user_by_id(&mut conn, 1))
                        .map_err(plan::Error::Run)?
                        .map_err(plan::Error::Call)?;
                    assert_eq!(user.id, 1);
                    assert_eq!(user.name, "alice");

                    let names = fibers::TEST
                        .try_run(driver, user_names(&mut conn, 1))
                        .map_err(plan::Error::Run)?
                        .map_err(plan::Error::Call)?;
                    assert_eq!(names.as_slice(), &["alice", "bob"]);

                    let missing = fibers::TEST
                        .try_run(driver, maybe_user(&mut conn, 99))
                        .map_err(plan::Error::Run)?
                        .map_err(plan::Error::Call)?;
                    assert!(missing.is_none());

                    fibers::TEST
                        .try_run(driver, rename(&mut conn, 2, "carol"))
                        .map_err(plan::Error::Run)?
                        .map_err(plan::Error::Call)?;
                    let user = fibers::TEST
                        .try_run(driver, user_by_id(&mut conn, 2))
                        .map_err(plan::Error::Run)?
                        .map_err(plan::Error::Call)?;
                    assert_eq!(user.name, "carol");
                    Ok(())
                },
                Policy::new(assert_reclaimed, assert_retirement),
                |result| result.expect("SQLite scope"),
            );
    result.expect("SQLite connection scope failed")
}
