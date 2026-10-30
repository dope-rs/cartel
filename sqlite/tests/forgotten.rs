use cartel_sqlite::{open, runtime};
use common::{engine::heap::*, forget, plan};
use dope_core::{driver::life::recovery, io};
use dope_manifold::timing::Throughput;
use dope_runtime::topology;

pub mod common;

#[test]
fn forgotten_app_retires() -> Result<(), plan::Error> {
    // SAFETY: this is the only test in the binary and initializes before any SQLite call.
    // Only Cartel uses installed SQLite; Core handles and allocations remain
    // exclusively owned by their originating worker.
    let runtime = runtime::Runtime::install(unsafe { runtime::raw::Permit::exclusive() })
        .map_err(plan::Error::Runtime)?;
    let report = || {
        recovery::Policy::new(
            |event| assert!(matches!(event, io::Reclaimed::Shutdown)),
            |failure: recovery::Recovery<'_, '_>| failure.abort(),
        )
    };
    for expected in [forget::Stage::Pending, forget::Stage::Installed] {
        let (limits, memory) = try_settings()?;
        let target = match expected {
            forget::Stage::Pending => open(runtime)?
                .commands(forget::COMMANDS)
                .map_err(plan::Error::Client)?,
            forget::Stage::Installed => open(runtime)?,
        };
        target
            .plan::<HeapDatabase>(limits, memory)
            .map_err(plan::Error::Plan)?
            .for_profile::<Throughput>()
            .map_err(plan::Error::Claim)?
            .try_create(
                |worker| {
                    let mut cleanup = open::Completion::new();
                    let result = worker.install_with(|open, stage| {
                        let config = stage.pinned().config();
                        let bytes = config.result_bytes();
                        let root = open.install(stage, &mut cleanup);
                        assert_eq!(config.result_bytes(), bytes);
                        forget::Forgotten {
                            root,
                            stage: expected,
                        }
                    });
                    assert!(matches!(
                        result,
                        Err(topology::InstallError::Install(forget::Error::Forgotten(stage)))
                            if stage == expected
                    ));
                    drop(result);
                    assert!(cleanup.take().is_ok());
                },
                report(),
                |result| assert!(result.is_ok()),
            );
    }

    let (limits, memory) = try_settings()?;
    let count = plan::Plan::<HeapDatabase>::try_new(open(runtime)?, limits, memory)?.try_count(
        HeapDatabase::CHECK,
        report(),
        |result| result.expect("SQLite scope"),
    );
    assert_eq!(count.expect("SQLite installation")?, 1);
    Ok(())
}
