use crate::{api, config};
use api::{stmt, work};
use std::{mem, result};
use work::{cleanup, guard, meter};

/// One cleanup attempt retains the exact work source that issued its pending debt.
pub(super) struct Call<'source> {
    source: &'source work::Work,
    cleanup: cleanup::Cleanup,
}

impl<'source> Call<'source> {
    pub(super) fn new(source: &'source work::Work) -> Option<Self> {
        source
            .pending
            .control()
            .map(|cleanup| Self { source, cleanup })
    }

    pub(super) fn run(
        self,
        controls: &mut stmt::Controls<'_, '_>,
        config: &config::Config,
    ) -> result::Result<cleanup::Status, crate::Error> {
        let source = self.source;
        let control = self.cleanup.control();
        source.meter.cleanup(config.work.cleanup.progress)?;
        let result = source.meter.run(|| controls.execute(control));
        let result = match source.meter.call_quota() {
            meter::Quota::Available => result,
            meter::Quota::Exhausted => guard::limit(result),
        };
        result?;
        source.pending.complete(self.cleanup, &source.depth);
        let status = source.pending.status();
        if let cleanup::Status::Idle = status {
            source.meter.stop();
        }
        Ok(status)
    }
}

const _: () = assert!(mem::size_of::<Call<'_>>() == 2 * mem::size_of::<usize>());
