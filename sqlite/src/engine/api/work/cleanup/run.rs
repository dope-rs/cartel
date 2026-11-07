use crate::{api, catalogs, limits};
use api::{stmt, work};
use dope_fiber::context;
use std::{pin, result, task};
use work::{cleanup::Status, guard, meter};

pub(crate) struct Run<'a, 'connection> {
    controls: stmt::Controls<'a, 'connection>,
    progress: limits::Progress,
    work: &'a work::Work,
}

impl<'a, 'connection> Run<'a, 'connection> {
    pub(crate) fn new<I: catalogs::Catalog>(
        engine: &'a mut stmt::Engine<'connection, I>,
        work: &'a work::Work,
        progress: limits::Progress,
    ) -> Self {
        Self {
            controls: engine.controls(),
            progress,
            work,
        }
    }

    pub(crate) fn poll(
        mut self,
        context: pin::Pin<&mut context::Context<'_, '_>>,
    ) -> task::Poll<result::Result<(), crate::Error>> {
        loop {
            match self.advance() {
                Ok(Status::Idle) => return task::Poll::Ready(Ok(())),
                Ok(Status::Pending) => match context.as_ref().try_continue() {
                    context::Continue::Permit(_) => {}
                    context::Continue::Exhausted => return task::Poll::Pending,
                },
                Err(error) => {
                    self.work.fail_cleanup();
                    return task::Poll::Ready(Err(error));
                }
            }
        }
    }

    pub(crate) fn park(mut self) {
        if let Err(error) = self.advance() {
            self.work.fail_cleanup().retain(error);
        }
    }

    fn advance(&mut self) -> result::Result<Status, crate::Error> {
        let source = self.work;
        let Some(cleanup) = source.pending.control() else {
            return Ok(Status::Idle);
        };
        let control = cleanup.control();
        source.meter.cleanup(self.progress)?;
        let result = source.meter.run(|| self.controls.execute(control));
        let result = match source.meter.call_quota() {
            meter::Quota::Available => result,
            meter::Quota::Exhausted => guard::limit(result),
        };
        result?;
        source.pending.complete(cleanup, &source.depth);
        let status = source.pending.status();
        if let Status::Idle = status {
            source.meter.finish();
        }
        Ok(status)
    }
}
