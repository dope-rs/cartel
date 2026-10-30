use crate::{api, config, retry, sys};
use api::{stmt, work};
use dope_core::driver::schedule::clock;
use dope_fiber::context;
use retry::lease;
use std::{pin, result, task};
use work::cleanup::{Activity, Call, Status};

pub(super) struct Drain<'a, 'slot, 'connection, 'd> {
    controls: stmt::Controls<'a, 'connection>,
    config: &'a config::Config,
    work: &'a work::Work,
    retry: &'a mut lease::Lease<'slot, 'd, lease::Cleanup>,
}

impl<'a, 'slot, 'connection, 'd> Drain<'a, 'slot, 'connection, 'd> {
    pub(super) const fn new(
        controls: stmt::Controls<'a, 'connection>,
        config: &'a config::Config,
        work: &'a work::Work,
        retry: &'a mut lease::Lease<'slot, 'd, lease::Cleanup>,
    ) -> Self {
        Self {
            controls,
            config,
            work,
            retry,
        }
    }

    pub(super) fn poll(
        mut self,
        mut context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> task::Poll<result::Result<(), crate::Error>> {
        loop {
            let Some(cleanup) = Call::new(self.work) else {
                self.retry.finish();
                return task::Poll::Ready(Ok(()));
            };
            match self.retry.wait() {
                retry::Wait::Ready => {}
                retry::Wait::Pending => {
                    let wake = context.as_ref().completion();
                    let now = context.as_mut().driver().driver_ref().clock().now();
                    match self.retry.poll(now, wake) {
                        task::Poll::Pending => return task::Poll::Pending,
                        task::Poll::Ready(Err(error)) => {
                            return task::Poll::Ready(Err(self.fail(error)));
                        }
                        task::Poll::Ready(Ok(())) => {}
                    }
                }
            }
            match self.advance(cleanup) {
                Ok(Status::Idle) => return task::Poll::Ready(Ok(())),
                Ok(Status::Pending) => match context.as_ref().try_continue() {
                    context::Continue::Permit(_) => {}
                    context::Continue::Exhausted => return task::Poll::Pending,
                },
                Err(crate::Error::Busy { action, code }) => {
                    let wake = context.as_ref().completion();
                    let now = context.as_mut().driver().driver_ref().clock().now();
                    let fault = sys::Fault::new(action, code);
                    if let Err(error) = self.retry.defer(now, fault) {
                        return task::Poll::Ready(Err(self.fail(error)));
                    }
                    match self.retry.poll(now, wake) {
                        task::Poll::Pending => return task::Poll::Pending,
                        task::Poll::Ready(Err(error)) => {
                            return task::Poll::Ready(Err(self.fail(error)));
                        }
                        task::Poll::Ready(Ok(())) => {}
                    }
                }
                Err(error) => return task::Poll::Ready(Err(self.fail(error))),
            }
        }
    }

    pub(super) fn poll_park(mut self, now: clock::Moment<'d>) -> task::Poll<Activity> {
        let Some(cleanup) = Call::new(self.work) else {
            self.retry.finish();
            return task::Poll::Ready(Activity::Idle);
        };
        match self.retry.poll_park(now) {
            task::Poll::Pending => return task::Poll::Pending,
            task::Poll::Ready(Err(error)) => {
                return self.fail_park(error);
            }
            task::Poll::Ready(Ok(())) => {}
        }
        match self.advance(cleanup) {
            Ok(Status::Idle | Status::Pending) => task::Poll::Ready(Activity::Advanced),
            Err(crate::Error::Busy { action, code }) => {
                match self.retry.defer(now, sys::Fault::new(action, code)) {
                    Ok(()) => task::Poll::Pending,
                    Err(error) => self.fail_park(error),
                }
            }
            Err(error) => self.fail_park(error),
        }
    }

    fn advance(&mut self, cleanup: Call<'_>) -> result::Result<Status, crate::Error> {
        let status = cleanup.run(&mut self.controls, self.config)?;
        if let Status::Idle = status {
            self.retry.finish();
        }
        Ok(status)
    }

    fn fail(self, error: crate::Error) -> crate::Error {
        self.work.fail_cleanup();
        self.retry.finish();
        error
    }

    fn fail_park(self, error: crate::Error) -> task::Poll<Activity> {
        let failed = self.work.fail_cleanup();
        self.retry.finish();
        failed.retain(error);
        task::Poll::Ready(Activity::Advanced)
    }
}
