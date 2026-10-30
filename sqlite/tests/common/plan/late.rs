use std::{cell, convert, mem, ops, pin};

use cartel_sqlite::{Connection, Error, Installed};
use dope_core::driver::{self, schedule, settings};
use dope_fiber::abi::{Pending, Task};
use dope_runtime::{
    executor::{self, session},
    topology::lending,
};
use dope_test::fibers;

use crate::common::database::IntegrationDatabase;

pub struct Create<'mark>(pub &'mark cell::Cell<bool>);

#[derive(Debug)]
pub enum Failure {
    Missing,
    Run(fibers::Run),
}

pub struct Held<'app, 'local: 'app, 'd: 'local> {
    conn: cell::RefCell<Option<Connection<'app, 'local, 'd, IntegrationDatabase>>>,
    pending: cell::RefCell<Option<Box<dyn FnOnce() + 'app>>>,
}

impl<'app, 'local: 'app, 'd: 'local> executor::raw::Layout<'d> for Held<'app, 'local, 'd> {
    type ParkPlan = convert::Infallible;
}

// SAFETY: these ordinary fields own no routes or lifecycle delegates. The pinned
// pending task remains owned until this dependent application is destroyed.
unsafe impl<'app, 'local: 'app, 'd: 'local> executor::raw::App<'d> for Held<'app, 'local, 'd> {
    const ROUTES: settings::Routes = settings::Routes::EMPTY;
    const COORDS: usize = 0;

    fn select_park() -> Option<Self::ParkPlan> {
        None
    }

    /// # Safety
    /// The selected plan is uninhabited, so this operation cannot be invoked.
    unsafe fn park<'loan, 'driver, 'turn, 'root>(
        _: pin::Pin<&'loan mut Self>,
        plan: &mut Self::ParkPlan,
        _: executor::raw::Park<'driver, 'turn, 'root, 'd>,
    ) where
        'd: 'driver + 'turn + 'root,
    {
        match *plan {}
    }

    fn progress(
        _: pin::Pin<&Self>,
        _: executor::raw::Progress<'_, '_, 'd>,
    ) -> schedule::Progress<'d> {
        schedule::Progress::Quiescent
    }

    fn stopping(
        _: pin::Pin<&Self>,
        _: executor::raw::Progress<'_, '_, 'd>,
    ) -> schedule::Progress<'d> {
        schedule::Progress::Quiescent
    }
}

impl<'mark, 'local, 'd: 'local> lending::Build<'d, Installed<'local, 'd, IntegrationDatabase>>
    for Create<'mark>
{
    type App<'app>
        = Held<'app, 'local, 'd>
    where
        Self: 'app,
        Installed<'local, 'd, IntegrationDatabase>: 'app,
        'd: 'app;
    type Data = &'mark cell::Cell<bool>;
    type Error = Error;

    fn build<'app>(
        self,
        conn: Result<Connection<'app, 'local, 'd, IntegrationDatabase>, Error>,
        _: driver::Context<'_, 'd>,
    ) -> Result<(Self::App<'app>, Self::Data), Self::Error>
    where
        Self: 'app,
        Installed<'local, 'd, IntegrationDatabase>: 'app,
        'd: 'app,
    {
        Ok((
            Held {
                conn: cell::RefCell::new(Some(conn?)),
                pending: cell::RefCell::new(None),
            },
            self.0,
        ))
    }
}

impl<'mark, 'local, 'life, 'd: 'local + 'life, Q>
    lending::Execute<'life, 'd, Installed<'local, 'd, IntegrationDatabase>, Q> for Create<'mark>
{
    type Output = Result<(), Failure>;

    fn run<'app, 'run>(
        entered: Self::Data,
        mut app: session::App<
            'run,
            'life,
            'd,
            lending::raw::Bound<
                'app,
                'd,
                Installed<'local, 'd, IntegrationDatabase>,
                Self::App<'app>,
            >,
            Q,
        >,
    ) -> Self::Output
    where
        Self: 'app,
        Installed<'local, 'd, IntegrationDatabase>: 'app,
        'd: 'app,
        'app: 'run,
        'life: 'run,
    {
        let mut conn = app.inspect(
            |app| app.tail(),
            |tail| tail.conn.borrow_mut().take().ok_or(Failure::Missing),
        )?;
        let waiting = dope_fiber::fiber!('_ => async move {
            let mut tx = conn.transaction().await?;
            fibers::Yield::new().await;
            let seen = cell::Cell::new(false);
            let scan = tx.query()
                .stream::<i64, _, _>(IntegrationDatabase::ROWS_OVER_LIMIT, ())
                .visit(|_| {
                    seen.set(true);
                    ops::ControlFlow::Continue(())
                });
            let mut task = Task::new(scan);
            assert!(fibers::TEST.poll_once(pin::Pin::new(&mut task)).await.is_none());
            assert!(seen.get());
            mem::forget(task);
            entered.set(true);
            Pending::<()>::new().await;
            drop(tx);
            Ok::<(), Error>(())
        });
        let mut task = Box::pin(Task::new(waiting));
        let pending = &mut task;
        fibers::TEST
            .try_run(
                &mut app,
                dope_fiber::fiber!('_ => async move {
                    while !entered.get() {
                        assert!(fibers::TEST.poll_once(pending.as_mut()).await.is_none());
                    }
                }),
            )
            .map_err(Failure::Run)?;
        app.inspect(
            |app| app.tail(),
            |tail| {
                assert!(
                    tail.pending
                        .replace(Some(Box::new(move || drop(task))))
                        .is_none()
                );
            },
        );
        Ok(())
    }
}
