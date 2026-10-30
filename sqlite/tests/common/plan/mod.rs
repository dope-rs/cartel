use cartel_sqlite::{
    Catalog, Connection, HasQuery, Installed, Limits, Memory, Statement, TypedQuery,
};
use cartel_sqlite::{database, open, runtime};
use dope_core::driver::{
    life::recovery,
    settings::{self, demands::slots, errors},
};
use dope_manifold::timing::Throughput;
use dope_runtime::{
    executor::{boot, session},
    topology::{self, error},
};
use dope_test::fibers;

pub mod late;

#[derive(Debug)]
pub enum Error {
    Program,
    Progress,
    Lookaside,
    Runtime(runtime::Error),
    Plan(database::Error),
    Claim(errors::Claim),
    Client(cartel_sqlite::Error),
    Call(cartel_sqlite::Error),
    Run(fibers::Run),
}

#[derive(Debug)]
pub enum Failure {
    Enter(runtime::Error),
    Install(topology::InstallError<open::Error>),
}

#[derive(Debug)]
pub enum Cause<'a, R> {
    Enter(error::Kind<'a, runtime::Error, &'a Output<'a, R>>),
    Session(session::Error<'a, R>),
    Cleanup {
        output: Result<Result<R, session::Error<'a, R>>, Failure>,
        first: cartel_sqlite::Error,
        second: Option<cartel_sqlite::Error>,
    },
}

type Output<'a, R> = Result<Result<R, Failure>, Cause<'a, R>>;

pub struct Plan<'sql, I: Catalog> {
    ready: topology::Prepared<runtime::Layout<'sql, I>, open::Plan<I>>,
}

pub struct Polls<'a, R> {
    pub root: R,
    pub count: &'a mut usize,
}

impl<'d, R: boot::Root<'d>> boot::Root<'d> for Polls<'_, R> {
    type Output = R::Output;

    fn poll(self, context: boot::Context<'_, 'd>) -> boot::Poll<Self, Self::Output> {
        *self.count += 1;
        match self.root.poll(context) {
            boot::Poll::Pending(root) => boot::Poll::Pending(Self {
                root,
                count: self.count,
            }),
            boot::Poll::Ready(output) => boot::Poll::Ready(output),
        }
    }

    fn cancel(self) {
        self.root.cancel();
    }
}

impl<'sql, I: Catalog> Plan<'sql, I> {
    pub fn try_new(
        db: database::Target<'sql>,
        limits: Limits,
        memory: Memory,
    ) -> Result<Self, Error> {
        let layout = settings::Config::for_profile::<Throughput>();
        Self::prepare(db, limits, memory, layout)
    }

    pub fn try_with(
        db: database::Target<'sql>,
        limits: Limits,
        memory: Memory,
        cap: Option<slots::Slots>,
    ) -> Result<Self, Error> {
        let base = settings::Config::for_profile::<Throughput>();
        let layout = match cap {
            Some(cap) => {
                use settings::demands::Schedule;

                let clock = settings::Schedule::fixed::<0, 0>()
                    .resolve(Schedule::new(base.schedule().ready(), cap));
                base.set_schedule(clock)
            }
            None => base,
        };
        Self::prepare(db, limits, memory, layout)
    }

    pub const fn layout(&self) -> settings::Config {
        self.ready.layout()
    }

    pub fn try_run<F, R, Done>(
        self,
        op: F,
        report: impl recovery::Observe,
        finish: impl for<'a> FnOnce(Output<'a, Result<R, Error>>) -> Done,
    ) -> Done
    where
        F: for<'app, 'owner, 'local, 'd> FnOnce(
            &mut session::App<'app, 'owner, 'd, Installed<'local, 'd, I>>,
            Connection<'app, 'local, 'd, I>,
        ) -> Result<R, Error>,
    {
        self.try_app(
            move |app| {
                let conn = app.root_client().map_err(Error::Client)?;
                op(app, conn)
            },
            report,
            finish,
        )
    }

    pub fn try_app<F, R, Done>(
        self,
        op: F,
        report: impl recovery::Observe,
        finish: impl for<'a> FnOnce(Output<'a, R>) -> Done,
    ) -> Done
    where
        F: for<'app, 'owner, 'local, 'd> FnOnce(
            &mut session::App<'app, 'owner, 'd, Installed<'local, 'd, I>>,
        ) -> R,
    {
        let mut complete = session::Completion::new();
        let complete = &mut complete;
        self.ready.try_create(
            move |worker| {
                let mut cleanup = open::Completion::new();
                let output =
                    match worker.install_with(|open, stage| open.install(stage, &mut cleanup)) {
                        Ok(live) => Ok(live.with_app(complete, |mut app| op(&mut app))),
                        Err(error) => Err(Failure::Install(error)),
                    };
                match (output, cleanup.take()) {
                    (output, Err((first, second))) => Err(Cause::Cleanup {
                        output,
                        first,
                        second,
                    }),
                    (Ok(Ok(output)), Ok(())) => Ok(Ok(output)),
                    (Ok(Err(error)), Ok(())) => Err(Cause::Session(error)),
                    (Err(error), Ok(())) => Ok(Err(error)),
                }
            },
            report,
            |outcome| match outcome {
                Ok(output) => finish(output),
                Err(topology::EnterError::Storage(error)) => {
                    finish(Ok(Err(Failure::Enter(error.cause))))
                }
                Err(error) => finish(Err(Cause::Enter(error.kind()))),
            },
        )
    }

    pub fn try_install<Done>(
        self,
        report: impl recovery::Observe,
        finish: impl for<'a> FnOnce(Output<'a, ()>) -> Done,
    ) -> Done {
        self.try_app(|_| (), report, finish)
    }

    pub fn try_count<Q: TypedQuery, Done>(
        self,
        stmt: Statement<I, Q>,
        report: impl recovery::Observe,
        finish: impl for<'a> FnOnce(Output<'a, Result<i64, Error>>) -> Done,
    ) -> Done
    where
        I: HasQuery<Q>,
    {
        self.try_run(
            move |driver, mut conn| {
                fibers::TEST
                    .try_run(driver, conn.query().run_one::<i64, _, _>(stmt, ()))
                    .map_err(Error::Run)?
                    .map_err(Error::Call)
            },
            report,
            finish,
        )
    }

    fn prepare(
        db: database::Target<'sql>,
        limits: Limits,
        memory: Memory,
        layout: settings::Config,
    ) -> Result<Self, Error> {
        let plan = db.plan::<I>(limits, memory).map_err(Error::Plan)?;
        let ready = plan.with_config(layout).map_err(Error::Claim)?;
        Ok(Self { ready })
    }
}
