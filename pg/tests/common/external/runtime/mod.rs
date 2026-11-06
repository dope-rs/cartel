use cartel_pg::api::traits::{Client as _, Status as _, Unchecked as _};
use dope::core::driver::life::recovery;
pub mod fixtures;
pub mod limits;
use std::{
    convert::Infallible,
    iter, marker,
    mem::MaybeUninit,
    net::SocketAddr,
    num::NonZero,
    ops::ControlFlow::{self, Break, Continue},
    time::Duration,
};

use crate::common::{env, port::Reply, topology};
use cartel_pg::{api, port, port::config, protocol::logic, schema};
use dope::runtime::topology::{Worker, error, ops};
use dope::{
    core::{
        driver::route::{Id, KeyTag},
        io::Reclaimed,
    },
    manifold::{
        BuildError, app,
        app::env::Bundle,
        client::side::{Enabled, registry},
        network::client,
        service::{bindings, sync},
        timing::Throughput,
    },
    net::{tcp::Tcp, wire::Identity},
    runtime::executor::{drive, session},
};
use dope_fiber::{BlockOn as _, abi::Ready};
use dope_test::{
    harness::GUARD,
    reclaim::{self, Reports},
};
use limits::Limits;
use o3::{base::time::Positive, mem::slot};

type Step<R> = Result<ControlFlow<R>, drive::Error>;
type Outcome<R> = (Step<R>, Step<R>);
type Output<'r, R> =
    Result<Result<Outcome<R>, session::Error<'r, Outcome<R>>>, BuildError<port::Build>>;

#[derive(Debug)]
pub enum Error<R = ()> {
    Environment(env::Error),
    Frames(NonZero<usize>),
    Config(config::Error),
    Identifier(cartel_pg::ParseError),
    Prepare(topology::Error),
    Run {
        output: ControlFlow<R, R>,
        reports: Result<Vec<Reclaimed>, reclaim::Error>,
    },
}

#[derive(Debug)]
pub enum Check<'a, 'd, R = ()> {
    Run(Result<&'a Output<'a, R>, error::Kind<'a, Infallible, &'a Output<'a, R>>>),
    Reply {
        sql: Option<&'a str>,
        prior: Option<&'a Step<R>>,
        result: Result<&'a (), Reply<'a, 'd>>,
    },
}

impl<R> Check<'_, '_, R> {
    pub fn is_ok(&self) -> bool {
        match self {
            Self::Run(result) => matches!(result, Ok(Ok(Ok((Ok(Continue(())), Ok(Continue(()))))))),
            Self::Reply { result, .. } => result.is_ok(),
        }
    }
}

pub fn check(result: Check<'_, '_>) {
    assert!(result.is_ok(), "{result:?}");
}

type Host<'q, 'h, 'p, 's, 'dg, 'dr, 'ds, 'b, 'c, 'd, I> = app::Host<
    client::Pool<
        'q,
        'h,
        'p,
        'd,
        'd,
        KeyTag<0>,
        { u32::MAX as usize },
        bindings::Bound<
            's,
            'd,
            port::Port<'s, 'dg, 'dr, 'ds, 'b, 'c, 'd, I>,
            logic::Session,
            KeyTag<0>,
            { u32::MAX as usize },
        >,
        sync::Replace,
        SocketAddr,
        Bundle<Tcp, Identity, Throughput>,
        16,
        Enabled<&'s registry::Registry<'d, port::Backend<'d, 0>, KeyTag<0>>>,
    >,
>;

type Handle<'app, 's, 'dg, 'dr, 'ds, 'b, 'c, 'd, I> = api::Client<
    'app,
    'dg,
    'dr,
    'ds,
    'b,
    'c,
    'd,
    bindings::Bound<
        's,
        'd,
        port::Port<'s, 'dg, 'dr, 'ds, 'b, 'c, 'd, I>,
        logic::Session,
        KeyTag<0>,
        { u32::MAX as usize },
    >,
    I,
>;

pub struct Harness<'app, 'life, 'q, 'h, 'p, 's, 'dg, 'dr, 'ds, 'b, 'c, 'd, I: schema::QuerySet + 'd>
{
    pub runtime: session::App<'app, 'life, 'd, Host<'q, 'h, 'p, 's, 'dg, 'dr, 'ds, 'b, 'c, 'd, I>>,
    client: Handle<'app, 's, 'dg, 'dr, 'ds, 'b, 'c, 'd, I>,
    schema: String,
}

impl<'app, 'life, 'q, 'h, 'p, 's, 'dg, 'dr, 'ds, 'b, 'c, 'd, I: schema::QuerySet + 'd>
    Harness<'app, 'life, 'q, 'h, 'p, 's, 'dg, 'dr, 'ds, 'b, 'c, 'd, I>
{
    pub fn client(&self) -> Handle<'app, 's, 'dg, 'dr, 'ds, 'b, 'c, 'd, I> {
        self.client
    }

    fn run<R>(
        mut self,
        callback: impl FnOnce(&mut Self) -> Step<R>,
        check: &impl Fn(Check<'_, '_, R>) -> R,
        cleanup: bool,
    ) -> Outcome<R> {
        let client = self.client;
        let body = self
            .runtime
            .block_on(Ready::new(()))
            .and_then(|()| callback(&mut self));
        let cleanup = if cleanup && !client.status().is_failed() {
            let prior = &body;
            let sql = format!("DROP SCHEMA IF EXISTS {} CASCADE", self.schema);
            self.runtime.block_on(dope_fiber::fiber!('_ => async move {
                let result = client.unchecked().command(&sql).await;
                let verdict = check(Check::Reply {
                    sql: Some(&sql),
                    prior: Some(prior),
                    result: result.as_ref().map_err(Reply::from),
                });
                match result {
                    Ok(()) => Continue(()),
                    Err(_) => Break(verdict),
                }
            }))
        } else {
            Ok(Continue(()))
        };
        (body, cleanup)
    }

    fn try_ready<R>(&mut self, check: &impl Fn(Check<'_, '_, R>) -> R) -> Step<R> {
        let client = self.client();
        let result = self.runtime.block_on(client.status().wait_ready())?;
        let verdict = check(Check::Reply {
            sql: None,
            prior: None,
            result: result.as_ref().map_err(Reply::from),
        });
        Ok(match result {
            Ok(()) => Continue(()),
            Err(_) => Break(verdict),
        })
    }
}

struct Run<'report, I, F, H> {
    schema: String,
    cleanup: bool,
    callback: F,
    check: H,
    finish: &'report mut session::Completion,
    marker: marker::PhantomData<I>,
}

impl<'report, 'scope, 'life, 'p, 'd, I, F, H, R>
    ops::Operation<
        'd,
        Worker<'scope, 'life, 'p, 'd, (), port::Node<I, 0, Bundle<Tcp, Identity, Throughput>>, ()>,
    > for Run<'report, I, F, H>
where
    I: schema::QuerySet,
    F: for<'app, 'owner, 'q, 'h, 'a, 's, 'dg, 'dr, 'ds, 'b, 'c, 'driver> FnOnce(
        &mut Harness<'app, 'owner, 'q, 'h, 'a, 's, 'dg, 'dr, 'ds, 'b, 'c, 'driver, I>,
    ) -> Step<R>,
    H: Fn(Check<'_, '_, R>) -> R,
{
    type Output = Output<'report, R>;

    fn run(
        self,
        worker: Worker<
            'scope,
            'life,
            'p,
            'd,
            (),
            port::Node<I, 0, Bundle<Tcp, Identity, Throughput>>,
            (),
        >,
    ) -> Self::Output
    where
        Self: 'd,
        I: 'd,
    {
        // Owning the schema marker connects I to this driver's scoped lifetime.
        let check = &self.check;
        let mut outcome = MaybeUninit::<Outcome<R>>::uninit();
        let slot = slot::Vacant::new(&mut outcome);
        let result = worker.lend(|node, stage| {
            node.install(stage, |host, stage| {
                stage.with_quiesce(self.finish, host, GUARD, |mut runtime| {
                    let client: api::Client<'_, '_, '_, '_, '_, '_, 'd, _, I> =
                        runtime.root_client();
                    slot.insert(
                        Harness {
                            runtime,
                            client,
                            schema: self.schema,
                        }
                        .run(self.callback, check, self.cleanup),
                    )
                })
            })
        })?;
        Ok(match result {
            Ok(output) => Ok(output.into_inner()),
            Err(session::Error::Quiesce {
                output,
                drive,
                disarm,
            }) => Err(session::Error::Quiesce {
                output: output.into_inner(),
                drive,
                disarm,
            }),
            Err(session::Error::Drive(cause)) => Err(session::Error::Drive(cause)),
            Err(session::Error::Install { cause, disarm }) => {
                Err(session::Error::Install { cause, disarm })
            }
        })
    }
}

pub struct Session<I> {
    address: SocketAddr,
    config: cartel_pg::Config,
    limits: Limits,
    schema: String,
    cleanup: bool,
    marker: marker::PhantomData<I>,
}

impl<I> Session<I> {
    pub const fn new(
        address: SocketAddr,
        config: cartel_pg::Config,
        maximum: NonZero<usize>,
        schema: String,
        cleanup: bool,
    ) -> Self {
        Self {
            address,
            config,
            limits: Limits::new(maximum),
            schema,
            cleanup,
            marker: marker::PhantomData,
        }
    }
}

impl<I: schema::QuerySet> Session<I> {
    pub fn try_run<R>(
        self,
        f: impl for<'app, 'life, 'q, 'h, 'p, 's, 'dg, 'dr, 'ds, 'b, 'c, 'd> FnOnce(
            &mut Harness<'app, 'life, 'q, 'h, 'p, 's, 'dg, 'dr, 'ds, 'b, 'c, 'd, I>,
        ) -> Result<
            (),
            drive::Error,
        >,
        recover: impl recovery::Recover + Copy,
        check: impl Fn(Check<'_, '_, R>) -> R,
    ) -> Result<R, Error<R>> {
        self.try_with(|rt| f(rt).map(Continue), recover, &check)
    }

    fn try_with<R>(
        self,
        f: impl for<'app, 'life, 'q, 'h, 'p, 's, 'dg, 'dr, 'ds, 'b, 'c, 'd> FnOnce(
            &mut Harness<'app, 'life, 'q, 'h, 'p, 's, 'dg, 'dr, 'ds, 'b, 'c, 'd, I>,
        ) -> Step<R>,
        recover: impl recovery::Recover + Copy,
        check: &impl Fn(Check<'_, '_, R>) -> R,
    ) -> Result<R, Error<R>> {
        let settings = self.limits.try_config()?;
        let name =
            cartel_pg::Identifier::try_from(&mut self.schema.clone()).map_err(Error::Identifier)?;
        let (output, reports) = {
            let prepared = topology::try_new::<_, Bundle<Tcp, Identity, Throughput>>(
                port::Port::<I>::plan(self.config.search_path(name), settings),
                self.address,
                Duration::from_millis(500),
                Duration::from_secs(512),
            )
            .map_err(Error::Prepare)?;
            let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
            let mut done = session::Completion::new();
            let output = prepared.enter_local(
                Run {
                    schema: self.schema,
                    cleanup: self.cleanup,
                    callback: f,
                    check,
                    finish: &mut done,
                    marker: self.marker,
                },
                recovery::Policy::new(|event| reports.push(event), recover),
                |outcome| {
                    let view = Check::Run(outcome.as_ref().map_err(|error| error.kind()));
                    let passed = view.is_ok();
                    let verdict = check(view);
                    if passed {
                        Continue(verdict)
                    } else {
                        Break(verdict)
                    }
                },
            );
            (output, reports.into_events())
        };
        match output {
            Continue(value) if reports.as_ref().is_ok_and(|events| events.is_empty()) => Ok(value),
            output => Err(Error::Run { output, reports }),
        }
    }
}

pub struct Plan<'a, I, B> {
    address: SocketAddr,
    config: cartel_pg::Config,
    boot: cartel_pg::Config,
    schema: &'a [&'a str],
    rows: &'a [&'a str],
    name: String,
    groups: Option<config::Groups>,
    types: marker::PhantomData<(I, B)>,
}

impl<'a, I, B> Plan<'a, I, B> {
    const fn new(
        address: SocketAddr,
        config: cartel_pg::Config,
        boot: cartel_pg::Config,
        schema: &'a [&'a str],
        rows: &'a [&'a str],
        name: String,
    ) -> Self {
        Self {
            address,
            config,
            boot,
            schema,
            rows,
            name,
            groups: None,
            types: marker::PhantomData,
        }
    }

    pub const fn with_groups(mut self, groups: config::Groups) -> Self {
        self.groups = Some(groups);
        self
    }
}

impl<I: schema::QuerySet, B: schema::QuerySet> Plan<'_, I, B> {
    fn try_seed<R>(
        address: SocketAddr,
        config: cartel_pg::Config,
        schema: &str,
        ddl: &[&str],
        rows: &[&str],
        recover: impl recovery::Recover + Copy,
        check: &impl Fn(Check<'_, '_, R>) -> R,
    ) -> Result<R, Error<R>> {
        Session::<B>::new(
            address,
            config,
            NonZero::<usize>::MIN,
            schema.to_string(),
            false,
        )
        .try_with(
            |rt| {
                if let Break(verdict) = rt.try_ready(check)? {
                    return Ok(Break(verdict));
                }
                let client = rt.client();
                let create = format!("CREATE SCHEMA {}", rt.schema);
                rt.runtime.block_on(dope_fiber::fiber!('_ => async move {
                    for sql in iter::once(create.as_str()).chain(ddl.iter().chain(rows).copied()) {
                        let result = client.unchecked().command(sql).await;
                        let verdict = check(Check::Reply {
                            sql: Some(sql),
                            prior: None,
                            result: result.as_ref().map_err(Reply::from),
                        });
                        if result.is_err() {
                            return Break(verdict);
                        }
                    }
                    Continue(())
                }))
            },
            recover,
            check,
        )
    }

    pub fn try_run<R>(
        self,
        f: impl for<'app, 'life, 'q, 'h, 'p, 's, 'dg, 'dr, 'ds, 'b, 'c, 'd> FnOnce(
            &mut Harness<'app, 'life, 'q, 'h, 'p, 's, 'dg, 'dr, 'ds, 'b, 'c, 'd, I>,
        ) -> Result<
            (),
            drive::Error,
        >,
        recover: impl recovery::Recover + Copy,
        check: impl Fn(Check<'_, '_, R>) -> R,
    ) -> Result<R, Error<R>> {
        self.try_conns(NonZero::<usize>::MIN, f, recover, check)
    }

    pub fn try_conns<R>(
        self,
        max: NonZero<usize>,
        f: impl for<'app, 'life, 'q, 'h, 'p, 's, 'dg, 'dr, 'ds, 'b, 'c, 'd> FnOnce(
            &mut Harness<'app, 'life, 'q, 'h, 'p, 's, 'dg, 'dr, 'ds, 'b, 'c, 'd, I>,
        ) -> Result<
            (),
            drive::Error,
        >,
        recover: impl recovery::Recover + Copy,
        check: impl Fn(Check<'_, '_, R>) -> R,
    ) -> Result<R, Error<R>> {
        self.try_limits(Limits::new(max), f, recover, &check)
    }

    pub fn try_inflight<R>(
        self,
        max: NonZero<usize>,
        inflight: NonZero<usize>,
        f: impl for<'app, 'life, 'q, 'h, 'p, 's, 'dg, 'dr, 'ds, 'b, 'c, 'd> FnOnce(
            &mut Harness<'app, 'life, 'q, 'h, 'p, 's, 'dg, 'dr, 'ds, 'b, 'c, 'd, I>,
        ) -> Result<
            (),
            drive::Error,
        >,
        recover: impl recovery::Recover + Copy,
        check: impl Fn(Check<'_, '_, R>) -> R,
    ) -> Result<R, Error<R>> {
        self.try_limits(Limits::new(max).inflight(inflight), f, recover, &check)
    }

    pub fn try_window<R>(
        self,
        window: Positive,
        f: impl for<'app, 'life, 'q, 'h, 'p, 's, 'dg, 'dr, 'ds, 'b, 'c, 'd> FnOnce(
            &mut Harness<'app, 'life, 'q, 'h, 'p, 's, 'dg, 'dr, 'ds, 'b, 'c, 'd, I>,
        ) -> Result<
            (),
            drive::Error,
        >,
        recover: impl recovery::Recover + Copy,
        check: impl Fn(Check<'_, '_, R>) -> R,
    ) -> Result<R, Error<R>> {
        self.try_limits(
            Limits::new(NonZero::<usize>::MIN).window(window),
            f,
            recover,
            &check,
        )
    }

    fn try_limits<R>(
        self,
        limits: Limits,
        f: impl for<'app, 'life, 'q, 'h, 'p, 's, 'dg, 'dr, 'ds, 'b, 'c, 'd> FnOnce(
            &mut Harness<'app, 'life, 'q, 'h, 'p, 's, 'dg, 'dr, 'ds, 'b, 'c, 'd, I>,
        ) -> Result<
            (),
            drive::Error,
        >,
        recover: impl recovery::Recover + Copy,
        check: &impl Fn(Check<'_, '_, R>) -> R,
    ) -> Result<R, Error<R>> {
        let address = self.address;
        let schema = self.name;
        Self::try_seed(
            address,
            self.boot,
            &schema,
            self.schema,
            self.rows,
            recover,
            check,
        )?;
        Session::<I> {
            address,
            config: self.config,
            limits: limits.groups(self.groups),
            schema,
            cleanup: true,
            marker: marker::PhantomData,
        }
        .try_with(
            |rt| match rt.try_ready(check)? {
                Continue(()) => f(rt).map(Continue),
                Break(verdict) => Ok(Break(verdict)),
            },
            recover,
            check,
        )
    }
}
