use cartel_pg::api::traits::{Client as _, Query as _, Status as _};
use dope::core::driver::life::recovery;
pub mod fixtures;
pub mod limits;
use std::{marker, mem::MaybeUninit, net::SocketAddr, num::NonZero, time::Duration};

use crate::common::{
    port::Reply,
    topology::{self, Host},
};
use cartel_pg::{api, port, port::config, protocol::logic, schema};
use dope::{
    core::{
        driver::route::{Id, KeyTag},
        io::Reclaimed,
    },
    manifold::{
        app::env::Bundle,
        service::bindings,
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
use o3::mem::slot;

type Step = Result<(), drive::Error>;

#[derive(Debug)]
pub enum Error {
    Frames(NonZero<usize>),
    Config(config::Error),
    Identifier(cartel_pg::ParseError),
    Prepare(topology::Error),
    Reclaim(Result<Vec<Reclaimed>, reclaim::Error>),
}

type Handle<'app, 's, 'dg, 'dr, 'ds, 'b, 'c, 'd, I> = api::Client<
    api::Context<
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
    >,
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

    fn run(mut self, callback: impl FnOnce(&mut Self) -> Step, cleanup: bool) -> (Step, Step) {
        let client = self.client;
        let body = self
            .runtime
            .block_on(Ready::new(()))
            .and_then(|()| callback(&mut self));
        let cleanup = if cleanup && !client.status().is_failed() {
            let prior = &body;
            let sql = format!("DROP SCHEMA IF EXISTS {} CASCADE", self.schema);
            self.runtime.block_on(dope_fiber::fiber!('_ => async move {
                let result = client.command(&sql).await;
                assert!(
                    result.is_ok(),
                    "{sql} after {prior:?}: {:?}",
                    result.as_ref().err().map(Reply::from),
                );
            }))
        } else {
            Ok(())
        };
        (body, cleanup)
    }

    fn try_ready(&mut self) -> Step {
        self.runtime
            .block_on(self.client.status().wait_ready())?
            .as_ref()
            .map_err(Reply::from)
            .expect("PostgreSQL ready");
        Ok(())
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

impl<I: schema::QuerySet + 'static> Session<I> {
    pub fn try_run(
        self,
        f: impl for<'app, 'life, 'q, 'h, 'p, 's, 'dg, 'dr, 'ds, 'b, 'c, 'd> FnOnce(
            &mut Harness<'app, 'life, 'q, 'h, 'p, 's, 'dg, 'dr, 'ds, 'b, 'c, 'd, I>,
        ) -> Step,
    ) -> Result<(), Error> {
        let settings = self.limits.try_config()?;
        let name =
            cartel_pg::Identifier::try_from(&mut self.schema.clone()).map_err(Error::Identifier)?;
        let prepared = topology::try_new::<_, Bundle<Tcp, Identity, Throughput>>(
            port::Port::<I>::plan(self.config.search_path(name), settings),
            self.address,
            Duration::from_millis(500),
            Duration::from_secs(512),
        )
        .map_err(Error::Prepare)?;
        let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
        let mut done = session::Completion::new();
        let mut output = MaybeUninit::uninit();
        let slot = slot::Vacant::new(&mut output);
        prepared.with_local(
            |worker| {
                worker.lend(|node, stage| {
                    node.install(stage, |host, stage| {
                        stage.with_quiesce(&mut done, host, GUARD, |mut runtime| {
                            let client = runtime.root_client();
                            slot.insert(
                                Harness {
                                    runtime,
                                    client,
                                    schema: self.schema,
                                }
                                .run(f, self.cleanup),
                            )
                        })
                    })
                })
            },
            recovery::Policy::new(|event| reports.push(event), crate::common::recover),
            |outcome| {
                let view = outcome.as_ref().map_err(|error| error.kind());
                assert!(
                    matches!(view, Ok(Ok(Ok(steps))) if matches!(**steps, (Ok(()), Ok(())))),
                    "PostgreSQL runtime: {view:?}",
                );
            },
        );
        match reports.into_events() {
            Ok(events) if events.is_empty() => Ok(()),
            reports => Err(Error::Reclaim(reports)),
        }
    }
}
