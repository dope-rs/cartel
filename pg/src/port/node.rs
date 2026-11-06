use std::marker;

use cartel_core::{deadline, queue, reply};
use client::topology::plan;
use dope::{
    core::driver::{
        route::KeyTag,
        settings,
        settings::{claim, errors},
        storage,
    },
    manifold,
    manifold::{
        app,
        app::env,
        client::{session, side, side::registry},
        network::client,
        service::{bindings, health, sync},
    },
    net,
    net::wire::{Identity, domain},
    runtime::{
        executor,
        topology::{self, lending},
    },
};
use o3::{buffer::pool, depot::arena::pool::shared};

use crate::{
    port::{self, backend::Storage, replies::groups::Access, requests},
    protocol::{self, logic},
    schema,
};

type Host<'q, 'h, 'a, 'g, 'r, 's, 'b, 'c, 'e, 'd, I, const ID: u8, E> = app::Host<
    client::Pool<
        'q,
        'h,
        'e,
        'd,
        'd,
        KeyTag<ID>,
        { port::CONNECTOR_CONNECTION_LIMIT },
        bindings::Bound<
            'a,
            'd,
            port::Port<'a, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
            logic::Session,
            KeyTag<ID>,
            { port::CONNECTOR_CONNECTION_LIMIT },
        >,
        sync::Replace,
        <<E as env::Spec>::Transport as net::Transport>::Addr,
        E,
        16,
        side::Enabled<&'a registry::Registry<'d, port::Backend<'d, ID>, KeyTag<ID>>>,
    >,
>;

/// Owns the protocol configuration through its complete installation scope.
pub struct Node<I, const ID: u8, E>
where
    E: env::Spec,
{
    settings: port::Plan<I, ID>,
    set: port::Snapshot<E>,
    plan: plan::Plan<KeyTag<ID>, { port::CONNECTOR_CONNECTION_LIMIT }, plan::Side>,
}

impl<I, const ID: u8, E> Node<I, ID, E>
where
    E: env::Spec,
{
    pub(super) const fn new(
        settings: port::Plan<I, ID>,
        set: port::Snapshot<E>,
        plan: plan::Plan<KeyTag<ID>, { port::CONNECTOR_CONNECTION_LIMIT }, plan::Side>,
    ) -> Self {
        Self {
            settings,
            set,
            plan,
        }
    }
}

impl<I, const ID: u8, E> settings::Resource for Node<I, ID, E>
where
    E: env::Spec,
{
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        self.plan.include_in(needs)
    }
}

impl<I, const ID: u8, E> Node<I, ID, E>
where
    E: env::Spec<Wire = Identity>,
    <E::Transport as net::Transport>::Addr: Eq,
{
    /// Retains the dependent plan and its operation until this provider is installed.
    pub fn compose<S, T, Q, F, R>(
        self,
        storage: S,
        tail: T,
        next: F,
    ) -> topology::Plan<S, (Self, lending::Composition<T, F>)>
    where
        S: storage::Plan,
        I: schema::QuerySet,
        F: for<'stage, 'scope, 'life, 'p, 'd, 'q, 'h, 'a, 'g, 'r, 's, 'b, 'c, 'e> FnOnce(
            T,
            Host<'q, 'h, 'a, 'g, 'r, 's, 'b, 'c, 'e, 'd, I, ID, E>,
            &mut lending::Stage<'stage, 'scope, 'life, 'p, 'd, S, Q>,
        )
            -> R,
    {
        topology::Plan::new(storage, (self, lending::Composition::new(tail, next)))
    }

    /// Borrows installation storage through the complete application callback.
    pub fn install<'stage, 'scope, 'life: 'scope, 'p: 'life, 'd: 'p, S, Q, O>(
        self,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'p, 'd, S, Q>,
        next: impl for<'q, 'h, 'a, 'g, 'r, 's, 'b, 'c, 'e> FnOnce(
            Host<'q, 'h, 'a, 'g, 'r, 's, 'b, 'c, 'e, 'd, I, ID, E>,
            &mut lending::Stage<'stage, 'scope, 'life, 'p, 'd, S, Q>,
        ) -> O,
    ) -> Result<O, manifold::BuildError<port::Build>>
    where
        S: storage::Plan + 'd,
        Q: executor::Startup,
        I: schema::QuerySet,
        E: 'd,
    {
        use net::link::egress;
        use protocol::runtime;
        use session::ingress;

        let Self {
            settings,
            set,
            plan,
        } = self;
        let port::Plan {
            database,
            config,
            queries: _,
        } = settings;
        let run = *config.runtime();
        let cap = config.side().primary();
        let window = config.operation_window;
        let (shared, requests, controls, cancel, buffer, queue, deadline, proof) = {
            let mut driver = stage.driver();
            let shared = runtime::Shared::try_new(
                database,
                config,
                driver.driver_ref(),
                driver.region_token().proof(),
            )
            .map_err(port::Build::Runtime)
            .map_err(manifold::BuildError::Factory)?;
            let requests = pool::Pool::try_from(run.request_pool())
                .map_err(port::Build::Requests)
                .map_err(manifold::BuildError::Factory)?;
            let controls = pool::Pool::try_from(run.control_pool())
                .map_err(port::Build::Requests)
                .map_err(manifold::BuildError::Factory)?;
            let cancel = registry::Registry::try_new(cap)
                .map_err(port::Build::Cancel)
                .map_err(manifold::BuildError::Factory)?;
            let buffer = Storage::try_new(cap)
                .map_err(port::Build::CancelBuffer)
                .map_err(manifold::BuildError::Factory)?;
            let queue = queue::Arena::<egress::NonEmpty<requests::Frame>>::try_new(
                run.queue(),
                driver.region_token(),
            )
            .map_err(port::Build::Queue)
            .map_err(manifold::BuildError::Factory)?;
            let deadline = deadline::Source::new(driver.driver_ref().timer(), window);
            (
                shared,
                requests,
                controls,
                cancel,
                buffer,
                queue,
                deadline,
                driver.region_token().proof(),
            )
        };
        reply::Storage::try_with(run.arena(), proof, |storage| {
            let mut records = match run.groups() {
                Some(layout) => {
                    let records = layout
                        .try_records()
                        .map_err(port::Build::GroupRecords)
                        .map_err(manifold::BuildError::Factory)?;
                    Some(records)
                }
                None => None,
            };
            let (groups, owners) = match records.as_mut() {
                Some(backing) => {
                    let (records, layout) = backing
                        .pool()
                        .map_err(port::Build::GroupRecordPool)
                        .map_err(manifold::BuildError::Factory)?;
                    let owners = shared::Store::try_new(layout.groups())
                        .map_err(port::Build::GroupOwners)
                        .map_err(manifold::BuildError::Factory)?;
                    Some(((records, layout), owners))
                }
                None => None,
            }
            .unzip();
            let access = match groups.as_ref().zip(owners.as_ref()) {
                Some(((records, layout), owners)) => {
                    let bytes = pool::Pool::try_from(layout.bytes())
                        .map_err(port::Build::GroupBytes)
                        .map_err(manifold::BuildError::Factory)?;
                    Some(Access::new(
                        bytes,
                        records.vacancies(),
                        owners.vacancies(),
                        layout.limits(),
                    ))
                }
                None => None,
            };
            storage
                .try_batches(|replies, batches| {
                    let queue = queue
                        .try_bind(replies.domain())
                        .map_err(port::Build::QueueDomain)
                        .map_err(manifold::BuildError::Factory)?;
                    let port = port::Port {
                        shared,
                        lifecycle: port::Lifecycle {
                            deadline,
                            cancel,
                            buffer,
                        },
                        request_queue: queue,
                        requests,
                        controls,
                        responses: replies,
                        batches,
                        diagnostics: access,
                        queries: marker::PhantomData,
                    };
                    let memory = port.config().memory_bound();
                    let ingress = ingress::Plan::local(
                        ingress::Bytes::from_limit(memory.ingress.retained),
                        memory.ingress.owners,
                    );
                    let session = bindings::Bound::bind(
                        &port,
                        logic::Session(protocol::Codec::new(&port.shared.config)),
                    );
                    let setup = session::Setup::new(session, ingress);
                    let mode = side::Enabled::new(&port.lifecycle.cancel);
                    let wire = domain::Local::new(Identity);
                    let hash = stage.hash_state(health::Domain::for_id(ID));
                    let plan = plan.bind(wire, hash);
                    plan.install::<_, _, _, sync::Replace, _, E, 16, _, _>(
                        setup,
                        mode,
                        set,
                        stage,
                        lending::Stage::driver,
                        |pool, stage| next(app::Host::new(pool), stage),
                    )
                    .map_err(manifold::BuildError::Create)
                })
                .map_err(port::Build::Replies)
                .map_err(manifold::BuildError::Factory)?
        })
        .map_err(port::Build::Replies)
        .map_err(manifold::BuildError::Factory)?
    }
}

impl<'stage, 'scope, 'life, 'p, 'd, S, Q, N, O, I, const ID: u8, E>
    lending::Installer<lending::Stage<'stage, 'scope, 'life, 'p, 'd, S, Q>, N> for Node<I, ID, E>
where
    S: storage::Plan,
    Q: executor::Startup,
    I: schema::QuerySet,
    E: env::Spec<Wire = Identity> + 'd,
    <E::Transport as net::Transport>::Addr: Eq,
    N: for<'q, 'h, 'a, 'g, 'r, 's, 'b, 'c, 'e> lending::Next<
            lending::Stage<'stage, 'scope, 'life, 'p, 'd, S, Q>,
            Host<'q, 'h, 'a, 'g, 'r, 's, 'b, 'c, 'e, 'd, I, ID, E>,
            Output = O,
        >,
{
    type Output = O;
    type Error = manifold::BuildError<port::Build>;

    fn install(
        self,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'p, 'd, S, Q>,
        next: N,
    ) -> Result<O, Self::Error> {
        self.install(stage, |host, stage| next.run(host, stage))
    }
}

impl<S, T, Q, F, R, I, const ID: u8, E> lending::ComposeFrom<S, lending::Composition<T, F>, Q>
    for Node<I, ID, E>
where
    S: storage::Plan,
    Q: executor::Startup,
    I: schema::QuerySet,
    E: env::Spec<Wire = Identity>,
    <E::Transport as net::Transport>::Addr: Eq,
    F: for<'stage, 'scope, 'life, 'p, 'd, 'q, 'h, 'a, 'g, 'r, 's, 'b, 'c, 'e> FnOnce(
        T,
        Host<'q, 'h, 'a, 'g, 'r, 's, 'b, 'c, 'e, 'd, I, ID, E>,
        &mut lending::Stage<'stage, 'scope, 'life, 'p, 'd, S, Q>,
    ) -> R,
{
    type Output = R;
    type Error = manifold::BuildError<port::Build>;

    fn compose<'scope, 'life, 'p, 'd>(
        self,
        tail: lending::Composition<T, F>,
        stage: &mut lending::Stage<'_, 'scope, 'life, 'p, 'd, S, Q>,
    ) -> Result<Self::Output, Self::Error>
    where
        'life: 'scope,
        'p: 'life,
        'd: 'p,
        Self: 'd,
        lending::Composition<T, F>: 'd,
        S: 'd,
        Q: 'd,
    {
        self.install(stage, |host, stage| {
            lending::Compose::compose(tail, host, stage)
        })
    }
}
