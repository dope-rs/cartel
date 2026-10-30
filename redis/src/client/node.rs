use dope::core::driver::route;
use dope::core::driver::settings;
use dope::core::driver::settings::{claim, errors};
use dope::core::driver::storage;
use dope::manifold;
use dope::manifold::app;
use dope::manifold::app::env;
use dope::manifold::client::session;
use dope::manifold::network::client::Pool;
use dope::manifold::network::client::topology::plan;
use dope::manifold::service::bindings;
use dope::manifold::service::health;
use dope::manifold::service::sync;
use dope::net;
use dope::net::wire::Identity;
use dope::net::wire::domain;
use dope::runtime::executor;
use dope::runtime::topology::{self, lending};
use lending::Compose;

use crate::client::{
    self,
    plan::Error,
    settings::{Config, init},
};

type Host<'q, 'h, 'r, 's, 'storage, 'b, 'c, 'n, 'd, const ID: u8, E> = app::Host<
    Pool<
        'q,
        'h,
        'n,
        'd,
        'd,
        route::KeyTag<ID>,
        { client::CONNECTOR_CONNECTION_LIMIT },
        bindings::Bound<
            'r,
            'd,
            crate::Port<'s, 'storage, 'b, 'c, 'd, ID>,
            client::Logic<'s, 'd>,
            route::KeyTag<ID>,
            { client::CONNECTOR_CONNECTION_LIMIT },
        >,
        sync::Replace,
        <<E as env::Spec>::Transport as net::Transport>::Addr,
        E,
        16,
    >,
>;

/// Owns Redis storage until the installed application's consumer returns.
pub struct Node<const ID: u8, E: env::Spec>
where
    E::Transport: net::Transport,
{
    config: Config,
    set: client::Snapshot<E>,
    plan: plan::Plan<route::KeyTag<ID>, { client::CONNECTOR_CONNECTION_LIMIT }>,
    init: init::Plan,
}

impl<const ID: u8, E: env::Spec> Node<ID, E>
where
    E::Transport: net::Transport,
{
    pub(super) const fn new(
        config: Config,
        set: client::Snapshot<E>,
        plan: plan::Plan<route::KeyTag<ID>, { client::CONNECTOR_CONNECTION_LIMIT }>,
        init: init::Plan,
    ) -> Self {
        Self {
            config,
            set,
            plan,
            init,
        }
    }

    /// Retains the dependent plan and its operation until this provider is installed.
    pub fn compose<S, T, Q, F, R>(
        self,
        storage: S,
        tail: T,
        next: F,
    ) -> topology::Plan<S, (Self, lending::Composition<T, F>)>
    where
        S: storage::Plan,
        E: env::Spec<Wire = Identity>,
        <E::Transport as net::Transport>::Addr: Eq,
        F: for<'stage, 'scope, 'life, 'p, 'd, 'q, 'h, 'r, 's, 'storage, 'b, 'c, 'n> FnOnce(
            T,
            Host<'q, 'h, 'r, 's, 'storage, 'b, 'c, 'n, 'd, ID, E>,
            &mut lending::Stage<'stage, 'scope, 'life, 'p, 'd, S, Q>,
        )
            -> R,
    {
        topology::Plan::new(storage, (self, lending::Composition::new(tail, next)))
    }

    /// Keeps Redis installation storage alive through the application callback.
    pub fn install<'stage, 'scope, 'life: 'scope, 'p: 'life, 'd: 'p, S, Q, O>(
        self,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'p, 'd, S, Q>,
        next: impl for<'q, 'h, 'r, 's, 'storage, 'b, 'c, 'n> FnOnce(
            Host<'q, 'h, 'r, 's, 'storage, 'b, 'c, 'n, 'd, ID, E>,
            &mut lending::Stage<'stage, 'scope, 'life, 'p, 'd, S, Q>,
        ) -> O,
    ) -> Result<O, manifold::BuildError<Error>>
    where
        S: storage::Plan + 'd,
        Q: executor::Startup,
        E: env::Spec<Wire = Identity> + 'd,
        <E::Transport as net::Transport>::Addr: Eq,
    {
        let Self {
            config,
            set,
            plan,
            init,
        } = self;
        crate::Port::try_with(config, stage, lending::Stage::driver, |port, stage| {
            use session::ingress::{Bytes, Plan};

            let memory = config.memory_bound().ingress;
            let ingress = Plan::local(Bytes::from_limit(memory.retained), memory.owners);
            let session = port.bind(init);
            let setup = session::Setup::new(session, ingress);
            let wire = domain::Local::new(Identity);
            let hash = stage.hash_state(health::Domain::for_id(ID));
            let plan = plan.bind(wire, hash);
            plan.install::<_, _, sync::Replace, _, E, 16, _, _>(
                setup,
                set,
                stage,
                lending::Stage::driver,
                |pool, stage| next(app::Host::new(pool), stage),
            )
            .map_err(manifold::BuildError::Create)
        })
        .map_err(manifold::BuildError::Factory)?
    }
}

impl<const ID: u8, E: env::Spec> settings::Resource for Node<ID, E>
where
    E::Transport: net::Transport,
{
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        self.plan.include_in(needs)
    }
}

impl<'stage, 'scope, 'life, 'p, 'd, S, Q, N, O, const ID: u8, E>
    lending::Installer<lending::Stage<'stage, 'scope, 'life, 'p, 'd, S, Q>, N> for Node<ID, E>
where
    S: storage::Plan,
    Q: executor::Startup,
    E: env::Spec<Wire = Identity> + 'd,
    E::Transport: net::Transport,
    <E::Transport as net::Transport>::Addr: Eq,
    N: for<'q, 'h, 'r, 's, 'storage, 'b, 'c, 'n> lending::Next<
            lending::Stage<'stage, 'scope, 'life, 'p, 'd, S, Q>,
            Host<'q, 'h, 'r, 's, 'storage, 'b, 'c, 'n, 'd, ID, E>,
            Output = O,
        >,
{
    type Output = O;
    type Error = manifold::BuildError<Error>;

    fn install(
        self,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'p, 'd, S, Q>,
        next: N,
    ) -> Result<O, Self::Error> {
        self.install(stage, |app, stage| next.run(app, stage))
    }
}

impl<S, T, Q, F, R, const ID: u8, E> lending::ComposeFrom<S, lending::Composition<T, F>, Q>
    for Node<ID, E>
where
    S: storage::Plan,
    Q: executor::Startup,
    E: env::Spec<Wire = Identity>,
    E::Transport: net::Transport,
    <E::Transport as net::Transport>::Addr: Eq,
    F: for<'stage, 'scope, 'life, 'p, 'd, 'q, 'h, 'r, 's, 'storage, 'b, 'c, 'n> FnOnce(
        T,
        Host<'q, 'h, 'r, 's, 'storage, 'b, 'c, 'n, 'd, ID, E>,
        &mut lending::Stage<'stage, 'scope, 'life, 'p, 'd, S, Q>,
    ) -> R,
{
    type Output = R;
    type Error = manifold::BuildError<Error>;

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
        self.install(stage, |host, stage| tail.compose(host, stage))
    }
}
