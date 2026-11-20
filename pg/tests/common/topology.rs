use crate::common::{
    self,
    port::Reply,
    wire::checked::{Database, Group},
};
use cartel_pg::{
    api::{
        Client,
        results::Row,
        traits::{self, Client as _, Status as _},
    },
    port::{self, replies::Payload},
    protocol::logic,
    schema::{self, result},
};
use dope::{
    core::driver::{
        life::recovery,
        route::{Id, KeyTag},
        settings::errors::Claim,
    },
    manifold::{
        app::{
            self,
            env::{self, Bundle},
        },
        client::side::{Enabled, registry},
        network::client,
        service::{self, bindings, health, sync},
        timing::{self, Throughput},
    },
    net::wire::Identity,
    net::{Transport, tcp::Tcp},
    runtime::{executor::session, topology::Prepared},
};
use dope_fiber::BlockOn as _;
use dope_test::{harness::GUARD, harness::peer::TcpScript, reclaim::Reports};
use std::{marker::PhantomData, net::SocketAddr, pin::Pin, time::Duration};

#[pin_project::pin_project]
#[derive(dope::Application)]
#[dispatcher(driver = 'd)]
pub struct App<'d, C, L> {
    #[pin]
    #[app]
    connector: C,
    #[pin]
    #[manifold]
    listener: L,
    #[dispatcher(state)]
    scope: PhantomData<&'d ()>,
}

impl<'d, C, L> App<'d, C, L> {
    pub fn new(connector: C, listener: L) -> Self {
        Self {
            connector,
            listener,
            scope: PhantomData,
        }
    }

    pub fn connector(self: Pin<&Self>) -> Pin<&C> {
        self.project_ref().connector
    }
}

#[derive(Debug)]
pub enum Error {
    Health(health::Error),
    Claim(Claim),
}

pub fn try_new<I: port::Instance, E>(
    plan: port::Plan<I>,
    address: <E::Transport as Transport>::Addr,
    base: Duration,
    maximum: Duration,
) -> Result<Prepared<(), port::Node<I, 0, E>>, Error>
where
    E: env::Spec<Wire = Identity>,
    E::Transport: Transport<Addr: Clone>,
{
    let snapshot = service::Snapshot::from_one(
        service::Revision::new(1),
        service::Endpoint::new(address.clone(), address),
    );
    plan.assemble::<E>(snapshot, base, maximum)
        .map_err(Error::Health)?
        .for_install::<Throughput>()
        .map_err(Error::Claim)
}

pub(super) type Host<'q, 'h, 'p, 's, 'dg, 'dr, 'ds, 'b, 'c, 'd, I, Timing = Throughput> = app::Host<
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
        Bundle<Tcp, Identity, Throughput, Timing>,
        16,
        Enabled<&'s registry::Registry<'d, port::Backend<'d, 0>, KeyTag<0>>>,
    >,
>;

pub fn run<I: port::Instance + 'static, Timing: timing::Policy + 'static, R>(
    plan: port::Plan<I>,
    address: SocketAddr,
    (base_ms, max_ms): (u64, u64),
    _: Timing,
    scenario: impl for<'app, 'life, 'q, 'h, 'p, 's, 'dg, 'dr, 'ds, 'b, 'c, 'd> FnOnce(
        session::App<'app, 'life, 'd, Host<'q, 'h, 'p, 's, 'dg, 'dr, 'ds, 'b, 'c, 'd, I, Timing>>,
    ) -> R,
) -> R {
    let prepared = try_new::<_, Bundle<Tcp, Identity, Throughput, Timing>>(
        plan,
        address,
        Duration::from_millis(base_ms),
        Duration::from_millis(max_ms),
    )
    .expect("PostgreSQL topology");
    let mut reports = Reports::new(prepared.layout(), [Id::fixed::<0>()]);
    let mut completion = session::Completion::new();
    let result = prepared
        .with_local(
            |worker| {
                worker.lend(|node, stage| {
                    node.install(stage, |host, stage| {
                        stage.with_quiesce(&mut completion, host, GUARD, scenario)
                    })
                })
            },
            recovery::Policy::new(|event| reports.push(event), common::recover),
            |entry| entry.map_err(common::entry_error).expect("PostgreSQL entry"),
        )
        .expect("PostgreSQL installation");
    assert!(
        result.is_ok(),
        "PostgreSQL session: {:?}",
        result.as_ref().err().map(session::Error::cause)
    );
    let result = result.ok().expect("PostgreSQL session");
    let events = reports.events().expect("native PostgreSQL reclamation");
    assert!(events.is_empty(), "unexpected PostgreSQL reclamation: {reports:?}");
    result
}

pub fn with_row<Q>(bytes: &[u8], check: impl for<'b, 'c, 'd> FnOnce(Row<Payload<'b, 'c, 'd>, Q>))
where
    Q: schema::TypedQuery<Group = Group, Result = result::Rows>,
    for<'p> Q::Params<'p>: Default,
{
    TcpScript::with(
        |socket| Database::serve_rows(socket, [bytes]),
        |server| {
            let login = cartel_pg::Config::new("user".into(), "password".into(), "database".into())
                .expect("database credentials");
            let config = Database::config().expect("bounded PostgreSQL config");
            let plan = port::Port::<Database>::plan(login, config);
            run(plan, server.addr(), (1, 1_000), Throughput, |mut app| {
                let client: Client<
                    cartel_pg::api::Context<
                        '_,
                        '_,
                        '_,
                        '_,
                        '_,
                        '_,
                        '_,
                        _,
                        Database,
                    >,
                > = app.root_client();
                app.block_on(client.status().wait_ready())
                    .expect("driver ready")
                    .ok()
                    .expect("PostgreSQL ready");
                let row = app
                    .block_on(traits::Query::run_one::<Q>(
                        client,
                        Default::default(),
                    ))
                    .expect("driver query")
                    .map_err(|error| format!("{:?}", Reply::from(&error)))
                    .expect("query row");
                check(row)
            });
            server
                .finish()
                .expect("script execution")
                .expect("PostgreSQL script");
        },
    )
    .expect("script listener")
}
