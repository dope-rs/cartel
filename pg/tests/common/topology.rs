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
    schema::{self, result},
};
use dope::{
    core::driver::{life::recovery, route::Id, settings::errors::Claim},
    manifold::app::env::{self, Bundle},
    manifold::service::{self, health},
    manifold::timing::Throughput,
    net::wire::Identity,
    net::{Transport, tcp::Tcp},
    runtime::{executor::session::Completion, topology::Prepared},
};
use dope_fiber::BlockOn as _;
use dope_test::{harness::GUARD, harness::peer::TcpScript, reclaim::Reports};
use std::{marker::PhantomData, pin::Pin, time::Duration};

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

pub fn try_new<I: schema::QuerySet, E>(
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
            let ready = try_new::<_, Bundle<Tcp, Identity, Throughput>>(
                port::Port::<Database>::plan(login, config),
                server.addr(),
                Duration::from_millis(1),
                Duration::from_secs(1),
            )
            .expect("prepared PostgreSQL topology");
            let mut reports = Reports::<_>::new(ready.layout(), [Id::fixed::<0>()]);
            let mut report = Completion::new();
            ready
                .with_local(
                    |worker| {
                        worker.lend(|node, stage| {
                            node.install(stage, |host, stage| {
                                stage.with_quiesce(&mut report, host, GUARD, |mut app| {
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
                                })
                            })
                        })
                    },
                    recovery::Policy::new(|event| reports.push(event), common::recover),
                    |outcome| outcome.map_err(common::entry_error).expect("application entry"),
                )
                .expect("scoped PostgreSQL")
                .expect("application scope");
            assert!(
                reports.events().expect("native reclamation").is_empty(),
                "unexpected native recovery"
            );
            server
                .finish()
                .expect("script execution")
                .expect("PostgreSQL script");
        },
    )
    .expect("script listener")
}
