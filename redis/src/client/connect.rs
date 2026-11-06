//! One TCP attempt whose connection remains owned by the returned client.

use std::{marker::PhantomData, pin::Pin, task::Poll};

use dope::{
    core::driver::{
        self,
        route::KeyTag,
        settings as driver_settings,
        settings::{claim, errors},
    },
    manifold::{
        self,
        app::{self, creation, env},
        client::{
            attempt::{self, ConnectFault, queue, queue::mailbox},
            close,
            conn::engine,
            session,
        },
        network::client::topology::plan::Layout,
        service::bindings::{self, endpoint},
        timing::Balanced,
    },
    net::{
        tcp::Tcp,
        wire::{Identity, domain},
    },
    runtime::{client as runtime, topology::lending},
};
use dope_fiber::abi::{self, machine};
use o3::depot::capacity;

use crate::{
    api::Client as _,
    client::{
        self,
        settings::{Config, init},
    },
    port,
};

type Mailbox<'d, const ID: u8> = mailbox::Domain<'d, Tcp, (), Result<(), ConnectFault>, KeyTag<ID>>;
type Session<'r, 's, 'storage, 'b, 'c, 'd, const ID: u8> = bindings::Bound<
    'r,
    'd,
    Endpoint<'s, 'storage, 'b, 'c, 'd, ID>,
    client::Logic<'s, 'd>,
    KeyTag<ID>,
    { client::CONNECTOR_CONNECTION_LIMIT },
>;
type Machine<'p, 'r, 's, 'storage, 'b, 'c, 'd, const ID: u8> = engine::Machine<
    'p,
    'd,
    'd,
    KeyTag<ID>,
    session::app::State<'d, 'd, KeyTag<ID>, Session<'r, 's, 'storage, 'b, 'c, 'd, ID>, Identity>,
    queue::Control<'r, 'd, Tcp, KeyTag<ID>>,
    env::Bundle<Tcp, Identity, Balanced>,
>;

/// Admits exactly one physical connection and one consuming start capability.
pub struct Plan<const ID: u8 = 0> {
    config: Config,
    init: init::Plan,
    layout: Layout<KeyTag<ID>, 1>,
}

#[derive(Debug)]
pub enum CreateError<'d, const ID: u8 = 0> {
    Port(client::plan::Error),
    Native(creation::Error),
    Reserve(mailbox::Error<'d, KeyTag<ID>>),
}

pub enum Error<'d, const ID: u8 = 0> {
    Attempt {
        key: attempt::Id<'d, KeyTag<ID>>,
        cause: ConnectFault,
    },
    Ready(crate::Error<'d, ID>),
}

/// Opaque installation storage. Only its matching Start can open a connection.
pub struct Endpoint<'s, 'storage, 'b, 'c, 'd, const ID: u8> {
    port: crate::Port<'s, 'storage, 'b, 'c, 'd, ID>,
    pending: Mailbox<'d, ID>,
}

#[pin_project::pin_project]
#[derive(dope::Forward)]
#[repr(transparent)]
pub struct Connector<'p, 'r, 's, 'storage, 'b, 'c, 'd, const ID: u8 = 0> {
    #[pin]
    #[forward('d)]
    engine: Machine<'p, 'r, 's, 'storage, 'b, 'c, 'd, ID>,
}

impl<'p, 'r, 's, 'storage, 'b, 'c, 'd, const ID: u8> lending::Root<'d>
    for Connector<'p, 'r, 's, 'storage, 'b, 'c, 'd, ID>
{
    type Provider = Session<'r, 's, 'storage, 'b, 'c, 'd, ID>;

    fn provider(self: Pin<&Self>) -> Pin<&Self::Provider> {
        Pin::new(self.project_ref().engine.application().session())
    }
}

/// An affine reservation; installation alone submits no connection attempt.
pub struct Start<'b, 'r, 'd, const ID: u8 = 0> {
    vacant: mailbox::Vacant<'r, 'd, Tcp, (), Result<(), ConnectFault>, KeyTag<ID>>,
    scope: PhantomData<fn(&'b ()) -> &'b ()>,
}

/// Application authority for the exact installed source, without command access.
pub struct Contact<'app, 's, 'storage, 'b, 'c, 'd: 'app, B: ?Sized, const ID: u8> {
    root: &'app Endpoint<'s, 'storage, 'b, 'c, 'd, ID>,
    bound: runtime::Bound<'app, 'd, B>,
}

/// Owns cancellation of this connection, including its completed initialization.
pub struct Connected<'app, 'source, 's, 'storage, 'b, 'c, 'd: 'app, B: ?Sized, const ID: u8 = 0> {
    contact: Contact<'app, 's, 'storage, 'b, 'c, 'd, B, ID>,
    _attempt: queue::Lease<'source, 'd, Tcp, KeyTag<ID>>,
}

impl<const ID: u8> Plan<ID> {
    pub fn new(config: Config, init: init::Plan) -> Result<Self, errors::Claim> {
        Ok(Self {
            layout: Layout::new(config.connections())?,
            config,
            init,
        })
    }

    /// Keeps all protocol storage and the single attempt source alive together.
    pub fn try_with<'d, C, R>(
        self,
        state: &mut C,
        driver: impl for<'a> Fn(&'a mut C) -> driver::Context<'a, 'd>,
        run: impl for<'p, 'r, 's, 'storage, 'b, 'c> FnOnce(
            app::Host<Connector<'p, 'r, 's, 'storage, 'b, 'c, 'd, ID>>,
            Start<'b, 'r, 'd, ID>,
            &mut C,
        ) -> R,
    ) -> Result<R, CreateError<'d, ID>> {
        let Self {
            config,
            init,
            layout,
        } = self;
        crate::Port::try_with(config, state, &driver, |port, state| {
            let root = Endpoint {
                port,
                pending: Mailbox::try_new(capacity::Count::new(1), &mut driver(state).storage)
                    .map_err(CreateError::Native)?,
            };
            let start = Start {
                vacant: root.pending.try_reserve().map_err(CreateError::Reserve)?,
                scope: PhantomData,
            };
            let memory = config.memory_bound().ingress;
            let input = session::ingress::Plan::local(
                session::ingress::Bytes::from_limit(memory.retained),
                memory.owners,
            );
            let session = bindings::Bound::bind(&root, client::Logic::new(&root.port, init));
            let app = session::app::State::try_new(
                session::Setup::new(session, input),
                layout.connections(),
                &mut driver(state),
            )
            .map_err(CreateError::Native)?;
            engine::Startup::new(
                app,
                root.pending.source(),
                layout.connections(),
                config.egress(),
                domain::Local::new(Identity),
            )
            .try_with(state, &driver, |engine, state| {
                run(app::Host::new(Connector { engine }), start, state)
            })
            .map_err(CreateError::Native)
        })
        .map_err(CreateError::Port)?
    }
}

impl<const ID: u8> driver_settings::Resource for Plan<ID> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        driver_settings::Resource::include_in(&self.layout, needs)
    }
}

impl<'s, 'storage, 'b, 'c, 'd, const ID: u8> client::Root<'d, ID>
    for Endpoint<'s, 'storage, 'b, 'c, 'd, ID>
{
    type Port = crate::Port<'s, 'storage, 'b, 'c, 'd, ID>;
    type Failure = manifold::client::app::failure::Observed;

    fn port(&self) -> &crate::Port<'s, 'storage, 'b, 'c, 'd, ID> {
        &self.port
    }

    fn complete(
        &self,
        key: &attempt::Completion<'_, 'd, KeyTag<ID>>,
        result: Result<(), ConnectFault>,
    ) {
        if let Ok(((), Some(waiter))) = self.pending.try_settle(key, result) {
            waiter.wake();
        }
    }

    fn retired(&self, reason: close::Reason) {
        self.port.activity.record_stop(port::Stop::Closed(reason));
    }
}

impl<'r, 's, 'storage, 'b, 'c, 'd: 'r, const ID: u8>
    endpoint::Provider<'r, 'd, KeyTag<ID>, { client::CONNECTOR_CONNECTION_LIMIT }>
    for Endpoint<'s, 'storage, 'b, 'c, 'd, ID>
where
    Self: 'r,
{
    type Client<'app, B: ?Sized + 'app>
        = Contact<'app, 's, 'storage, 'b, 'c, 'd, B, ID>
    where
        'r: 'app,
        'd: 'app;

    fn provide<'app, B: ?Sized + 'app>(
        &'r self,
        bound: runtime::Bound<'app, 'd, B>,
    ) -> Self::Client<'app, B>
    where
        'r: 'app,
        'd: 'app,
    {
        Contact { root: self, bound }
    }
}

impl<'b, 'source, 'd, const ID: u8> Start<'b, 'source, 'd, ID> {
    pub fn connect<'app, 's, 'storage, 'c: 'b, B: ?Sized + 'app>(
        self,
        contact: Contact<'app, 's, 'storage, 'b, 'c, 'd, B, ID>,
        target: attempt::StreamTarget<Tcp>,
    ) -> impl abi::Fiber<
        'd,
        Output = Result<Connected<'app, 'source, 's, 'storage, 'b, 'c, 'd, B, ID>, Error<'d, ID>>,
    >
    where
        'd: 'c,
    {
        dope_fiber::fiber!('d => async move {
            let mut pending = machine::Machine::new(self.vacant.start((), target));
            let attempt = abi::poll_fn(move |cx| pending.step(|pending| {
                match pending.poll(cx.as_ref().completion()) {
                    mailbox::Poll::Pending(pending) => machine::Step::Suspend(pending, Poll::Pending),
                    mailbox::Poll::Ready(result, attempt) => {
                        let result = match result {
                            Ok(()) => Ok(attempt),
                            Err(cause) => Err(Error::Attempt { key: attempt.id(), cause }),
                        };
                        machine::Step::Complete(Poll::Ready(result))
                    }
                }
            })).await?;
            let connected = Connected { contact, _attempt: attempt };
            connected.redis().wait_active().await.map_err(Error::Ready)?;
            Ok(connected)
        })
    }
}

impl<'app, 'source, 's, 'storage, 'b, 'c, 'd: 'app, B: ?Sized, const ID: u8>
    Connected<'app, 'source, 's, 'storage, 'b, 'c, 'd, B, ID>
{
    pub fn redis(&self) -> client::Redis<'_, 's, 'b, 'c, 'd, B, ID> {
        endpoint::Provider::provide(&self.contact.root.port, self.contact.bound.borrow())
    }
}
