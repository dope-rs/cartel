use cartel_core::frame::Buffer;
use dope::{
    core::driver::{self, route},
    manifold::client::{IOV_CAP, close, conn, session::turns},
    net::link::egress,
};
use parser::frame;

use crate::{
    client::{
        self,
        settings::init::{Auth, Command, Plan, Step},
    },
    init,
    port::schedule::Ready,
    protocol::{self, parser},
    request::encode::write::init::Writer,
    value,
    value::source::Reader as _,
};

pub(in crate::client) struct Init {
    plan: Plan,
}

struct Turn<'a, 'e, 'd, const ID: u8> {
    conn: conn::Id<'d, route::KeyTag<ID>>,
    state: &'a mut protocol::ConnState,
    sink: egress::Queue<'a, 'e, 'd, IOV_CAP, Buffer>,
    driver: driver::Context<'a, 'd>,
    close: conn::CloseRequest<'a, 'd, route::KeyTag<ID>>,
}

impl<'a, 'e, 'd, const ID: u8> Turn<'a, 'e, 'd, ID> {
    fn from_context(
        ctx: &'a mut turns::Context<'_, 'e, 'd, protocol::ConnState, Buffer, route::KeyTag<ID>>,
    ) -> Self {
        let parts = ctx.parts();
        Self {
            conn: parts.conn_id,
            state: parts.state,
            sink: parts.sink,
            driver: parts.driver,
            close: parts.close,
        }
    }

    fn from_startup(
        ctx: turns::Startup<'a, 'e, 'd, protocol::ConnState, Buffer, route::KeyTag<ID>>,
    ) -> Self {
        let conn = ctx.connection();
        Self {
            conn,
            state: ctx.state,
            sink: ctx.sink,
            driver: ctx.driver,
            close: ctx.close,
        }
    }
}

impl Init {
    pub(in crate::client) const fn new(plan: Plan) -> Self {
        Self { plan }
    }

    fn send<'e, 's, 'b, 'c, 'd, const ID: u8>(
        &self,
        port: &crate::Port<'s, '_, 'b, 'c, 'd, ID>,
        command: Command<'_>,
        turn: &mut Turn<'_, 'e, 'd, ID>,
    ) -> Result<(), client::Failure<'d>> {
        let frame = port
            .encode(|out| match command {
                Command::Authenticate(auth) => match auth {
                    Auth::Password(pass) => Writer::new(out).auth(pass),
                    Auth::User {
                        username: user,
                        password: pass,
                    } => Writer::new(out).auth_user(user, pass),
                },
                Command::Select(database) => Writer::new(out).select(database),
                Command::Name(name) => Writer::new(out).set_name(name),
            })
            .map_err(client::Failure::Request)?;
        match turn
            .sink
            .try_insert(turn.driver.region_token(), frame.into_frame())
        {
            Ok(()) => Ok(()),
            Err(rejected) => Err(client::Failure::RequestEgress(
                egress::Rejection::Capacity {
                    charge: rejected.load.charge(),
                    cause: rejected.cause,
                },
            )),
        }
    }

    fn fail<'e, 's, 'b, 'c, 'd, const ID: u8>(
        port: &crate::Port<'s, '_, 'b, 'c, 'd, ID>,
        cause: client::Failure<'d>,
        turn: &mut Turn<'_, 'e, 'd, ID>,
    ) {
        let policy = cause.start_policy();
        turn.close
            .request(close::Directive::with_policy(cause.close_reason(), policy));
        port.fail(turn.conn, cause, turn.driver.region_token());
    }

    fn begin<'e, 's, 'b, 'c, 'd, const ID: u8>(
        &self,
        port: &crate::Port<'s, '_, 'b, 'c, 'd, ID>,
        command: Command<'_>,
        turn: &mut Turn<'_, 'e, 'd, ID>,
    ) {
        turn.state.begin_init(command.step());
        if let Err(error) = self.send(port, command, turn) {
            Self::fail(port, error, turn);
        }
    }

    fn ready<'e, 's, 'b, 'c, 'd, const ID: u8>(
        port: &crate::Port<'s, '_, 'b, 'c, 'd, ID>,
        turn: &mut Turn<'_, 'e, 'd, ID>,
    ) {
        if matches!(
            port.schedule
                .mark_ready(turn.conn, turn.driver.region_token(), &port.activity),
            Ready::Rejected
        ) {
            Self::fail(port, client::Failure::UnexpectedResponse, turn);
            return;
        }
        turn.state.mark_ready();
    }

    pub(super) fn response<'e, 's, 'b, 'c, 'd, const ID: u8>(
        &self,
        port: &crate::Port<'s, '_, 'b, 'c, 'd, ID>,
        step: Step,
        reply: frame::Stored<'s, 'd>,
        ctx: &mut turns::Context<'_, 'e, 'd, protocol::ConnState, Buffer, route::KeyTag<ID>>,
    ) {
        let mut turn = Turn::from_context(ctx);
        let mut reader = reply.reader();
        let kind = match reader.next_event() {
            Some(value::Event::Ok) => match reader.finish() {
                Ok(()) => None,
                Err(error) => {
                    Self::fail(port, client::Failure::Protocol(error), &mut turn);
                    return;
                }
            },
            Some(value::Event::Error(_)) => Some(init::Kind::Rejected),
            Some(_) | None => Some(init::Kind::Unexpected),
        };
        if let Some(kind) = kind {
            let frame = reply.with(|frame, _nodes| frame.into_inner());
            let cause = init::Failure {
                step,
                kind,
                frame: Ok(frame),
            };
            Self::fail(port, client::Failure::Handshake(cause), &mut turn);
            return;
        }
        match self.plan.next(step) {
            Some(next) => self.begin(port, next, &mut turn),
            None => Self::ready(port, &mut turn),
        }
    }

    pub(super) fn start<'e, 's, 'b, 'c, 'd, const ID: u8>(
        &self,
        port: &crate::Port<'s, '_, 'b, 'c, 'd, ID>,
        ctx: turns::Startup<'_, 'e, 'd, protocol::ConnState, Buffer, route::KeyTag<ID>>,
    ) {
        ctx.state.reset();
        let mut turn = Turn::from_startup(ctx);
        match self.plan.first() {
            Some(command) => self.begin(port, command, &mut turn),
            None => Self::ready(port, &mut turn),
        }
    }
}

const _: () = assert!(std::mem::size_of::<Init>() == std::mem::size_of::<Plan>());
