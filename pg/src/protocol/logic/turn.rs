use cartel_core::reply::order;
use dope::{
    core::driver::route,
    manifold::client::{
        self, conn,
        session::{Input, response::Delivery, turns},
    },
    net::link::egress,
};
use o3::{
    base::cell::region,
    buffer::{self, hold},
};

use crate::{
    api::sql,
    port::{self, pool::key, replies, requests::Frame},
    protocol::{
        Reason, Wire, encode, frame,
        logic::{Pending, State, payloads, state},
        messages,
        runtime::tx::poll,
        scram,
    },
    reply::{self, server},
    schema::{self, desc, result},
};

pub(super) struct Turn<
    'turn,
    'life,
    'e,
    'g,
    'r,
    's,
    'b,
    'c,
    'd: 'life,
    I: schema::QuerySet,
    const ID: u8,
> {
    pub(super) port: &'life port::Port<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
    pub(super) conn: conn::Id<'d, route::KeyTag<ID>>,
    pub(super) state: &'turn mut State<'life, 'd, I, ID>,
    sink: egress::Queue<'turn, 'e, 'd, { client::IOV_CAP }, Frame<'d>>,
    pub(super) region: &'turn mut region::Token<region::Identity<'d>>,
}

impl<'turn, 'life, 'e, 'g, 'r, 's, 'b, 'c, 'd: 'life, I: schema::QuerySet, const ID: u8>
    Turn<'turn, 'life, 'e, 'g, 'r, 's, 'b, 'c, 'd, I, ID>
{
    pub(super) fn new(
        port: &'life port::Port<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        ctx: &'turn mut turns::Context<
            '_,
            'e,
            'd,
            State<'life, 'd, I, ID>,
            Frame<'d>,
            route::KeyTag<ID>,
        >,
    ) -> Self {
        let parts = ctx.parts();
        Self {
            port,
            conn: parts.conn_id,
            state: parts.state,
            sink: parts.sink,
            region: &mut parts.driver.into_regions().0,
        }
    }

    pub(super) fn from_startup(
        port: &'life port::Port<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        ctx: turns::Startup<'turn, 'e, 'd, State<'life, 'd, I, ID>, Frame<'d>, route::KeyTag<ID>>,
    ) -> Self {
        let conn = ctx.connection();
        *ctx.state = State::default();
        Self {
            port,
            conn,
            state: ctx.state,
            sink: ctx.sink,
            region: &mut ctx.driver.into_regions().0,
        }
    }

    pub(super) fn connect(mut self) {
        let frame = self.port.request().control(|frame| {
            frame.startup(
                self.port.shared.database.user(),
                self.port.shared.database.database(),
                "cartel-pg",
                self.port.shared.database.schema(),
                sql::Timeout::DEFAULT.milliseconds(),
            )
        });
        match frame.and_then(|frame| Self::enqueue(&self.sink, self.region, frame)) {
            Ok(()) => self.state.phase = state::Phase::StartupSent,
            Err(error) => self.fail(error),
        }
    }

    pub(super) fn try_flush(self, input: Input) -> Result<(), crate::Error> {
        if matches!(
            self.state.phase,
            state::Phase::Ready | state::Phase::CopyInActive | state::Phase::CopyOutActive
        ) {
            let lane = self
                .port
                .responses(self.conn)
                .ok_or(crate::Error::Protocol(Reason::ResponseUnknown))?;
            let receiving = match input {
                Input::Empty => false,
                Input::Partial => lane.front(self.state.batch, self.region)?.0.is_none(),
                Input::Yield | Input::Blocked => true,
            };
            self.port
                .shared
                .ready
                .set_receiving(self.conn, receiving, || lane.depth(self.region));
        }
        match &mut self.state.phase {
            state::Phase::Sasl { scram } => {
                Self::drive_scram(self.port, &self.sink, self.region, scram)?
            }
            state::Phase::Ready => {
                let result = self
                    .port
                    .request()
                    .try_flush(self.conn, self.region, |region| {
                        let frame = egress::NonEmpty::try_new(Frame::from(encode::SYNC.as_slice()))
                            .map_err(crate::Error::RequestLoad)?;
                        Self::enqueue(&self.sink, region, frame)
                    });
                if let Err(error) = result {
                    self.port
                        .connection()
                        .reconnect(self.conn, replies::Failure::from_error(error));
                }
            }
            state::Phase::NeedsStartup
            | state::Phase::StartupSent
            | state::Phase::AwaitingReady
            | state::Phase::Preparing { .. }
            | state::Phase::CopyInActive
            | state::Phase::CopyOutActive
            | state::Phase::Failed => {}
        }
        Ok(())
    }

    pub(super) fn response<'input, R: buffer::Cursor + hold::raw::Retain<'d>>(
        mut self,
        message: frame::Message<'input, 'd, R>,
    ) -> Delivery<Pending<'d>>
    where
        'd: 'input,
    {
        let frame::Message { typ, payload: data } = message;
        if typ == messages::Be::ERROR_RESPONSE {
            return match reply::Plan::parse(data) {
                Ok(plan) => self
                    .error(plan)
                    .map_or(Delivery::Committed, Delivery::Prepared),
                Err(error) => {
                    self.fail(error);
                    Delivery::Committed
                }
            };
        }
        if typ == messages::Be::NOTICE_RESPONSE {
            if let Err(error) = server::Fields::try_classify(data.wire()) {
                self.fail(crate::Error::Protocol(error));
            }
            return Delivery::Committed;
        }
        let result = match self.state.phase {
            state::Phase::StartupSent | state::Phase::AwaitingReady => {
                self.startup(typ, data.wire()).map(|()| None)
            }
            state::Phase::Sasl { .. } => self.handle_sasl(typ, data.wire()).map(|()| None),
            state::Phase::Preparing { remaining: prep } => {
                self.prepare(typ, data.wire(), prep).map(|()| None)
            }
            state::Phase::Ready | state::Phase::CopyInActive | state::Phase::CopyOutActive => {
                self.ready(typ, data)
            }
            state::Phase::NeedsStartup | state::Phase::Failed => Ok(None),
        };
        match result {
            Ok(next) => next.map_or(Delivery::Committed, Delivery::Prepared),
            Err(error) => {
                self.fail(error);
                Delivery::Committed
            }
        }
    }

    fn error<'input, R: buffer::Cursor + hold::raw::Retain<'d>>(
        mut self,
        plan: reply::Plan<'input, 'd, R>,
    ) -> Option<Pending<'d>>
    where
        'd: 'input,
    {
        let result = match self.state.phase {
            state::Phase::StartupSent
            | state::Phase::AwaitingReady
            | state::Phase::Sasl { .. }
            | state::Phase::Preparing { .. } => {
                let class = plan.class();
                let server = match plan.try_build() {
                    Ok(built) => built,
                    Err(error) => {
                        self.fail(error);
                        return None;
                    }
                };
                self.fail_server(class, server);
                return None;
            }
            state::Phase::Ready | state::Phase::CopyInActive | state::Phase::CopyOutActive => {
                self.reply_error(plan)
            }
            state::Phase::NeedsStartup | state::Phase::Failed => Ok(None),
        };
        match result {
            Ok(next) => next,
            Err(error) => {
                self.fail(error);
                None
            }
        }
    }

    fn enqueue(
        sink: &egress::Queue<'_, 'e, 'd, { client::IOV_CAP }, Frame<'d>>,
        region: &mut region::Token<region::Identity<'d>>,
        frame: egress::NonEmpty<Frame<'d>>,
    ) -> Result<(), crate::Error> {
        sink.try_insert(region, frame).map_err(|rejected| {
            crate::Error::RequestEgress(egress::Rejection::Capacity {
                charge: rejected.load.charge(),
                cause: rejected.cause,
            })
        })
    }

    fn startup(&mut self, typ: u8, data: Wire<'_>) -> Result<(), crate::Error> {
        match typ {
            messages::Be::AUTH => {
                if !matches!(self.state.phase, state::Phase::StartupSent) {
                    return Err(crate::Error::UnexpectedStartupMessage(typ));
                }
                match payloads::AuthRequest::decode(data)? {
                    payloads::AuthRequest::Ok => {
                        self.state.phase = state::Phase::AwaitingReady;
                        Ok(())
                    }
                    payloads::AuthRequest::Sasl { mechanisms: set } => {
                        let mode = set.pick()?;
                        let scram =
                            scram::Scram::new(&self.port.shared.scram, self.conn, |nonce| {
                                let frame = self.port.request().control(|frame| {
                                    frame.sasl_first(mode.as_str(), |frame| {
                                        frame.extend(b"n,,n=,r=")?;
                                        frame.extend(nonce)
                                    })
                                })?;
                                Self::enqueue(&self.sink, self.region, frame)
                            })?;
                        self.state.phase = state::Phase::Sasl { scram };
                        Ok(())
                    }
                    payloads::AuthRequest::SaslContinue { .. }
                    | payloads::AuthRequest::SaslFinal { .. } => {
                        Err(crate::Error::Auth(Reason::SaslContinue))
                    }
                    payloads::AuthRequest::Other(method) => {
                        Err(crate::Error::UnsupportedAuthMethod(method))
                    }
                }
            }
            messages::Be::PARAMETER_STATUS => {
                if !matches!(self.state.phase, state::Phase::AwaitingReady) {
                    return Err(crate::Error::UnexpectedStartupMessage(typ));
                }
                payloads::parameter(data)
            }
            messages::Be::BACKEND_KEY_DATA => {
                if !matches!(self.state.phase, state::Phase::AwaitingReady) {
                    return Err(crate::Error::Protocol(Reason::BackendKeyBeforeAuth));
                }
                let key = key::Key::decode(data).map_err(crate::Error::Protocol)?;
                self.port
                    .connection()
                    .try_install(self.conn, key)
                    .map_err(crate::Error::Protocol)
            }
            messages::Be::READY_FOR_QUERY => {
                if !matches!(self.state.phase, state::Phase::AwaitingReady)
                    || !matches!(payloads::ready_status(data)?, poll::Status::Idle)
                {
                    return Err(crate::Error::Protocol(Reason::ReadyUnknown));
                }
                let catalog = schema::Catalog::<I>::try_new()?;
                let set = I::PREPARE;
                let frame = egress::NonEmpty::try_new(Frame::from(set.wire()))
                    .map_err(crate::Error::RequestLoad)?;
                Self::enqueue(&self.sink, self.region, frame)?;
                self.state.phase = state::Phase::Preparing {
                    remaining: state::Prepare::new(catalog),
                };
                Ok(())
            }
            other => Err(crate::Error::UnexpectedStartupMessage(other)),
        }
    }

    fn handle_sasl(&mut self, typ: u8, data: Wire<'_>) -> Result<(), crate::Error> {
        if typ != messages::Be::AUTH {
            return Err(crate::Error::UnexpectedSaslMessage(typ));
        }
        let auth = payloads::AuthRequest::decode(data)?;
        match (&mut self.state.phase, auth) {
            (state::Phase::Sasl { scram }, payloads::AuthRequest::SaslContinue { data }) => {
                scram.begin(self.port.shared.database.password().as_bytes(), data)?;
                Self::drive_scram(self.port, &self.sink, self.region, scram)
            }
            (state::Phase::Sasl { scram }, payloads::AuthRequest::SaslFinal { data }) => {
                scram.verify_final(data)
            }
            (state::Phase::Sasl { scram }, payloads::AuthRequest::Ok) => {
                scram.finish()?;
                self.state.phase = state::Phase::AwaitingReady;
                Ok(())
            }
            _ => Err(crate::Error::Auth(Reason::SaslState)),
        }
    }

    fn drive_scram(
        port: &'life port::Port<'life, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        sink: &egress::Queue<'_, 'e, 'd, { client::IOV_CAP }, Frame<'d>>,
        region: &mut region::Token<region::Identity<'d>>,
        scram: &mut scram::Scram<'life, 'd, ID>,
    ) -> Result<(), crate::Error> {
        let Some(derive) = scram.deriving() else {
            return Ok(());
        };
        let Some((frame, sig)) = scram
            .ticket
            .poll(
                derive.key(),
                port.shared.database.password().as_bytes(),
                region,
                |wake| port.connection().wake(wake),
                |keys| {
                    port.request()
                        .secret(|frame| frame.sasl_final(|frame| derive.write_final(keys, frame)))
                },
            )
            .transpose()?
        else {
            return Ok(());
        };
        scram.await_final(sig);
        Self::enqueue(sink, region, frame)
    }

    fn prepare(
        &mut self,
        typ: u8,
        data: Wire<'_>,
        prep: state::Prepare<I>,
    ) -> Result<(), crate::Error> {
        use state::Prepare;

        if typ == messages::Be::PARAMETER_STATUS {
            return payloads::parameter(data);
        }
        let next = match (prep, typ) {
            (Prepare::LtreeParse { next }, messages::Be::PARSE_COMPLETE) => {
                payloads::empty(typ, data)?;
                Prepare::LtreeParameters { next }
            }
            (Prepare::QueryParse { cursor, registry }, messages::Be::PARSE_COMPLETE) => {
                payloads::empty(typ, data)?;
                Prepare::QueryParameters { cursor, registry }
            }
            (Prepare::LtreeParameters { next }, messages::Be::PARAMETER_DESCRIPTION) => {
                desc::Types::parameters(data.as_slice(), &[])?;
                Prepare::LtreeResult { next }
            }
            (
                Prepare::QueryParameters { cursor, registry },
                messages::Be::PARAMETER_DESCRIPTION,
            ) => {
                desc::Types::parameters(data.as_slice(), cursor.metadata().param_oids)?;
                Prepare::QueryResult { cursor, registry }
            }
            (Prepare::LtreeResult { next }, messages::Be::ROW_DESCRIPTION) => {
                Prepare::register(next, desc::Types::ltree_oid(data.as_slice())?)
            }
            (Prepare::QueryResult { cursor, registry }, messages::Be::ROW_DESCRIPTION) => {
                cursor.validate(data.as_slice(), registry)?;
                Prepare::resume(cursor.next(), registry)
            }
            (Prepare::LtreeResult { .. }, messages::Be::NO_DATA) => {
                return Err(crate::Error::Protocol(Reason::LtreeMissing));
            }
            (Prepare::QueryResult { cursor, registry }, messages::Be::NO_DATA) => {
                let query = cursor.metadata();
                if matches!(query.result, result::Kind::Rows) || !data.is_empty() {
                    return Err(crate::Error::Protocol(Reason::NoDataMismatch));
                }
                Prepare::resume(cursor.next(), registry)
            }
            (Prepare::Complete, messages::Be::READY_FOR_QUERY) => {
                if !matches!(payloads::ready_status(data)?, poll::Status::Idle) {
                    return Err(crate::Error::Protocol(Reason::ReadyUnknown));
                }
                let Some(depth) = self.port.depth(self.conn, self.region) else {
                    return Err(crate::Error::Protocol(Reason::PrepareReplies));
                };
                if depth != order::Depth::ZERO {
                    return Err(crate::Error::Protocol(Reason::PrepareRetained));
                }
                self.port
                    .connection()
                    .try_ready(self.conn)
                    .map_err(crate::Error::Protocol)?;
                self.state.phase = state::Phase::Ready;
                return Ok(());
            }
            (_, messages::Be::READY_FOR_QUERY) => {
                return Err(crate::Error::Protocol(Reason::ReadyBeforeDescription));
            }
            (_, other) => return Err(crate::Error::UnexpectedEagerPrepareMessage(other)),
        };
        self.state.phase = state::Phase::Preparing { remaining: next };
        Ok(())
    }
}
