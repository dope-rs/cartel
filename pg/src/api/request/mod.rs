use crate::api::{sql, tx};
use crate::{api, port, protocol, schema};
use cartel_core::deadline;
use dope::core::driver::schedule::clock::timer;
use o3::buffer::{pool, write::Sink as _};
use port::requests;
use protocol::encode;
use std::marker::PhantomData;

#[derive(Clone, Copy)]
pub(super) struct Begin {
    isolation: tx::Isolation,
    access: tx::AccessMode,
    deferral: tx::Deferral,
}

impl Begin {
    pub(super) const fn new(
        level: tx::Isolation,
        access: tx::AccessMode,
        deferral: tx::Deferral,
    ) -> Self {
        Self {
            isolation: level,
            access,
            deferral,
        }
    }
}

#[derive(Clone, Copy)]
enum Quoted {
    Savepoint,
    ReleaseSavepoint,
    RollbackToSavepoint,
    Listen,
    Unlisten,
}

#[derive(Clone, Copy)]
enum Sql<'a> {
    Raw(&'a str),
    Begin(Begin),
    StatementTimeout(sql::Timeout),
    Quoted(Quoted, &'a str),
}

impl Sql<'_> {
    fn write(self, out: &mut encode::Writer<'_>) -> Result<(), encode::Error> {
        match self {
            Self::Raw(sql) => {
                if let Some(position) = sql.bytes().position(|byte| byte == 0) {
                    return Err(encode::Error::StatementSqlNul { position });
                }
                out.write_slice(sql.as_bytes())?;
            }
            Self::Begin(begin) => {
                out.write_slice(b"BEGIN")?;
                match begin.isolation {
                    tx::Isolation::Default => {}
                    tx::Isolation::ReadCommitted => {
                        out.write_slice(b" ISOLATION LEVEL READ COMMITTED")?;
                    }
                    tx::Isolation::RepeatableRead => {
                        out.write_slice(b" ISOLATION LEVEL REPEATABLE READ")?;
                    }
                    tx::Isolation::Serializable => {
                        out.write_slice(b" ISOLATION LEVEL SERIALIZABLE")?;
                    }
                }
                match begin.access {
                    tx::AccessMode::Default => {}
                    tx::AccessMode::ReadWrite => out.write_slice(b" READ WRITE")?,
                    tx::AccessMode::ReadOnly => out.write_slice(b" READ ONLY")?,
                }
                match begin.deferral {
                    tx::Deferral::Default => {}
                    tx::Deferral::Deferred => out.write_slice(b" DEFERRABLE")?,
                }
            }
            Self::StatementTimeout(timeout) => {
                out.write_slice(b"SET LOCAL statement_timeout TO ")?;
                out.write_u32(timeout.milliseconds())?;
            }
            Self::Quoted(cmd, name) => {
                match cmd {
                    Quoted::Savepoint => out.write_slice(b"SAVEPOINT \"")?,
                    Quoted::ReleaseSavepoint => {
                        out.write_slice(b"RELEASE SAVEPOINT \"")?;
                    }
                    Quoted::RollbackToSavepoint => {
                        out.write_slice(b"ROLLBACK TO SAVEPOINT \"")?;
                    }
                    Quoted::Listen => out.write_slice(b"LISTEN \"")?,
                    Quoted::Unlisten => out.write_slice(b"UNLISTEN \"")?,
                }
                let mut parts = name.split('"');
                if let Some(first) = parts.next() {
                    out.write_slice(first.as_bytes())?;
                }
                for part in parts.by_ref() {
                    out.write_slice(b"\"\"")?;
                    out.write_slice(part.as_bytes())?;
                }
                out.write_byte(b'"')?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(super) struct Request<'a> {
    sql: Sql<'a>,
    extra: Extra<'a>,
}

impl<'a> Request<'a> {
    pub(super) fn raw(sql: &'a str) -> Self {
        Self::raw_extra(sql, Extra::Plain)
    }

    pub(super) fn raw_extra(sql: &'a str, extra: Extra<'a>) -> Self {
        Self {
            sql: Sql::Raw(sql),
            extra,
        }
    }

    pub(super) fn begin(begin: Begin) -> Self {
        Self {
            sql: Sql::Begin(begin),
            extra: Extra::Plain,
        }
    }

    pub(super) fn timeout(timeout: sql::Timeout) -> Self {
        Self {
            sql: Sql::StatementTimeout(timeout),
            extra: Extra::Plain,
        }
    }

    pub(super) fn savepoint(name: &'a str) -> Self {
        Self::quoted(Quoted::Savepoint, name)
    }

    pub(super) fn release(name: &'a str) -> Self {
        Self::quoted(Quoted::ReleaseSavepoint, name)
    }

    pub(super) fn rollback(name: &'a str) -> Self {
        Self::quoted(Quoted::RollbackToSavepoint, name)
    }

    pub(super) fn listen(channel: &'a str) -> Self {
        Self::quoted(Quoted::Listen, channel)
    }

    pub(super) fn unlisten(channel: &'a str) -> Self {
        Self::quoted(Quoted::Unlisten, channel)
    }

    fn quoted(command: Quoted, name: &'a str) -> Self {
        Self {
            sql: Sql::Quoted(command, name),
            extra: Extra::Plain,
        }
    }

    fn write(self, out: &mut encode::Writer<'_>) -> Result<(), encode::Error> {
        out.parse_with(|out| self.sql.write(out))?;
        out.bind("", &[1], 0, &[1], |_| Ok(()))?;
        out.execute()?;
        match self.extra {
            Extra::Plain => out.sync()?,
            Extra::CopyIn { data } => {
                out.copy_data(data)?;
                out.copy_done()?;
                out.sync()?;
            }
            Extra::CopyInOpen => {}
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(super) enum Extra<'a> {
    Plain,
    CopyIn { data: &'a [u8] },
    CopyInOpen,
}

/// One encoded request retaining its exact publication boundary and owner state.
pub(super) struct Pending<'d, S = (), M = requests::Boundary> {
    pub(in crate::api) frame: requests::Complete<'d>,
    pub(in crate::api) boundary: M,
    pub(in crate::api) state: S,
}

impl<'d, S, M> Pending<'d, S, M> {
    pub(in crate::api) fn map_state<R>(self, map: impl FnOnce(S) -> R) -> Pending<'d, R, M> {
        Pending {
            frame: self.frame,
            boundary: self.boundary,
            state: map(self.state),
        }
    }
}

impl<'d> Pending<'d> {
    pub(in crate::api) fn typed<
        Q: schema::TypedQuery,
        I: schema::QuerySet,
        X: api::Sync,
        const ID: u8,
    >(
        port: &port::Port<'_, '_, '_, '_, '_, '_, 'd, I, ID>,
        params: Q::Params<'_>,
        frame: impl FnOnce(&pool::Pool) -> Option<pool::Span>,
    ) -> Result<Pending<'d, (), PhantomData<X>>, crate::Error> {
        if !schema::contains::<I, Q>() {
            return Err(crate::Error::QueryMetadata);
        }
        let frame = port.request().encode(frame(&port.requests), |frame| {
            schema::QueryMeta::validate::<Q>()?.write(frame, params)?;
            if X::SYNC { frame.sync() } else { Ok(()) }
        })?;
        Ok(Pending {
            frame,
            boundary: PhantomData,
            state: (),
        })
    }

    pub(in crate::api) fn group<'app, 'g, 'r, 's, 'b, 'c, Q, I, const ID: u8, const N: usize>(
        port: &'app port::Port<'app, 'g, 'r, 's, 'b, 'c, 'd, I, ID>,
        params: [Q::Params<'_>; N],
        frame: impl FnOnce(&pool::Pool) -> Option<pool::Span>,
    ) -> Result<
        Pending<'d, crate::port::replies::groups::Draft<'g, 'r, 's, 'b, 'c, 'd>>,
        crate::Error,
    >
    where
        Q: schema::TypedQuery,
        I: schema::QuerySet,
        'd: 'app,
    {
        if !schema::contains::<I, Q>() {
            return Err(crate::Error::QueryMetadata);
        }
        let access = port
            .diagnostics
            .as_ref()
            .ok_or(crate::Error::GroupDisabled)?;
        let ready = access.prepare()?;
        let frame = port.request().encode(frame(&port.requests), |frame| {
            let query = schema::QueryMeta::validate::<Q>()?;
            for params in params {
                query.write(frame, params)?;
            }
            frame.sync()
        })?;
        Ok(Pending {
            frame,
            boundary: requests::Boundary::Group,
            state: ready,
        })
    }

    pub(in crate::api) fn raw<I: schema::QuerySet, const ID: u8>(
        port: &port::Port<'_, '_, '_, '_, '_, '_, 'd, I, ID>,
        req: Request<'_>,
        frame: impl FnOnce(&pool::Pool) -> Option<pool::Span>,
    ) -> Result<Self, crate::Error> {
        let boundary = match (req.sql, req.extra) {
            (Sql::Begin(_), _) => requests::Boundary::Begin,
            (_, Extra::Plain | Extra::CopyIn { .. }) => requests::Boundary::Close,
            (_, Extra::CopyInOpen) => requests::Boundary::External,
        };
        let frame = port
            .request()
            .encode(frame(&port.requests), |frame| req.write(frame))?;
        Ok(Self {
            frame,
            boundary,
            state: (),
        })
    }

    pub(in crate::api) fn commit<I: schema::QuerySet, const ID: u8>(
        port: &port::Port<'_, '_, '_, '_, '_, '_, 'd, I, ID>,
    ) -> Result<Self, crate::Error> {
        let mut request = Self::raw(port, Request::raw("COMMIT"), pool::Pool::acquire)?;
        request.boundary = requests::Boundary::Commit;
        Ok(request)
    }
}

impl<Q: schema::TypedQuery> schema::Admission<Q> {
    fn write(
        &self,
        out: &mut encode::Writer<'_>,
        params: Q::Params<'_>,
    ) -> Result<(), encode::Error> {
        use protocol::value::encode::BindWriter;
        out.bind(
            Q::STATEMENT_NAME,
            Q::PARAM_FORMAT_CODES,
            self.count(),
            Q::RESULT_FORMAT_CODES,
            |out| {
                let mut values = BindWriter::new(out, self);
                Q::encode(params, &mut values)?;
                values.finish()
            },
        )?;
        out.execute()
    }
}

pub struct Acquired<'d> {
    pub(in crate::api) frame: pool::Span,
    pub(in crate::api) deadline: deadline::Guard<'d>,
}

pub(super) trait Input<'d> {
    type Clock: Into<Result<deadline::Guard<'d>, timer::Exhausted>> + Into<api::task::Budget<'d>>;

    fn into_parts(
        self,
    ) -> (
        impl FnOnce(&pool::Pool) -> Option<pool::Span>,
        impl FnOnce(deadline::Source<'d>) -> Self::Clock,
    );
}

impl<'d> Input<'d> for () {
    type Clock = deadline::Source<'d>;

    fn into_parts(
        self,
    ) -> (
        impl FnOnce(&pool::Pool) -> Option<pool::Span>,
        impl FnOnce(deadline::Source<'d>) -> Self::Clock,
    ) {
        (pool::Pool::acquire, std::convert::identity)
    }
}

impl<'d> Input<'d> for Acquired<'d> {
    type Clock = deadline::Guard<'d>;

    fn into_parts(
        self,
    ) -> (
        impl FnOnce(&pool::Pool) -> Option<pool::Span>,
        impl FnOnce(deadline::Source<'d>) -> Self::Clock,
    ) {
        (move |_| Some(self.frame), move |_| self.deadline)
    }
}
