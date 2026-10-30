use std::{
    convert::Infallible,
    env, fmt,
    io::{self, Write as _},
    net::SocketAddr,
    process::{self, ExitCode},
    str,
};

use cartel_redis::{
    api::{Client, Connection as _},
    client::{
        plan,
        settings::{self, DEFAULT_BACKOFF, DEFAULT_BACKOFF_MAXIMUM, init},
    },
    value::source,
};
use dope::{
    core::driver::life::recovery,
    manifold::{app::env::Bundle, service, timing::Throughput},
    net::{tcp::Tcp, wire::Identity},
    runtime::executor::{drive, session},
};
use dope_fiber::BlockOn;

fn fail(message: fmt::Arguments<'_>) -> ExitCode {
    match writeln!(io::stderr().lock(), "{message}") {
        Ok(()) => ExitCode::FAILURE,
        Err(_) => ExitCode::from(2),
    }
}

fn stop<'a, 'd>(error: recovery::Recovery<'a, 'd>) -> Infallible {
    let code = fail(format_args!("Redis retirement: {:?}", error.cause()));
    process::exit(if code == ExitCode::from(2) { 2 } else { 1 });
}

fn main() -> ExitCode {
    let addr: SocketAddr = match env::var("REDIS_ADDR") {
        Ok(value) => match value.parse() {
            Ok(addr) => addr,
            Err(error) => return fail(format_args!("REDIS_ADDR {value:?}: {error}")),
        },
        Err(env::VarError::NotPresent) => SocketAddr::from(([127, 0, 0, 1], 6379)),
        Err(error) => return fail(format_args!("REDIS_ADDR: {error:?}")),
    };

    let config = match settings::Config::new(settings::Capacities {
        connections: 1,
        admission_waiters: 16,
        inflight: 256,
        request_frame_bytes: 64 * 1024,
        response_bytes: 64 * 1024 * 1024,
        response_values: 65_536,
        response_frame_bytes: 16 * 1024 * 1024,
        response_frame_values: 65_536,
    }) {
        Ok(config) => config,
        Err(error) => return fail(format_args!("Redis configuration: {error:?}")),
    };
    let snap = service::Snapshot::from_one(
        service::Revision::new(1),
        service::Endpoint::new(addr, addr),
    );
    let plan = match plan::Plan::<0>::new(config).assemble::<Bundle<Tcp, Identity, Throughput>>(
        snap,
        DEFAULT_BACKOFF,
        DEFAULT_BACKOFF_MAXIMUM,
        init::Plan::new(),
    ) {
        Ok(plan) => plan,
        Err(cause) => return fail(format_args!("Redis topology: {cause:?}")),
    };
    let plan = match plan.for_install::<Throughput>() {
        Ok(plan) => plan,
        Err(cause) => return fail(format_args!("Redis resource layout: {cause:?}")),
    };
    let mut failed = false;
    let mut done = session::Completion::new();
    let (plan, done) = (plan, &mut done);
    let result = plan.with_local(
        |worker| {
            worker.lend(|node, stage| {
                node.install(stage, |host, stage| {
                    stage.with_app(done, host, |mut app| probe(app.root_client(), &mut app))
                })
            })
        },
        recovery::Policy::new(
            |report| {
                failed |=
                    fail(format_args!("redis reclaimed done: {report:?}")) == ExitCode::from(2);
            },
            stop,
        ),
        |outcome| match outcome {
            Ok(Ok(Ok(Ok(code)))) => code,
            Ok(Ok(Ok(Err(cause)))) => fail(format_args!("Redis probe: {cause:?}")),
            Ok(Ok(Err(error))) => {
                let code = fail(format_args!("Redis session: {:?}", error.kind()));
                if code == ExitCode::from(2) {
                    return code;
                }
                match error.output() {
                    Some(Ok(code)) if *code != ExitCode::SUCCESS => *code,
                    Some(Ok(_) | Err(_)) | None => ExitCode::FAILURE,
                }
            }
            Ok(Err(cause)) => fail(format_args!("Redis installation: {cause:?}")),
            Err(error) => fail(format_args!("Redis runtime: {:?}", error.kind())),
        },
    );
    if failed { ExitCode::from(2) } else { result }
}

fn probe<'d, C: Client<'d>>(
    redis: C,
    app: &mut impl BlockOn<'d>,
) -> Result<ExitCode, drive::Error> {
    let probe = dope_fiber::fiber!('_ => async move {
        redis.wait_active().await.map_err(Error::Ready)?;
        redis.server().ping().await.map_err(Error::Ping)?;
        let id = {
            let mut conn = redis.connection().await.map_err(Error::Acquire)?;
            conn.client_id().await.map_err(Error::Identity)?
        };
        let info = redis.server().info(Some(b"server")).await.map_err(Error::Info)?;
        Ok::<_, Error<C::DriverError, C::Error>>((id, info))
    });
    match app.block_on(probe)? {
        Ok((id, info)) => {
            let text = match str::from_utf8(info.as_ref()) {
                Ok(text) => text,
                Err(error) => return Ok(fail(format_args!("Redis INFO UTF-8: {error}"))),
            };
            let ver = text
                .lines()
                .find(|line| line.starts_with("redis_version:"))
                .unwrap_or("redis_version: unknown");
            Ok(
                match writeln!(io::stdout().lock(), "client_id={id} {ver}") {
                    Ok(()) => ExitCode::SUCCESS,
                    Err(error) => fail(format_args!("Redis probe output: {error}")),
                },
            )
        }
        Err(error) => Ok(fail(format_args!("Redis probe: {:?}", error.diagnostic()))),
    }
}

#[derive(Debug)]
enum Error<D, E> {
    Ready(D),
    Ping(E),
    Acquire(D),
    Identity(E),
    Info(E),
}

impl<D: source::Control, E: source::Error<DriverError = D>> Error<D, E> {
    fn diagnostic(&self) -> impl fmt::Debug + '_ {
        match self {
            Self::Ready(error) => Error::Ready(error.diagnostic()),
            Self::Ping(error) => Error::Ping(error.diagnostic()),
            Self::Acquire(error) => Error::Acquire(error.diagnostic()),
            Self::Identity(error) => Error::Identity(error.diagnostic()),
            Self::Info(error) => Error::Info(error.diagnostic()),
        }
    }
}
