use std::{
    convert::Infallible,
    env::{self, VarError},
    fmt,
    io::{self, Write as _},
    net::{AddrParseError, IpAddr, SocketAddr},
    num::ParseIntError,
    process::{self, ExitCode},
    time::Duration,
};

use cartel_pg::{
    PgRow, PgTable,
    api::traits::{self, Stream as _},
    dsl::query,
    pg_instance,
    port::{self, config},
    query_group, reply,
};
use dope::{
    core::driver::{life::recovery, settings},
    manifold::{app::env::Bundle, service, timing::Throughput},
    net::{tcp::Tcp, wire::Identity},
    runtime::executor::{drive, session},
};
use dope_fiber::BlockOn;

enum InputError {
    Environment {
        variable: &'static str,
        cause: VarError,
    },
    Host {
        value: String,
        cause: AddrParseError,
    },
    Port {
        value: String,
        cause: ParseIntError,
    },
    Config(cartel_pg::ConfigError),
}

fn read_env(variable: &'static str, default: &str) -> Result<String, InputError> {
    match env::var(variable) {
        Ok(value) => Ok(value),
        Err(VarError::NotPresent) => Ok(default.to_owned()),
        Err(cause) => Err(InputError::Environment { variable, cause }),
    }
}

struct Input {
    address: SocketAddr,
    credentials: cartel_pg::Config,
}

impl Input {
    fn stop<'a, 'd>(failure: recovery::Recovery<'a, 'd>) -> Infallible {
        let status = fail(format_args!("pg native retirement: {:?}", failure.cause()));
        process::exit(if status == ExitCode::from(2) { 2 } else { 1 });
    }

    fn from_env() -> Result<Self, InputError> {
        let host = read_env("PG_HOST", "127.0.0.1")?;
        let host = match host.parse::<IpAddr>() {
            Ok(host) => host,
            Err(cause) => return Err(InputError::Host { value: host, cause }),
        };
        let port = read_env("PG_PORT", "5432")?;
        let port = match port.parse::<u16>() {
            Ok(port) => port,
            Err(cause) => return Err(InputError::Port { value: port, cause }),
        };
        let auth = cartel_pg::Config::new(
            read_env("PG_USER", "bench")?,
            read_env("PG_PASSWORD", "bench")?,
            read_env("PG_DATABASE", "bench")?,
        )
        .map_err(InputError::Config)?;
        Ok(Self {
            address: SocketAddr::new(host, port),
            credentials: auth,
        })
    }

    fn run(self, cfg: config::Config) -> Result<Setting, ExitCode> {
        let Self {
            address,
            credentials: auth,
        } = self;
        let snapshot = service::Snapshot::from_one(
            service::Revision::new(1),
            service::Endpoint::new(address, address),
        );
        let assembly = port::Port::<Database>::plan(auth, cfg)
            .assemble::<Bundle<Tcp, Identity, Throughput>>(
                snapshot,
                Duration::from_millis(500),
                Duration::from_secs(512),
            )
            .map_err(|cause| fail(format_args!("PostgreSQL topology: {cause:?}")))?;
        let prepared = match assembly.with_install(settings::Config::for_profile::<Throughput>()) {
            Ok(prepared) => prepared,
            Err(cause) => return Err(fail(format_args!("PostgreSQL resource layout: {cause:?}"))),
        };
        let mut failed = false;
        let mut done = session::Completion::new();
        let (prepared, done) = (prepared, &mut done);
        let result = prepared.with_local(
            |worker| {
                worker.lend(|node, stage| {
                    node.install(stage, |host, stage| {
                        stage.with_app(done, host, |mut app| {
                            let client = app.root_client();
                            Setting::read(client, &mut app)
                        })
                    })
                })
            },
            recovery::Policy::new(
                |report| {
                    failed |= fail(format_args!("pg reclaimed completion: {report:?}"))
                        == ExitCode::from(2);
                },
                Self::stop,
            ),
            |outcome| match outcome {
                Ok(Ok(Ok(Ok(result)))) => result,
                Ok(Ok(Ok(Err(cause)))) => {
                    Err(fail(format_args!("PostgreSQL query drive: {cause:?}")))
                }
                Ok(Ok(Err(error))) => {
                    let code = fail(format_args!("PostgreSQL session: {:?}", error.cause()));
                    if code == ExitCode::from(2) {
                        return Err(code);
                    }
                    match error.output() {
                        Some(Ok(Ok(value))) => Err(fail(format_args!(
                            "retained PostgreSQL setting: {}={}",
                            value.name, value.setting
                        ))),
                        Some(Ok(Err(code))) => Err(*code),
                        Some(Err(_)) | None => Err(ExitCode::FAILURE),
                    }
                }
                Ok(Err(cause)) => Err(fail(format_args!("PostgreSQL installation: {cause:?}"))),
                Err(error) => {
                    let code = fail(format_args!("PostgreSQL runtime: {:?}", error.kind()));
                    if let Some(Ok(Ok(Ok(Ok(value))))) = error.output() {
                        let code = fail(format_args!(
                            "retained PostgreSQL setting: {}={}",
                            value.name, value.setting
                        ));
                        if code == ExitCode::from(2) {
                            return Err(code);
                        }
                    }
                    Err(code)
                }
            },
        );
        if failed {
            Err(ExitCode::from(2))
        } else {
            result
        }
    }
}

enum ReadError<'d> {
    Read(reply::Error<'d>),
    Decode(cartel_pg::Error),
    Missing,
    Duplicate,
}

#[derive(PgTable, PgRow, Debug)]
#[table_name("pg_catalog.pg_settings")]
struct Setting {
    name: String,
    setting: String,
}

#[query_group]
impl Setting {
    fn by_name(name: &str) -> query::Stream<Setting> {
        Setting::filter(Setting::name == name).stream()
    }
}

impl Setting {
    fn read<'d, C: traits::Query<'d, Schema = Database>>(
        query: C,
        app: &mut impl BlockOn<'d>,
    ) -> Result<Result<Self, ExitCode>, drive::Error> {
        let mut rows = Self::by_name(query, "server_version");
        let result = match app.block_on(rows.next_row())? {
            Ok(Some(row)) => match app.block_on(rows.next_row())? {
                Ok(None) => row.into_owned().map_err(ReadError::Decode),
                Ok(Some(_)) => Err(ReadError::Duplicate),
                Err(error) => Err(ReadError::Read(error)),
            },
            Ok(None) => Err(ReadError::Missing),
            Err(error) => Err(ReadError::Read(error)),
        };
        let error = match result {
            Ok(value) => return Ok(Ok(value)),
            Err(error) => error,
        };
        Ok(Err(match &error {
            ReadError::Missing => fail(format_args!("PostgreSQL setting is missing")),
            ReadError::Duplicate => fail(format_args!("PostgreSQL setting is duplicated")),
            ReadError::Decode(cause) => fail(format_args!("decode PostgreSQL setting: {cause:?}")),
            ReadError::Read(cause) => {
                fail(format_args!("read PostgreSQL setting: {:?}", cause.kind()))
            }
        }))
    }
}

pg_instance! { Database: Setting }

const CONNECTION_COUNT: usize = 1;
const REQUEST_FRAMES: usize = 16;
const REQUEST_FRAME_BYTES: usize = 4 * 1024;
const RESPONSE_ITEMS: usize = 65_536;
const RESPONSE_BYTES: usize = 256 * 1024 * 1024;
const RESPONSE_FRAME_BYTES: usize = 16 * 1024 * 1024;
const INFLIGHT: usize = 16;
const WAITERS: usize = 16;
const NOTIFICATIONS: usize = 1024;
const NOTIFICATION_BYTES: usize = 4 * 1024 * 1024;

fn fail(message: fmt::Arguments<'_>) -> ExitCode {
    match writeln!(io::stderr().lock(), "{message}") {
        Ok(()) => ExitCode::FAILURE,
        Err(_) => ExitCode::from(2),
    }
}

fn main() -> ExitCode {
    let input = match Input::from_env() {
        Ok(input) => input,
        Err(error) => {
            return match &error {
                InputError::Environment { variable, cause } => {
                    let cause = match cause {
                        VarError::NotPresent => "not present",
                        VarError::NotUnicode(_) => "not Unicode",
                    };
                    fail(format_args!("read {}: {cause}", variable))
                }
                InputError::Host { value, cause } => {
                    fail(format_args!("parse host {value:?}: {cause:?}"))
                }
                InputError::Port { value, cause } => {
                    fail(format_args!("parse port {value:?}: {cause:?}"))
                }
                InputError::Config(cause) => {
                    fail(format_args!("database configuration: {cause:?}"))
                }
            };
        }
    };
    let cfg = match config::Config::new(config::Capacities {
        connections: CONNECTION_COUNT,
        inflight: INFLIGHT,
        admission_waiters: WAITERS,
        requests: config::Request {
            frames: REQUEST_FRAMES,
            frame_bytes: REQUEST_FRAME_BYTES,
        },
        responses: config::Response {
            items: RESPONSE_ITEMS,
            bytes: RESPONSE_BYTES,
            frame_bytes: RESPONSE_FRAME_BYTES,
        },
        notifications: config::Notification {
            items: NOTIFICATIONS,
            bytes: NOTIFICATION_BYTES,
        },
    }) {
        Ok(config) => config,
        Err(error) => {
            return fail(format_args!(
                "build PostgreSQL port configuration: {error:?}"
            ));
        }
    };
    let version = match input.run(cfg) {
        Ok(version) => version,
        Err(failed) => return failed,
    };

    if let Err(error) = writeln!(io::stdout().lock(), "{}={}", version.name, version.setting) {
        return fail(format_args!("write PostgreSQL setting: {error}"));
    }
    ExitCode::SUCCESS
}
