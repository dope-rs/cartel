use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;

use cartel_gen::{pg_instance, query_group};
use cartel_pg::{Client, PgTable, Port, Stream, port};
use dope::driver;
use dope::manifold::connector::source::Static;
use dope::manifold::env::Bundle;
use dope::runtime::Executor;
use dope::runtime::profile::Throughput;
use dope_net::tcp::Tcp;
use dope_net::wire::identity::Identity;

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 5432;
const DEFAULT_USER: &str = "bench";
const DEFAULT_PASSWORD: &str = "bench";
const DEFAULT_DATABASE: &str = "bench";
const CONNECTION_COUNT: usize = 1;
const REQUEST_ENTRIES: usize = 16;
const REQUEST_BYTES: usize = 4 * 1024;
const RESPONSE_ENTRIES: usize = 65_536;
const RESPONSE_BYTES: usize = 256 * 1024 * 1024;
const INFLIGHT: usize = 16;
const WAITERS: usize = 16;
const NOTIFICATIONS: usize = 1024;
const DRIVER_CAPACITY: usize = 8;
const RECONNECT_DELAY: Duration = Duration::from_millis(500);
const SERVER_VERSION_SETTING: &str = "server_version";

type ExampleError = Box<dyn std::error::Error>;
type PgEnvironment = Bundle<Tcp, Identity, Throughput>;

#[derive(PgTable)]
#[table_name("pg_catalog.pg_settings")]
struct Setting {
    #[pk]
    name: String,
    setting: String,
}

#[query_group]
impl Setting {
    fn by_name(name: String) -> Stream<Setting> {
        Setting::filter(|setting| setting.name == name).stream()
    }
}

pg_instance! { ExampleDatabase: Setting }

struct ExampleConfig {
    address: SocketAddr,
    credentials: cartel_pg::Config,
}

impl ExampleConfig {
    fn from_environment() -> Result<Self, ExampleError> {
        let host = environment_value("PG_HOST", DEFAULT_HOST);
        let port = environment_value("PG_PORT", &DEFAULT_PORT.to_string()).parse::<u16>()?;
        let address = (host.as_str(), port)
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::AddrNotAvailable, "PG_HOST resolved empty")
            })?;
        let credentials = cartel_pg::Config::new(
            environment_value("PG_USER", DEFAULT_USER),
            environment_value("PG_PASSWORD", DEFAULT_PASSWORD),
            environment_value("PG_DATABASE", DEFAULT_DATABASE),
        );
        Ok(Self {
            address,
            credentials,
        })
    }
}

fn environment_value(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn main() -> Result<(), ExampleError> {
    let ExampleConfig {
        address,
        credentials,
    } = ExampleConfig::from_environment()?;
    let port_config = port::Config::new(port::Capacities {
        connections: CONNECTION_COUNT,
        request_entries: REQUEST_ENTRIES,
        request_bytes: REQUEST_BYTES,
        response_entries: RESPONSE_ENTRIES,
        response_bytes: RESPONSE_BYTES,
        inflight: INFLIGHT,
        waiters: WAITERS,
        notifications: NOTIFICATIONS,
    })?;
    let executor = Executor::new(driver::Config::for_tcp_profile::<Throughput>(
        DRIVER_CAPACITY,
    ))?
    .with_storage_factory(Port::<ExampleDatabase>::factory(credentials, port_config));

    let server_version = executor
        .enter(|mut session| {
            let backoff = session.seed().derive(dope::hash::domain::BACKOFF).state();
            let port = session.storage();
            let client: Client<'_, ExampleDatabase> = port.client();
            let upstreams = Static::<Tcp>::new(vec![address], RECONNECT_DELAY, backoff);
            let connector = {
                let mut driver = session.driver_access();
                port.connect::<0, _, PgEnvironment>(upstreams, &mut driver)?
            };
            cartel_pg::AppRuntime::enter(&mut session, connector, |mut runtime| {
                let mut matching_settings =
                    Setting::by_name(&client, SERVER_VERSION_SETTING.to_owned());
                let server_version = runtime
                    .block_on(matching_settings.next_row())??
                    .ok_or(cartel_pg::Error::NotFound)?;
                let duplicate = runtime.block_on(matching_settings.next_row())??;
                if duplicate.is_some() {
                    return Err(cartel_pg::Error::Other(
                        "server_version setting is not unique".to_owned(),
                    ));
                }
                Ok(server_version)
            })
        })
        .map_err(|error| io::Error::other(error.to_string()))?;

    println!("{}={}", server_version.name, server_version.setting);
    Ok(())
}
