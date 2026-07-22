use std::net::SocketAddr;
use std::pin::pin;
use std::time::Duration;

use cartel_gen::pg_instance;
use cartel_pg::{PgOps, port};
use dope::driver::token::Token;
use dope::manifold::Manifold;
use dope::manifold::connector::Connector;
use dope::manifold::connector::source::Static;
use dope::manifold::env::Bundle;
use dope::runtime::profile::Throughput;
use dope::{Completion as _, driver};
use dope_net::tcp::Tcp;
use dope_net::wire::identity::Identity;
use o3::cell::BrandCell as Branded;

const ROUTE: u8 = 0;

pg_instance! { Probe: }

type PgConn<'d> =
    Connector<'d, 0, cartel_pg::Session<'d, Probe>, Static<Tcp>, Bundle<Tcp, Identity, Throughput>>;

fn pg_addr() -> SocketAddr {
    let host = std::env::var("PG_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port: u16 = std::env::var("PG_PORT")
        .unwrap_or_else(|_| "5432".into())
        .parse()
        .unwrap();
    format!("{}:{}", host, port).parse().unwrap()
}

fn pg_cfg() -> cartel_pg::Config {
    cartel_pg::Config::new(
        std::env::var("PG_USER").unwrap_or_else(|_| "bench".into()),
        std::env::var("PG_PASSWORD").unwrap_or_else(|_| "bench".into()),
        std::env::var("PG_DATABASE").unwrap_or_else(|_| "bench".into()),
    )
}

#[test]
fn probe_step_by_step() {
    if std::env::var_os("CARTEL_PG_TEST").is_none() {
        return;
    }
    let addr = pg_addr();
    let cfg = pg_cfg();
    let port_config = port::Config::new(port::Capacities {
        connections: 1,
        request_entries: 16,
        request_bytes: 4 * 1024,
        response_entries: 65_536,
        response_bytes: 256 * 1024 * 1024,
        inflight: 16,
        waiters: 16,
        notifications: 1024,
    })
    .expect("port config");

    let exec = dope::runtime::Executor::new(driver::Config::for_tcp_profile::<Throughput>(8))
        .expect("driver")
        .with_storage_factory(cartel_pg::Port::<Probe>::factory(cfg, port_config));
    exec.enter(|mut sess| {
        let backoff = sess.seed().derive(dope::hash::domain::BACKOFF).state();
        let port = sess.storage();
        let (token, mut driver) = sess.token_and_driver();
        let upstreams = Static::<Tcp>::new(vec![addr], Duration::from_millis(500), backoff);
        let connector: PgConn<'_> = port
            .connect::<0, _, Bundle<Tcp, Identity, Throughput>>(upstreams, &mut driver)
            .expect("connector");
        let pg = pin!(Branded::new(connector));
        let pg = pg.as_ref();
        let client = port.client();

        pg.borrow_pin_mut(token).pre_park(&mut driver);

        let mut buf = [const { None }; 64];
        let mut wake_buf: Vec<Token> = Vec::with_capacity(64);
        for _ in 0..200 {
            let n = driver.drain(&mut buf);
            if n > 0 {
                for event in &mut buf[..n] {
                    let Some(ev) = event.take() else {
                        continue;
                    };
                    if ev.route() == ROUTE {
                        Manifold::dispatch(pg.borrow_pin_mut(token), ev, &mut driver);
                    }
                }
            }
            wake_buf.clear();
            driver
                .driver_ref()
                .drain_ready(|target| wake_buf.push(target));
            for t in &wake_buf {
                if t.route() == ROUTE {
                    let __typed = dope::manifold::TypedToken::<PgConn>::try_new(*t)
                        .expect("ready target route was checked");
                    Manifold::activate(pg.borrow_pin_mut(token), __typed, &mut driver);
                }
            }
            pg.borrow_pin_mut(token).pre_park(&mut driver);
            let live = client.live_count();
            if live >= 1 {
                return;
            }
            let _ = driver.wait(Some(Duration::from_millis(100)));
        }
        panic!("never reached live=1 after 200 iters (20s)");
    });
}
