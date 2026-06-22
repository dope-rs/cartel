use std::net::SocketAddr;
use std::pin::Pin;
use std::time::Duration;

use cartel_gen::pg_instance;
use cartel_pg::PgOps;
use dope::fiber::Holding;
use dope::manifold::Manifold;
use dope::manifold::connector::Connector;
use dope::manifold::connector::source::Static;
use dope::manifold::env::Bundle;
use dope::runtime::park::Parker;
use dope::runtime::profile::Throughput;
use dope::runtime::token::Token;
use dope::transport::Tcp;
use dope::wire::Identity;
use dope::{Cqe, Drive, Driver, DriverCfg, DriverConfig};

const ROUTE: u8 = 0;

pg_instance! { Probe: }

type PgConn =
    Connector<0, cartel_pg::Session<Probe>, Static<Tcp>, Bundle<Tcp, Identity, Throughput>>;

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
    let addr = pg_addr();
    let cfg = pg_cfg();

    let mut driver = Driver::new(DriverCfg::for_tcp_profile::<Throughput>(8)).expect("driver");
    let upstreams = Static::<Tcp>::new(vec![addr], Duration::from_millis(500));
    let pg = PgConn::new(cartel_pg::Session::new(cfg), upstreams, 1, &mut driver);
    let mut pg = Box::pin(pg);
    // SAFETY: single-threaded test; `Connector` stays boxed and unmoved for the whole test.
    let pg_ptr = ::std::ptr::NonNull::from(unsafe { pg.as_mut().get_unchecked_mut() });
    // SAFETY: same `Box`; `Holding` keeps only a `NonNull` into the pinned connector.
    let pg_ref = Holding::of(unsafe { Pin::new_unchecked(&mut *pg_ptr.as_ptr()) });

    eprintln!("[probe] addr={}", addr);
    eprintln!("[probe] connector created");

    eprintln!("[probe] -- first tick --");
    pg.as_mut().pre_park(&mut driver);

    let mut buf = [Cqe::ZERO; 64];
    let mut wake_buf: Vec<Token> = Vec::with_capacity(64);
    for i in 0..200 {
        let pending_before = matches!(pg.as_ref().idle(), dope::Idle::Busy);
        let n = Drive::drain(&mut driver, &mut buf);
        if n > 0 {
            for cqe in &buf[..n] {
                let r = cqe.route();
                let k = cqe.kind();
                eprintln!(
                    "[probe] iter {} CQE route={} kind={} result={}",
                    i, r, k, cqe.result
                );
                if r == ROUTE {
                    let Ok(ev) = dope::Event::try_from(*cqe) else {
                        continue;
                    };
                    Manifold::dispatch(pg.as_mut(), ev, &mut driver);
                }
            }
        }
        wake_buf.clear();
        Parker::drain(&driver, &mut wake_buf);
        for t in &wake_buf {
            if t.route() == ROUTE {
                // SAFETY: gate `t.route() == ROUTE` verified token bits encode <PgConn as Manifold>::ID.
                let __typed =
                    unsafe { dope::manifold::route::TypedToken::<PgConn>::from_raw_token(*t) };
                Manifold::on_wake(pg.as_mut(), __typed, &mut driver);
            }
        }
        pg.as_mut().pre_park(&mut driver);
        let live = pg_ref.live_count();
        let pending = matches!(pg.as_ref().idle(), dope::Idle::Busy);
        eprintln!(
            "[probe] iter {} n={} pending_before={} pending_after={} live={}",
            i, n, pending_before, pending, live
        );
        if live >= 1 {
            eprintln!("[probe] READY at iter {}", i);
            return;
        }
        let _ = driver.park(Duration::from_millis(100));
    }
    panic!("never reached live=1 after 200 iters (20s)");
}
