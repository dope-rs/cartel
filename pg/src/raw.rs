use std::future::Future;

use dope::manifold::connector::source::Dialer;
use dope::manifold::env::Env;
use dope::transport::Transport;

use crate::{Dispatched, Error, ExtractUnit, Fiber, PgHolding, PgOps, QuerySet};

pub trait PgRawExt<'d, I, S, E>: PgOps<'d, I, S, E>
where
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    fn execute_raw(
        &self,
        sql: &str,
    ) -> Fiber<'d, impl Future<Output = Result<(), Error>> + use<'d, Self, I, S, E>> {
        let dispatched: Dispatched<'d, I, S, E, ExtractUnit> = self.dispatch_sql(sql);
        Fiber::new(dispatched)
    }

    fn migrate(
        &self,
        stmts: &'static [&'static str],
    ) -> Fiber<'d, impl Future<Output = Result<(), Error>> + use<'d, Self, I, S, E>> {
        let holding: PgHolding<'d, I, S, E> = self.holding();
        Fiber::new(async move {
            for sql in stmts {
                holding.dispatch_sql(sql).await?;
            }
            Ok(())
        })
    }
}

impl<'d, I, S, E, T> PgRawExt<'d, I, S, E> for T
where
    T: PgOps<'d, I, S, E>,
    I: QuerySet + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
}
