use dope_fiber::Fiber;

use crate::{Dispatched, Error, ExtractUnit, PgOps, QuerySet};

pub trait PgRawExt<'d, I>: PgOps<'d, I>
where
    I: QuerySet + 'd,
{
    fn execute_raw(&self, sql: &str) -> Dispatched<'d, I, ExtractUnit> {
        self.dispatch_sql(sql)
    }

    fn migrate(
        &self,
        stmts: &'static [&'static str],
    ) -> impl Fiber<'d, Output = Result<(), Error>> + use<'d, Self, I> {
        let client = self.client();
        dope_fiber::fiber!('d => async move {
            for statement in stmts {
                client.dispatch_sql(statement).await?;
            }
            Ok(())
        })
    }
}

impl<'d, I, T> PgRawExt<'d, I> for T
where
    T: PgOps<'d, I>,
    I: QuerySet + 'd,
{
}
