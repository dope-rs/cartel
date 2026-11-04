pub(crate) mod batches;
mod bind;
pub(crate) mod exec;
mod state;
#[doc(hidden)]
pub(crate) mod stmt;
pub(crate) mod tx;
pub(crate) mod work;

use crate::catalogs;
pub(crate) use bind::Bind;
pub use bind::{Bound, Limit, Offset};
pub(crate) use state::{Open, Plan, State};

#[doc(hidden)]
pub trait IntoQuery<'query, 'connection: 'query, 'd: 'connection, I: catalogs::Catalog> {
    fn into_query(self) -> exec::Query<'query, 'connection, 'd, I>;
}

impl<'query, 'connection: 'query, 'd: 'connection, I: catalogs::Catalog>
    IntoQuery<'query, 'connection, 'd, I> for exec::Query<'query, 'connection, 'd, I>
{
    fn into_query(self) -> exec::Query<'query, 'connection, 'd, I> {
        self
    }
}

impl<'query, 'database: 'query, 'connection: 'database, 'd: 'connection, I: catalogs::Catalog>
    IntoQuery<'query, 'connection, 'd, I>
    for &'query mut tx::Transaction<'database, 'connection, 'd, I>
{
    fn into_query(self) -> exec::Query<'query, 'connection, 'd, I> {
        self.query()
    }
}

impl<'query, 'transaction: 'query, 'connection: 'transaction, 'd: 'connection, I: catalogs::Catalog>
    IntoQuery<'query, 'connection, 'd, I>
    for &'query mut tx::Savepoint<'transaction, 'connection, 'd, I>
{
    fn into_query(self) -> exec::Query<'query, 'connection, 'd, I> {
        self.query()
    }
}
