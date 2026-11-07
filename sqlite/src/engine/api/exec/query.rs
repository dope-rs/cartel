use crate::Cursor;
use crate::{api, catalogs, row};
use api::{batches, exec};
use dope_fiber::abi;
use std::result;

pub struct Query<
    'query,
    'connection: 'query,
    'd: 'connection,
    I: catalogs::Catalog = catalogs::Empty,
> {
    start: exec::Start<'query, 'connection, 'd, I>,
    arena: &'query mut batches::Arena<'connection>,
}

impl<'query, 'connection: 'query, 'd: 'connection, I: catalogs::Catalog>
    Query<'query, 'connection, 'd, I>
{
    pub(crate) const fn new(
        start: exec::Start<'query, 'connection, 'd, I>,
        arena: &'query mut batches::Arena<'connection>,
    ) -> Self {
        Self { start, arena }
    }

    #[doc(hidden)]
    pub fn run_no_rows<Q: catalogs::TypedQuery, P: api::Bind>(
        self,
        stmt: catalogs::Statement<I, Q>,
        params: P,
    ) -> impl abi::Fiber<'d, Output = result::Result<(), crate::Error>>
    + use<'query, 'connection, 'd, I, Q, P>
    where
        I: catalogs::HasQuery<Q>,
    {
        exec::Command::new(self.start.run(stmt, params))
    }

    #[doc(hidden)]
    pub fn run_one<R: row::raw::Decode + 'query, P: api::Bind, Q: catalogs::TypedQuery>(
        self,
        stmt: catalogs::Statement<I, Q>,
        params: P,
    ) -> impl abi::Fiber<'d, Output = result::Result<R::Borrowed<'query>, crate::Error>>
    + use<'query, 'connection, 'd, I, R, P, Q>
    where
        I: catalogs::HasQuery<Q>,
    {
        let machine = self.start.run(stmt, params);
        let create = exec::Single::<R, _>::new(self.arena, |value: Option<_>| {
            value.ok_or(crate::Error::QueryReturnedNoRows)
        });
        exec::Rows::new(machine, create)
    }

    #[doc(hidden)]
    pub fn run_first<R: row::raw::Decode + 'query, P: api::Bind, Q: catalogs::TypedQuery>(
        self,
        stmt: catalogs::Statement<I, Q>,
        params: P,
    ) -> impl abi::Fiber<'d, Output = result::Result<Option<R::Borrowed<'query>>, crate::Error>>
    + use<'query, 'connection, 'd, I, R, P, Q>
    where
        I: catalogs::HasQuery<Q>,
    {
        let machine = self.start.run(stmt, params);
        let create = exec::Single::<R, _>::new(self.arena, Ok);
        exec::Rows::new(machine, create)
    }

    #[doc(hidden)]
    pub fn run_all<R: row::raw::Decode + 'query, P: api::Bind, Q: catalogs::TypedQuery>(
        self,
        stmt: catalogs::Statement<I, Q>,
        params: P,
    ) -> impl abi::Fiber<'d, Output = result::Result<batches::Rows<'query, R>, crate::Error>>
    + use<'query, 'connection, 'd, I, R, P, Q>
    where
        I: catalogs::HasQuery<Q>,
    {
        let machine = self.start.run(stmt, params);
        exec::Rows::new(machine, exec::Collect::<R>::new(self.arena))
    }

    #[doc(hidden)]
    pub fn stream<R: row::raw::Decode, P: api::Bind, Q: catalogs::TypedQuery>(
        self,
        stmt: catalogs::Statement<I, Q>,
        params: P,
    ) -> Cursor<'query, 'connection, 'd, R, Q, P, I>
    where
        I: catalogs::HasQuery<Q>,
    {
        Cursor::new(self.start.run(stmt, params))
    }
}
