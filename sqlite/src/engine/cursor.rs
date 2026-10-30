use crate::{api, catalogs, row};
use api::exec;
use dope_fiber::abi;
use std::{marker, mem, ops, result};

/// A bounded row scan tied to the connection or transaction that issued it.
/// Borrowed rows are valid only for one visitor invocation and cannot be
/// retained by a later invocation.
pub struct Cursor<
    'query,
    'connection: 'query,
    'd: 'connection,
    R,
    Q: catalogs::TypedQuery,
    P = (),
    I: catalogs::Catalog = catalogs::Empty,
> where
    I: catalogs::HasQuery<Q>,
{
    machine: exec::Machine<'query, 'connection, 'd, I, P, catalogs::Statement<I, Q>>,
    marker: marker::PhantomData<fn() -> R>,
}

impl<'query, 'connection: 'query, 'd: 'connection, R, Q, P, I>
    Cursor<'query, 'connection, 'd, R, Q, P, I>
where
    Q: catalogs::TypedQuery,
    I: catalogs::Catalog + catalogs::HasQuery<Q>,
{
    const LAYOUT: () = assert!(
        mem::size_of::<Self>()
            == mem::size_of::<
                exec::Machine<'query, 'connection, 'd, I, P, catalogs::Statement<I, Q>>,
            >()
    );

    pub(super) const fn new(
        machine: exec::Machine<'query, 'connection, 'd, I, P, catalogs::Statement<I, Q>>,
    ) -> Self {
        let () = Self::LAYOUT;
        Self {
            machine,
            marker: marker::PhantomData,
        }
    }
}

impl<
    'query,
    'connection: 'query,
    'd: 'connection,
    R: row::raw::Decode,
    Q: catalogs::TypedQuery,
    P: api::Bind,
    I: catalogs::Catalog + catalogs::HasQuery<Q>,
> Cursor<'query, 'connection, 'd, R, Q, P, I>
{
    pub fn visit<V>(
        self,
        visitor: V,
    ) -> impl abi::Fiber<'d, Output = result::Result<(), crate::Error>>
    + use<'query, 'connection, 'd, R, Q, P, I, V>
    where
        V: for<'row> FnMut(R::Borrowed<'row>) -> ops::ControlFlow<()>,
    {
        exec::Rows::<I, Q, R, P, _>::new(self.machine, exec::Visit::new(visitor))
    }
}
