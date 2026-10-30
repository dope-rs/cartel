mod save;

pub use save::Savepoint;

use crate::{api, catalogs, sys};
use api::{exec, stmt};
use dope_fiber::abi;
use o3::mem::{self, lease};
use std::{cell, marker, result};

/// A transaction capability whose queries cannot outlive it.
/// The transaction cannot expose its underlying engine capability:
///
pub struct Transaction<
    'db,
    'connection: 'db,
    'd: 'connection,
    I: catalogs::Catalog = catalogs::Empty,
> {
    scope: mem::Lease<Live<'db, 'connection, 'd, I>, lease::Open>,
}

struct Live<'db, 'connection: 'db, 'd: 'connection, I: catalogs::Catalog> {
    failed: cell::Cell<Option<sys::Fault>>,
    state: marker::PhantomData<&'db mut api::State<'connection, 'd, I>>,
}

impl<'db, 'connection: 'db, 'd: 'connection, I: catalogs::Catalog>
    Transaction<'db, 'connection, 'd, I>
{
    pub(crate) const fn new(state: &'db mut api::State<'connection, 'd, I>) -> Self {
        Self {
            scope: mem::Lease::open(
                Live {
                    failed: cell::Cell::new(None),
                    state: marker::PhantomData,
                },
                state,
            ),
        }
    }

    pub fn query(&mut self) -> exec::Query<'_, 'connection, 'd, I> {
        let (live, state) = self.scope.parts_mut();
        state.query_in(exec::Tx::new(&live.failed))
    }

    pub fn savepoint<'transaction>(
        &'transaction mut self,
    ) -> impl abi::Fiber<
        'd,
        Output = result::Result<Savepoint<'transaction, 'connection, 'd, I>, crate::Error>,
    > + use<'transaction, 'db, 'connection, 'd, I> {
        let (live, state) = self.scope.parts_mut();
        Savepoint::open(state, &live.failed)
    }

    pub fn commit(
        mut self,
    ) -> impl abi::Fiber<'d, Output = result::Result<(), crate::Error>> + use<'db, 'connection, 'd, I>
    {
        dope_fiber::fiber!('d => async move {
            let result = {
                let (live, state) = self.scope.parts_mut();
                let txn = exec::Tx::new(&live.failed);
                txn.check()?;
                state.control(stmt::Control::Commit, txn).await
            };
            if result.is_ok() || matches!(self.scope.item().tx(), sys::Tx::Idle) {
                self.scope.item().work().depth.reset();
                let _ = self.scope.into_parts();
                return result;
            }
            result
        })
    }

    pub fn rollback(
        mut self,
    ) -> impl abi::Fiber<'d, Output = result::Result<(), crate::Error>> + use<'db, 'connection, 'd, I>
    {
        dope_fiber::fiber!('d => async move {
            let result = {
                let (live, state) = self.scope.parts_mut();
                exec::Tx::new(&live.failed).rollback()?;
                state.control(stmt::Control::Rollback, exec::Tx::NONE).await
            };
            if result.is_ok() || matches!(self.scope.item().tx(), sys::Tx::Idle) {
                self.scope.item().work().depth.reset();
                let _ = self.scope.into_parts();
                return result;
            }
            result
        })
    }
}

impl<'db, 'connection: 'db, 'd: 'connection, I: catalogs::Catalog> mem::Return
    for Live<'db, 'connection, 'd, I>
{
    type Item = &'db mut api::State<'connection, 'd, I>;

    fn return_item(self, state: Self::Item) {
        let work = state.work();
        if self.failed.get().is_some_and(sys::Fault::is_aborted) {
            work.depth.reset();
        } else {
            work.pending.transaction();
        }
    }
}
