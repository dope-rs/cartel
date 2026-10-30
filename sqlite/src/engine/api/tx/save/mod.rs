use crate::{api, catalogs, config, sys};
use api::{exec, stmt, work};
use dope_fiber::abi;
use o3::mem::{self, lease};
use release::Release;
use std::{cell, marker, result};
use work::guard::{self, call};

/// A savepoint capability whose queries cannot outlive the savepoint.
pub struct Savepoint<
    'transaction,
    'connection: 'transaction,
    'd: 'connection,
    I: catalogs::Catalog = catalogs::Empty,
> {
    scope: mem::Lease<Live<'transaction, 'connection, 'd, I>, lease::Open>,
}

/// Owns one successful savepoint's return to its original work state.
struct Live<'scope, 'connection: 'scope, 'd: 'connection, I: catalogs::Catalog> {
    source: &'scope work::Work,
    depth: work::Count,
    failed: &'scope cell::Cell<Option<sys::Fault>>,
    state: marker::PhantomData<&'scope mut api::State<'connection, 'd, I>>,
}

impl<'transaction, 'connection: 'transaction, 'd: 'connection, I: catalogs::Catalog>
    Savepoint<'transaction, 'connection, 'd, I>
{
    pub(super) fn open(
        state: &'transaction mut api::State<'connection, 'd, I>,
        failed: &'transaction cell::Cell<Option<sys::Fault>>,
    ) -> impl abi::Fiber<'d, Output = result::Result<Self, crate::Error>>
    + use<'transaction, 'connection, 'd, I> {
        dope_fiber::fiber!('d => async move {
            let txn = exec::Tx::new(failed);
            txn.check()?;
            let depth = state.control(Open, txn).await?;
            let live = Live {
                source: state.work(),
                depth,
                failed,
                state: marker::PhantomData,
            };
            Ok(Self {
                scope: mem::Lease::open(live, state),
            })
        })
    }

    pub fn query(&mut self) -> exec::Query<'_, 'connection, 'd, I> {
        let (live, state) = self.scope.parts_mut();
        state.query_in(exec::Tx::new(live.failed))
    }

    pub fn savepoint<'nested>(
        &'nested mut self,
    ) -> impl abi::Fiber<
        'd,
        Output = result::Result<Savepoint<'nested, 'connection, 'd, I>, crate::Error>,
    > + use<'nested, 'transaction, 'connection, 'd, I> {
        let (live, state) = self.scope.parts_mut();
        Savepoint::open(state, live.failed)
    }

    pub fn commit(
        mut self,
    ) -> impl abi::Fiber<'d, Output = result::Result<(), crate::Error>>
    + use<'transaction, 'connection, 'd, I> {
        dope_fiber::fiber!('d => async move {
            loop {
                let (live, state) = self.scope.parts_mut();
                let txn = exec::Tx::new(live.failed);
                if state.control(Release::new(live, false), txn).await? {
                    break;
                }
            }
            let _ = self.scope.into_parts();
            Ok(())
        })
    }

    pub fn rollback<'rollback>(
        &'rollback mut self,
    ) -> impl abi::Fiber<'d, Output = result::Result<(), crate::Error>>
    + use<'rollback, 'transaction, 'connection, 'd, I> {
        dope_fiber::fiber!('d => async move {
            loop {
                let (live, state) = self.scope.parts_mut();
                let txn = exec::Tx::new(live.failed);
                if state.control(Release::new(live, true), txn).await? {
                    break;
                }
            }
            Ok(())
        })
    }
}

impl<'scope, 'connection: 'scope, 'd: 'connection, I: catalogs::Catalog> mem::Return
    for Live<'scope, 'connection, 'd, I>
{
    type Item = &'scope mut api::State<'connection, 'd, I>;

    fn return_item(self, _: Self::Item) {
        if self.failed.get().is_none() {
            self.source.revert(self.depth);
        }
    }
}

const _: () = {
    use std::mem;

    assert!(mem::size_of::<Savepoint<'_, '_, '_>>() == 4 * mem::size_of::<usize>());
};

struct Open;

impl<I: catalogs::Catalog> stmt::target::Target<I> for Open {
    type Access = work::Permit;

    fn bind<'query, 'connection, P: api::Bind>(
        self,
        engine: &'query mut stmt::Engine<'connection, I>,
        params: P,
        config: &config::Config,
        guard: &guard::Guard<'_, call::Call>,
    ) -> result::Result<(stmt::Bound<'query, 'connection, P>, Self::Access), crate::Error> {
        let permit = guard.work().depth.check(config.work.savepoints)?;
        Ok((
            engine
                .controls()
                .bind(stmt::Control::Savepoint, params, config)?,
            permit,
        ))
    }
}

mod release;
