use crate::api::stmt::{Binder, Prepared, access::Access};
use crate::{api, config, row};
use api::exec;
use std::{marker, mem, num, result};

pub(crate) struct Bound<'statement, 'connection, P> {
    statement: &'statement mut Prepared<'connection>,
    // Bind admits only scalar values and external storage borrows.
    params: marker::PhantomData<P>,
}

impl<'statement, 'connection, P: api::Bind> Bound<'statement, 'connection, P> {
    /// Reuses the admitted catalog count, retaining native discovery for its sentinel.
    pub(crate) fn new(
        stmt: &'statement mut Prepared<'connection>,
        params: P,
        config: &config::Config,
        declared: usize,
    ) -> result::Result<Self, crate::Error> {
        let expected = if declared == usize::MAX {
            stmt.params()
        } else {
            declared
        };
        if P::COUNT != expected {
            let limit = config.params();
            if P::COUNT > limit {
                return Err(crate::Error::ParameterItems {
                    required: P::COUNT,
                    available: limit,
                });
            }
            return Err(crate::Error::ParameterCount {
                expected,
                actual: P::COUNT,
            });
        }
        let result = {
            let mut binder = Binder::new(stmt, config.param_bytes());
            params.bind(&mut binder, 1)
        };
        if let Err(error) = result {
            stmt.clear();
            return Err(error);
        }
        Ok(Self {
            statement: stmt,
            params: marker::PhantomData,
        })
    }
}

impl<'connection, P> Bound<'_, 'connection, P> {
    pub(crate) fn prepared<'a>(&'a mut self) -> &'a mut Prepared<'connection> {
        &mut *self.statement
    }

    pub(crate) fn columns<const CURRENT: bool>(
        &self,
        expected: usize,
        declared: usize,
    ) -> result::Result<(), crate::Error> {
        let actual = if !CURRENT && declared != usize::MAX {
            declared
        } else {
            self.statement.columns::<CURRENT>()
        };
        if actual != expected {
            return Err(crate::Error::ColumnCount { expected, actual });
        }
        Ok(())
    }

    pub(crate) fn read<'row, R: row::raw::Decode>(
        &'row mut self,
        bytes: num::NonZero<u32>,
    ) -> result::Result<R::Borrowed<'row>, crate::Error> {
        let layout = mem::size_of::<R::Borrowed<'row>>();
        let mut reader = row::raw::Reader::new(self.statement, bytes, layout)?;
        R::borrow(&mut reader)
    }

    pub(crate) fn finish<A: Access>(
        &mut self,
        access: &A,
        txn: exec::Tx<'_>,
    ) -> result::Result<(), crate::Error> {
        self.statement.reset(access, txn)
    }
}
