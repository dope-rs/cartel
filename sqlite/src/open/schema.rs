use crate::api::{self, exec, stmt, work::guard};
use crate::{config, open, row, sys};
use o3::alloc::BoxSliceExt as _;
use std::{marker, mem};

/// Exclusive initialization transaction, returned on each successful operation.
pub struct Schema<'transaction> {
    db: &'transaction sys::Connection,
    guard: &'transaction guard::Guard<'transaction, guard::open::Open>,
    config: &'transaction config::Config,
    brand: marker::PhantomData<fn(&'transaction ()) -> &'transaction ()>,
}

impl<'t> Schema<'t> {
    pub(crate) fn new(
        db: &'t sys::Connection,
        guard: &'t guard::Guard<'t, guard::open::Open>,
        config: &'t config::Config,
    ) -> Self {
        Self {
            db,
            guard,
            config,
            brand: marker::PhantomData,
        }
    }

    /// Executes one columnless statement with borrowed parameters.
    pub fn execute<P: api::Bind>(self, sql: &str, params: P) -> Result<Self, crate::Error> {
        let mut stmt = self.statement(sql, P::COUNT, 0)?;
        let mut bound = stmt::Bound::new(&mut stmt, params, self.config, P::COUNT)?;
        match self
            .guard
            .charge(bound.prepared().step(&stmt::access::Plain, exec::Tx::NONE))?
        {
            sys::Step::Done => {}
            sys::Step::Row => {
                return Err(crate::Error::UnexpectedRows {
                    expected: 0,
                    minimum: 1,
                });
            }
        }
        stmt.finish()?;
        Ok(self)
    }

    /// Reads the first integer of a single-column query.
    pub fn integer(self, sql: &str) -> Result<(Self, i64), crate::Error> {
        let value = self
            .first::<i64, _>(sql, Ok)?
            .ok_or(crate::Error::QueryReturnedNoRows)?;
        Ok((self, value))
    }

    /// Returns the first single-column BLOB as owned bytes, or None for no row.
    pub fn first_blob(self, sql: &str) -> Result<(Self, Option<Vec<u8>>), crate::Error> {
        let value = self.first::<Vec<u8>, _>(sql, |bytes| {
            Ok(Box::<[u8]>::try_copy(bytes)
                .map_err(|error| crate::Error::Allocation(error.into()))?
                .into_vec())
        })?;
        Ok((self, value))
    }

    fn first<R: row::raw::Decode, T>(
        &self,
        sql: &str,
        own: impl for<'r> FnOnce(R::Borrowed<'r>) -> Result<T, crate::Error>,
    ) -> Result<Option<T>, crate::Error> {
        let mut stmt = self.statement(sql, 0, R::N_COLS)?;
        let value = match self
            .guard
            .charge(stmt.step(&stmt::access::Plain, exec::Tx::NONE))?
        {
            sys::Step::Done => None,
            sys::Step::Row => {
                let mut row = row::raw::Reader::new(
                    &mut stmt,
                    self.config.row_bytes().cast_unsigned(),
                    mem::size_of::<R::Borrowed<'_>>(),
                )?;
                Some(own(R::borrow(&mut row)?)?)
            }
        };
        stmt.finish()?;
        Ok(value)
    }

    fn statement(
        &self,
        sql: &str,
        parameters: usize,
        columns: usize,
    ) -> Result<stmt::Prepared<'t>, crate::Error> {
        let available = self.config.params();
        if parameters > available {
            return Err(crate::Error::ParameterItems {
                required: parameters,
                available,
            });
        }
        self.guard.charge(stmt::Prepared::new::<0>(
            self.db,
            self.guard.work(),
            sql,
            parameters,
            columns,
        ))
    }
}

pub(crate) trait Setup {
    type Error: From<open::Error>;
    type Transaction: Copy + Into<Transaction>;
    const TRANSACTION: Option<Self::Transaction>;
    fn apply(self, transaction: Self::Transaction, schema: Schema<'_>) -> Result<(), Self::Error>;
}

#[derive(Clone, Copy)]
pub(crate) struct Transaction(());

impl From<std::convert::Infallible> for Transaction {
    fn from(never: std::convert::Infallible) -> Self {
        match never {}
    }
}

pub(crate) struct Autocommit;

impl Setup for Autocommit {
    type Error = open::Error;
    type Transaction = std::convert::Infallible;
    const TRANSACTION: Option<Self::Transaction> = None;
    fn apply(self, transaction: Self::Transaction, _: Schema<'_>) -> Result<(), Self::Error> {
        match transaction {}
    }
}

pub(crate) struct Immediate<F>(pub(crate) F);

impl<E: From<open::Error>, F: for<'t> FnOnce(Schema<'t>) -> Result<Schema<'t>, E>> Setup
    for Immediate<F>
{
    type Error = E;
    type Transaction = Transaction;
    const TRANSACTION: Option<Transaction> = Some(Transaction(()));
    fn apply(self, _: Transaction, schema: Schema<'_>) -> Result<(), E> {
        (self.0)(schema).map(|_| ())
    }
}
