use crate::{api, open::journal, sys};
use api::work::guard::{Guard, open::Open};
use std::mem;

pub(crate) struct Call<'connection, 'guard, 'work> {
    inner: &'connection sys::Connection,
    guard: &'guard Guard<'work, Open>,
}

impl<'connection, 'guard, 'work> Call<'connection, 'guard, 'work> {
    pub(super) const fn new(
        inner: &'connection sys::Connection,
        guard: &'guard Guard<'work, Open>,
    ) -> Self {
        Self { inner, guard }
    }

    pub(crate) const fn inner(&self) -> &'connection sys::Connection {
        self.inner
    }

    pub(crate) fn execute(&self, sql: sys::Sql<'_>) -> Result<(), crate::Error> {
        self.complete(self.inner.execute(sql))
    }

    pub(crate) fn exec(&self, sql: &str) -> Result<(), crate::Error> {
        self.complete(self.inner.exec(sql))
    }

    pub(crate) fn pragma(&self, name: &str, value: i64) -> Result<(), crate::Error> {
        self.complete(self.inner.pragma(name, value))
    }

    pub(crate) fn query_i64(&self, sql: &str) -> Result<i64, crate::Error> {
        self.complete(self.inner.query_i64(sql))
    }

    pub(crate) fn journal(&self, sql: &str) -> Result<journal::Mode, crate::Error> {
        self.complete(self.inner.journal(sql))
    }

    pub(crate) fn check<T>(&self, result: Result<T, crate::Error>) -> Result<T, crate::Error> {
        self.guard.check(result)
    }

    fn complete<T>(&self, done: sys::Completion<T>) -> Result<T, crate::Error> {
        match (done.result, done.cleanup) {
            (Ok(_), Some(fault)) => self.check(Err(fault.into_error())),
            (Err(error), Some(fault)) => {
                self.guard.work().finalized(fault);
                self.check(Err(error))
            }
            (result, None) => self.check(result),
        }
    }
}

const _: () =
    assert!(mem::size_of::<Call<'static, 'static, 'static>>() == 2 * mem::size_of::<usize>());
