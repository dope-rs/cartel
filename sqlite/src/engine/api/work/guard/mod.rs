use crate::{api, sys};
use api::work;
use std::{marker, mem, result};
use work::{call::Call, meter};

pub(crate) mod call;
pub(crate) mod open;

pub(super) fn limit<T>(result: result::Result<T, crate::Error>) -> result::Result<T, crate::Error> {
    match result {
        Err(crate::Error::Sqlite { action, code })
            if code.primary() == libsqlite3_sys::SQLITE_INTERRUPT =>
        {
            Err(crate::Error::WorkLimit { action, code })
        }
        Err(error) => Err(error),
        Ok(_) => Err(crate::Error::WorkExhausted),
    }
}

#[repr(transparent)]
pub(crate) struct Guard<'work, R> {
    work: &'work work::Work,
    role: marker::PhantomData<R>,
}

impl<'work, R> Guard<'work, R> {
    pub(super) const fn new(work: &'work work::Work) -> Self {
        Self {
            work,
            role: marker::PhantomData,
        }
    }

    pub(crate) const fn work(&self) -> &'work work::Work {
        self.work
    }

    pub(crate) fn check<T>(
        &self,
        result: result::Result<T, crate::Error>,
    ) -> result::Result<T, crate::Error> {
        match self.work.meter.call_quota() {
            meter::Quota::Available => result,
            meter::Quota::Exhausted => limit(result),
        }
    }
}

impl<'work> Guard<'work, open::Open> {
    pub(crate) const fn call<'connection, 'guard>(
        &'guard self,
        db: &'connection sys::Connection,
    ) -> Call<'connection, 'guard, 'work> {
        Call::new(db, self)
    }
}

impl<R> Drop for Guard<'_, R> {
    fn drop(&mut self) {
        self.work.meter.finish();
    }
}

const _: () = {
    let word = mem::size_of::<&work::Work>();
    assert!(mem::size_of::<Guard<'static, call::Call>>() == word);
    assert!(mem::size_of::<Guard<'static, open::Open>>() == word);
};
