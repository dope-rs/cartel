use crate::{api, config, row};
use api::exec::{Create, Policy};
use std::{mem, ops, result};

pub(crate) struct Visit<V> {
    visitor: V,
}

impl<V> Visit<V> {
    pub(crate) const fn new(visitor: V) -> Self {
        Self { visitor }
    }
}

impl<R, V> Create<R> for Visit<V>
where
    R: row::raw::Decode,
    V: for<'row> FnMut(R::Borrowed<'row>) -> ops::ControlFlow<()>,
{
    type Policy = Self;

    fn create(self, _config: &config::Config) -> result::Result<Self::Policy, crate::Error> {
        Ok(self)
    }
}

impl<R, V> Policy<R> for Visit<V>
where
    R: row::raw::Decode,
    V: for<'row> FnMut(R::Borrowed<'row>) -> ops::ControlFlow<()>,
{
    type Output = ();

    fn row<'row>(
        &mut self,
        value: R::Borrowed<'row>,
    ) -> result::Result<ops::ControlFlow<Self::Output>, crate::Error> {
        Ok((self.visitor)(value))
    }

    fn done(self) -> result::Result<Self::Output, crate::Error> {
        Ok(())
    }
}

const _: () = assert!(mem::size_of::<Visit<()>>() == 0);
