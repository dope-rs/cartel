use crate::{api, config, row};
use api::exec::{Create, Policy};
use std::{ops, result};

impl<R, V> Create<R> for V
where
    R: row::raw::Decode,
    V: for<'row> FnMut(R::Borrowed<'row>) -> ops::ControlFlow<()>,
{
    type Policy = Self;

    fn create(self, _config: &config::Config) -> result::Result<Self::Policy, crate::Error> {
        Ok(self)
    }
}

impl<R, V> Policy<R> for V
where
    R: row::raw::Decode,
    V: for<'row> FnMut(R::Borrowed<'row>) -> ops::ControlFlow<()>,
{
    type Output = ();
    type Break = ();

    fn row<'row>(
        &mut self,
        value: R::Borrowed<'row>,
    ) -> result::Result<ops::ControlFlow<Self::Break>, crate::Error> {
        Ok(self(value))
    }

    fn done(self) -> result::Result<Self::Output, crate::Error> {
        Ok(())
    }
}
