use crate::{config, row};
use std::{ops, result};

pub(super) mod collect;
pub(super) mod single;
pub(super) mod visit;

pub(crate) trait Create<R: row::raw::Decode> {
    type Policy: Policy<R>;

    fn create(self, config: &config::Config) -> result::Result<Self::Policy, crate::Error>;
}

pub(crate) trait Policy<R: row::raw::Decode> {
    type Output;

    fn row<'row>(
        &mut self,
        value: R::Borrowed<'row>,
    ) -> result::Result<ops::ControlFlow<Self::Output>, crate::Error>;

    fn done(self) -> result::Result<Self::Output, crate::Error>;
}
