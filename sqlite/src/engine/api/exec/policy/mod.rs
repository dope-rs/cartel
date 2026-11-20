use crate::{config, row};
use std::{convert, ops, result};

mod collect;
pub(super) mod single;
pub(super) mod visit;

pub(crate) trait Break<T> {
    fn complete(self) -> T;
}

impl<T> Break<T> for convert::Infallible {
    fn complete(self) -> T {
        match self {}
    }
}

impl Break<()> for () {
    fn complete(self) {}
}

pub(crate) trait Create<R: row::raw::Decode> {
    type Policy: Policy<R>;

    fn create(self, config: &config::Config) -> result::Result<Self::Policy, crate::Error>;
}

pub(crate) trait Policy<R: row::raw::Decode> {
    type Output;
    type Break: Break<Self::Output>;

    fn row<'row>(
        &mut self,
        value: R::Borrowed<'row>,
    ) -> result::Result<ops::ControlFlow<Self::Break>, crate::Error>;

    fn done(self) -> result::Result<Self::Output, crate::Error>;
}
