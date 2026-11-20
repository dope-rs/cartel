use crate::api::exec::{Create, Policy};
use crate::row::{modes, modes::Kind as _};
use crate::{config, row};
use std::{convert, ops, result};

impl<'query, R: row::raw::Decode + 'query> Create<R> for modes::Input<'query> {
    type Policy = modes::ArenaState<'query, R::Borrowed<'query>>;

    fn create(self, config: &config::Config) -> result::Result<Self::Policy, crate::Error> {
        modes::Arena::start(self, config.result_rows())
    }
}

impl<'query, R: row::raw::Decode + 'query> Policy<R>
    for modes::ArenaState<'query, R::Borrowed<'query>>
{
    type Output = &'query [R::Borrowed<'query>];
    type Break = convert::Infallible;

    fn row<'row>(
        &mut self,
        value: R::Borrowed<'row>,
    ) -> result::Result<ops::ControlFlow<Self::Break>, crate::Error> {
        let retained = R::retain(value, R::Retention::from_arena(self))?;
        self.push(retained)?;
        Ok(ops::ControlFlow::Continue(()))
    }

    fn done(self) -> result::Result<Self::Output, crate::Error> {
        Ok(self.finish())
    }
}
