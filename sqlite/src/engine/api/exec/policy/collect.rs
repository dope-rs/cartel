use crate::row::modes::{self, Kind as _};
use crate::{api, config, row};
use api::{batches, exec};
use exec::{Create, Policy};
use std::{marker, ops, result};

pub(crate) struct Collect<'query, 'owner: 'query, R: row::raw::Decode> {
    init: <modes::Arena as modes::Kind>::Init<'query, 'owner>,
    row: marker::PhantomData<fn() -> R>,
}

pub(crate) struct All<'query, R: row::raw::Decode + 'query> {
    state: modes::ArenaState<'query, R::Borrowed<'query>>,
}

impl<'query, 'owner: 'query, R: row::raw::Decode> Collect<'query, 'owner, R> {
    pub(crate) fn new(arena: &'query mut batches::Arena<'owner>) -> Self {
        Self {
            init: modes::Arena::init(modes::Input::new(arena)),
            row: marker::PhantomData,
        }
    }
}

impl<'query, 'owner: 'query, R> Create<R> for Collect<'query, 'owner, R>
where
    R: row::raw::Decode + 'query,
{
    type Policy = All<'query, R>;

    fn create(self, config: &config::Config) -> result::Result<Self::Policy, crate::Error> {
        let state = modes::Arena::start(self.init, config.result_rows())?;
        Ok(All { state })
    }
}

impl<'query, R> Policy<R> for All<'query, R>
where
    R: row::raw::Decode + 'query,
{
    type Output = batches::Rows<'query, R>;

    fn row<'row>(
        &mut self,
        value: R::Borrowed<'row>,
    ) -> result::Result<ops::ControlFlow<Self::Output>, crate::Error> {
        let retained = R::retain(value, R::Retention::from_arena(&mut self.state))?;
        self.state.push(retained)?;
        Ok(ops::ControlFlow::Continue(()))
    }

    fn done(self) -> result::Result<Self::Output, crate::Error> {
        Ok(batches::Rows::new(self.state.finish()))
    }
}
