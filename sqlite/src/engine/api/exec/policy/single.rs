use crate::row::modes::{self, Kind as _};
use crate::{api, config, row};
use api::{batches, exec};
use exec::{Create, Policy};
use std::{ops, result};

pub(crate) struct Single<'query, 'owner: 'query, R: row::raw::Decode, M> {
    init: <R::Retention as modes::Kind>::Init<'query, 'owner>,
    finish: M,
}

pub(crate) struct Take<'query, R: row::raw::Decode, M> {
    state: <R::Retention as modes::Kind>::State<'query, ()>,
    value: Option<R::Borrowed<'query>>,
    finish: M,
}

impl<'query, 'owner: 'query, R: row::raw::Decode, M> Single<'query, 'owner, R, M> {
    pub(crate) fn new(arena: &'query mut batches::Arena<'owner>, finish: M) -> Self {
        Self {
            init: R::Retention::init(modes::Input::new(arena)),
            finish,
        }
    }
}

impl<'query, 'owner: 'query, R, M> Create<R> for Single<'query, 'owner, R, M>
where
    R: row::raw::Decode + 'query,
    Take<'query, R, M>: Policy<R>,
{
    type Policy = Take<'query, R, M>;

    fn create(self, _config: &config::Config) -> result::Result<Self::Policy, crate::Error> {
        let state = R::Retention::start(self.init, 1)?;
        Ok(Take {
            state,
            value: None,
            finish: self.finish,
        })
    }
}

impl<'query, R, M, O> Policy<R> for Take<'query, R, M>
where
    R: row::raw::Decode + 'query,
    M: FnOnce(Option<R::Borrowed<'query>>) -> result::Result<O, crate::Error>,
{
    type Output = O;

    fn row<'row>(
        &mut self,
        value: R::Borrowed<'row>,
    ) -> result::Result<ops::ControlFlow<Self::Output>, crate::Error> {
        if self.value.is_none() {
            self.value = Some(R::retain(value, &mut self.state)?);
        }
        Ok(ops::ControlFlow::Continue(()))
    }

    fn done(self) -> result::Result<Self::Output, crate::Error> {
        (self.finish)(self.value)
    }
}
