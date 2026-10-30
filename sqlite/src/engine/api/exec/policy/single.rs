use crate::row::modes::{self, Kind as _};
use crate::{api, config, row};
use api::{batches, exec};
use exec::{Create, Policy};
use std::{marker, ops, result};

pub(crate) struct One;

pub(crate) struct First;

pub(crate) struct Single<'query, 'owner: 'query, R: row::raw::Decode, M> {
    init: <R::Retention as modes::Kind>::Init<'query, 'owner>,
    marker: marker::PhantomData<M>,
}

pub(crate) struct Take<'query, R: row::raw::Decode, M> {
    state: <R::Retention as modes::Kind>::State<'query, ()>,
    value: Option<R::Borrowed<'query>>,
    marker: marker::PhantomData<M>,
}

impl<'query, 'owner: 'query, R: row::raw::Decode, M> Single<'query, 'owner, R, M> {
    pub(crate) fn new(arena: &'query mut batches::Arena<'owner>) -> Self {
        Self {
            init: R::Retention::init(modes::Input::new(arena)),
            marker: marker::PhantomData,
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
            marker: marker::PhantomData,
        })
    }
}

impl<'query, R: row::raw::Decode + 'query> Policy<R> for Take<'query, R, One> {
    type Output = R::Borrowed<'query>;

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
        self.value.ok_or(crate::Error::QueryReturnedNoRows)
    }
}

impl<'query, R: row::raw::Decode + 'query> Policy<R> for Take<'query, R, First> {
    type Output = Option<R::Borrowed<'query>>;

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
        Ok(self.value)
    }
}
