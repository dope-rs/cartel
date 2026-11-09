use crate::{api, config, row};
use api::{exec, stmt};
use dope_fiber::context;
use exec::machine::call;
use o3::mem;
use std::{num, ops, pin, result, task};
use stmt::access::Access;

pub(crate) struct Active<'query, 'connection, 'd, P, A: Access> {
    bound: stmt::Bound<'query, 'connection, P>,
    state: call::State<'query, 'd>,
    access: A,
}

impl<'query, 'connection, 'd, P, A: Access> Active<'query, 'connection, 'd, P, A> {
    pub(crate) const fn new(
        bound: stmt::Bound<'query, 'connection, P>,
        state: call::State<'query, 'd>,
        access: A,
    ) -> Self {
        Self {
            bound,
            state,
            access,
        }
    }

    pub(in crate::engine::api::exec) fn poll(
        &mut self,
        retry: bool,
        context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> task::Poll<result::Result<stmt::Step, crate::Error>> {
        self.state
            .poll(self.bound.prepared(), &self.access, retry, context)
    }

    pub(in crate::engine::api::exec) fn config(&self) -> &config::Config {
        self.state.config()
    }

    pub(in crate::engine::api::exec) fn columns<const CURRENT: bool>(
        &self,
        expected: usize,
    ) -> result::Result<(), crate::Error> {
        self.bound.columns::<CURRENT>(expected)
    }

    pub(in crate::engine::api::exec) fn read<R: row::raw::Decode, M: exec::Policy<R>>(
        &mut self,
        bytes: num::NonZero<u32>,
        policy: &mut M,
    ) -> result::Result<ops::ControlFlow<M::Output>, crate::Error> {
        policy.row(self.bound.read::<R>(bytes)?)
    }

    pub(in crate::engine::api::exec) fn finish<T>(
        mut self,
        result: result::Result<T, crate::Error>,
    ) -> result::Result<A::Output<T>, crate::Error> {
        let reset = self.state.reset(&mut self.bound, &self.access);
        match (result, reset) {
            (Ok(value), reset) => {
                reset?;
                Ok(self.access.complete(&self.state.guard, value))
            }
            (Err(error), Ok(())) => Err(error),
            (Err(error), Err(reset)) => {
                drop(self.state.defer(reset));
                Err(error)
            }
        }
    }

    pub(in crate::engine::api::exec) fn cancel(mut self) {
        if let Err(error) = self.state.reset(&mut self.bound, &self.access) {
            drop(self.state.defer(error));
        }
    }
}

impl<P, A: Access> mem::Return for Active<'_, '_, '_, P, A> {
    type Item = ();

    fn return_item(self, (): Self::Item) {
        self.cancel();
    }
}
