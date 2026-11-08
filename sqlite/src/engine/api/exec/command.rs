use crate::{api, catalogs};
use api::{exec, stmt};
use dope_fiber::{abi, context};
use exec::machine::{self, State};
use std::task;
use stmt::{access::Access, target::Target};

#[must_use = "a SQLite command does nothing unless it is driven"]
pub(crate) struct Command<
    'query,
    'connection: 'query,
    'd: 'connection,
    I: catalogs::Catalog,
    P,
    T: Target<I>,
> {
    state: State<
        exec::Machine<'query, 'connection, 'd, I, P, T>,
        machine::Lease<'query, 'connection, 'd, P, T::Access>,
    >,
}

impl<'query, 'connection: 'query, 'd: 'connection, I: catalogs::Catalog, P: api::Bind, T: Target<I>>
    Command<'query, 'connection, 'd, I, P, T>
{
    pub(crate) const fn new(
        machine: exec::Machine<'query, 'connection, 'd, I, P, T>,
    ) -> abi::Owned<Self> {
        abi::Owned::new(Self {
            state: State::Start(machine),
        })
    }
}

impl<'d, I: catalogs::Catalog, P: api::Bind, T: Target<I>> abi::Resume<'d>
    for Command<'_, '_, 'd, I, P, T>
{
    type Output = Result<<T::Access as Access>::Output<()>, crate::Error>;
    type Ready = Option<Result<stmt::Step, crate::Error>>;

    fn poll<'slot, 'id>(
        live: &mut abi::Live<'slot, 'id, Self>,
        admit: &mut context::Admission<'_, '_, 'd>,
    ) -> task::Poll<Self::Ready> {
        match &mut live.get_mut().state {
            State::Start(_) => task::Poll::Ready(None),
            State::Active(active) => active.parts_mut().0.poll(true, admit.context()).map(Some),
        }
    }

    fn finish<'slot, 'id>(
        mut live: abi::Live<'slot, 'id, Self>,
        ready: Self::Ready,
        mut admit: context::Admission<'_, '_, 'd>,
    ) -> abi::Transition<'id, Self::Output> {
        let mut context = admit.context();
        let cleaned = match &mut live.get_mut().state {
            State::Start(machine) => match machine.poll(context.as_mut()) {
                task::Poll::Pending => return live.pending(),
                task::Poll::Ready(result) => result,
            },
            State::Active(_) => Ok(()),
        };
        let (vacant, mut this) = live.take();
        let mut active = match this.state {
            State::Start(machine) => match machine.prepare(cleaned) {
                Ok(active) => active,
                Err(error) => return vacant.complete(Err(error)),
            },
            State::Active(active) => active.into_parts().0,
        };
        let result = match ready {
            Some(result) => result,
            None => match active.poll(true, context.as_mut()) {
                task::Poll::Pending => {
                    this.state = State::Active(machine::Lease::open(active, ()));
                    return vacant.insert(this).pending();
                }
                task::Poll::Ready(result) => result,
            },
        };
        let result = match result {
            Ok(stmt::Step::Done) => Ok(()),
            Ok(stmt::Step::Row) => Err(crate::Error::UnexpectedRows {
                expected: 0,
                minimum: 1,
            }),
            Err(error) => Err(error),
        };
        vacant.complete(active.finish(result))
    }
}

impl<I: catalogs::Catalog, P, T: Target<I>> Unpin for Command<'_, '_, '_, I, P, T> {}
