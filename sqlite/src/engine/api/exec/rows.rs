use crate::{api, catalogs, row};
use api::{exec, stmt};
use dope_fiber::{abi, context};
use exec::Policy as _;
use exec::machine::{self, State};
use std::{marker, num, ops, result, task};
use stmt::access;

type Machine<'query, 'connection, 'd, I, Q, P> =
    exec::Machine<'query, 'connection, 'd, I, P, catalogs::Statement<I, Q>>;

type RowState<'query, 'connection, 'd, I, Q, R, P, C> = State<
    (Machine<'query, 'connection, 'd, I, Q, P>, C),
    (
        machine::Lease<'query, 'connection, 'd, P, access::Plain>,
        <C as exec::Create<R>>::Policy,
    ),
>;

#[must_use = "a SQLite row request does nothing unless it is driven"]
pub(crate) struct Rows<
    'query,
    'connection: 'query,
    'd: 'connection,
    I: catalogs::Catalog + catalogs::HasQuery<Q>,
    Q: catalogs::TypedQuery,
    R: row::raw::Decode,
    P,
    C: exec::Create<R>,
> {
    state: RowState<'query, 'connection, 'd, I, Q, R, P, C>,
    remaining: usize,
    /// A present capacity proves the live row shape until a busy reset yields.
    byte_capacity: Option<num::NonZero<i32>>,
    row: marker::PhantomData<R>,
}

impl<
    'query,
    'connection: 'query,
    'd: 'connection,
    I: catalogs::Catalog + catalogs::HasQuery<Q>,
    Q: catalogs::TypedQuery,
    R: row::raw::Decode,
    P: api::Bind,
    C: exec::Create<R>,
> Rows<'query, 'connection, 'd, I, Q, R, P, C>
{
    pub(crate) const fn new(
        machine: Machine<'query, 'connection, 'd, I, Q, P>,
        create: C,
    ) -> abi::Owned<Self> {
        abi::Owned::new(Self {
            state: State::Start((machine, create)),
            remaining: 0,
            byte_capacity: None,
            row: marker::PhantomData,
        })
    }
}

impl<
    'query,
    'connection: 'query,
    'd: 'connection,
    I: catalogs::Catalog + catalogs::HasQuery<Q>,
    Q: catalogs::TypedQuery,
    R: row::raw::Decode,
    P: api::Bind,
    C: exec::Create<R>,
> abi::Resume<'d> for Rows<'query, 'connection, 'd, I, Q, R, P, C>
{
    type Output = result::Result<<C::Policy as exec::Policy<R>>::Output, crate::Error>;
    type Ready =
        Option<result::Result<Option<<C::Policy as exec::Policy<R>>::Output>, crate::Error>>;

    fn poll<'slot, 'id>(
        live: &mut abi::Live<'slot, 'id, Self>,
        admit: &mut context::Admission<'_, '_, 'd>,
    ) -> task::Poll<Self::Ready> {
        let mut context = admit.context();
        let this = live.get_mut();
        let (active, policy) = match &mut this.state {
            State::Start(_) => return task::Poll::Ready(None),
            State::Active((active, policy)) => (active.parts_mut().0, policy),
        };
        let mut polled = active.poll(context.as_mut());
        let result = loop {
            let step = match polled {
                task::Poll::Pending => {
                    this.byte_capacity = None;
                    return task::Poll::Pending;
                }
                task::Poll::Ready(Ok(step)) => step,
                task::Poll::Ready(Err(error)) => break Err(error),
            };
            let bytes = match this.byte_capacity {
                Some(bytes) => bytes,
                None => {
                    if let Err(error) = active.columns::<true>(R::N_COLS) {
                        break Err(error);
                    }
                    let bytes = active.config().row_bytes();
                    this.byte_capacity = Some(bytes);
                    bytes
                }
            };
            if matches!(step, stmt::Step::Done) {
                break Ok(None);
            }
            let Some(left) = this.remaining.checked_sub(1) else {
                let capacity = active.config().scan_rows();
                break Err(crate::Error::ScanRows { capacity });
            };
            this.remaining = left;
            let flow = match active.read::<R, _>(bytes.cast_unsigned(), policy) {
                Ok(flow) => flow,
                Err(error) => break Err(error),
            };
            if let ops::ControlFlow::Break(output) = flow {
                break Ok(Some(output));
            }
            match context.as_ref().try_continue() {
                context::Continue::Permit(permit) => {
                    polled = permit.run(|| active.poll(context.as_mut()));
                }
                context::Continue::Exhausted => return task::Poll::Pending,
            }
        };
        task::Poll::Ready(Some(result))
    }

    fn finish<'slot, 'id>(
        live: abi::Live<'slot, 'id, Self>,
        ready: Self::Ready,
        mut admit: context::Admission<'_, '_, 'd>,
    ) -> abi::Transition<'id, Self::Output> {
        let mut context = admit.context();
        let (vacant, mut this) = live.take();
        let (active, policy) = match this.state {
            State::Start((machine, create)) => {
                let active = match machine.poll(context.as_mut()) {
                    machine::Next::Pending(machine) => {
                        this.state = State::Start((machine, create));
                        return vacant.insert(this).pending();
                    }
                    machine::Next::Ready(Ok(active)) => active,
                    machine::Next::Ready(Err(error)) => return vacant.complete(Err(error)),
                };
                if let Err(error) = active.columns::<false>(R::N_COLS) {
                    return vacant.complete(active.finish(Err(error)));
                }
                let config = active.config();
                this.remaining = config.scan_rows();
                let policy = match create.create(config) {
                    Ok(policy) => policy,
                    Err(error) => return vacant.complete(active.finish(Err(error))),
                };
                (active, policy)
            }
            State::Active((active, policy)) => (active.into_parts().0, policy),
        };
        let result = match ready {
            Some(result) => result,
            None => {
                this.state = State::Active((machine::Lease::open(active, ()), policy));
                let mut live = vacant.insert(this);
                return match Self::poll(&mut live, &mut admit) {
                    task::Poll::Pending => live.pending(),
                    task::Poll::Ready(ready) => Self::finish(live, ready, admit),
                };
            }
        };
        let result = match result {
            Ok(None) => policy.done(),
            Ok(Some(output)) => Ok(output),
            Err(error) => Err(error),
        };
        vacant.complete(active.finish(result))
    }
}

impl<I, Q, R, P, C> Unpin for Rows<'_, '_, '_, I, Q, R, P, C>
where
    I: catalogs::Catalog + catalogs::HasQuery<Q>,
    Q: catalogs::TypedQuery,
    R: row::raw::Decode,
    C: exec::Create<R>,
{
}
