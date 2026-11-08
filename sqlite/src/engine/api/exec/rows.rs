use crate::{api, catalogs, row};
use api::{exec, stmt};
use dope_fiber::{abi, context};
use exec::Policy as _;
use exec::machine::{self, State};
use std::{num, ops, result, task};
use stmt::access;

type Machine<'query, 'connection, 'd, I, Q, P> =
    exec::Machine<'query, 'connection, 'd, I, P, catalogs::Statement<I, Q>>;

type RowState<'query, 'connection, 'd, I, Q, R, P, C> = State<
    (Machine<'query, 'connection, 'd, I, Q, P>, C),
    (
        machine::Lease<'query, 'connection, 'd, P, access::Plain>,
        <C as exec::Create<R>>::Policy,
        usize,
        Option<num::NonZero<i32>>,
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
        let (active, policy, remaining, byte_capacity) = match &mut this.state {
            State::Start(_) => return task::Poll::Ready(None),
            State::Active((active, policy, remaining, bytes)) => {
                (active.parts_mut().0, policy, remaining, bytes)
            }
        };
        let mut polled = active.poll(byte_capacity.is_none(), context.as_mut());
        let result = loop {
            let step = match polled {
                task::Poll::Pending => return task::Poll::Pending,
                task::Poll::Ready(Ok(step)) => step,
                task::Poll::Ready(Err(error)) => break Err(error),
            };
            let bytes = match *byte_capacity {
                Some(bytes) => bytes,
                None => {
                    if let Err(error) = active.columns::<true>(R::N_COLS) {
                        break Err(error);
                    }
                    let bytes = active.config().row_bytes();
                    *byte_capacity = Some(bytes);
                    bytes
                }
            };
            if matches!(step, stmt::Step::Done) {
                break Ok(None);
            }
            let Some(left) = remaining.checked_sub(1) else {
                let capacity = active.config().scan_rows();
                break Err(crate::Error::ScanRows { capacity });
            };
            *remaining = left;
            let flow = match active.read::<R, _>(bytes.cast_unsigned(), policy) {
                Ok(flow) => flow,
                Err(error) => break Err(error),
            };
            if let ops::ControlFlow::Break(output) = flow {
                break Ok(Some(output));
            }
            match context.as_ref().try_continue() {
                context::Continue::Permit(permit) => {
                    polled = permit.run(|| active.poll(false, context.as_mut()));
                }
                context::Continue::Exhausted => return task::Poll::Pending,
            }
        };
        task::Poll::Ready(Some(result))
    }

    fn finish<'slot, 'id>(
        mut live: abi::Live<'slot, 'id, Self>,
        ready: Self::Ready,
        mut admit: context::Admission<'_, '_, 'd>,
    ) -> abi::Transition<'id, Self::Output> {
        let mut context = admit.context();
        let cleaned = match &mut live.get_mut().state {
            State::Start((machine, _)) => match machine.poll(context.as_mut()) {
                task::Poll::Pending => return live.pending(),
                task::Poll::Ready(result) => result,
            },
            State::Active(_) => Ok(()),
        };
        let (vacant, mut this) = live.take();
        let (active, policy, remaining) = match this.state {
            State::Start((machine, create)) => {
                let active = match machine.prepare(cleaned) {
                    Ok(active) => active,
                    Err(error) => return vacant.complete(Err(error)),
                };
                if let Err(error) = active.columns::<false>(R::N_COLS) {
                    return vacant.complete(active.finish(Err(error)));
                }
                let config = active.config();
                let remaining = config.scan_rows();
                let policy = match create.create(config) {
                    Ok(policy) => policy,
                    Err(error) => return vacant.complete(active.finish(Err(error))),
                };
                (active, policy, remaining)
            }
            State::Active((active, policy, remaining, _)) => {
                (active.into_parts().0, policy, remaining)
            }
        };
        let result = match ready {
            Some(result) => result,
            None => {
                this.state =
                    State::Active((machine::Lease::open(active, ()), policy, remaining, None));
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
