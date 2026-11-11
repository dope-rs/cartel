use crate::protocol::scram::{self, secret32};
use dope::{
    core::driver::{route, route::table::conn::Capacity},
    manifold::client::conn,
};
use o3::{
    alloc,
    base::{cell::region, num::bounded},
    depot::{
        array,
        queue::{
            indexed::shared,
            round::{self, values},
        },
    },
};
use sha2::block_api::compress256;
use std::{iter, mem, num};
use zeroize::Zeroizing;

const ITERATIONS_PER_POLL: u32 = 256;
const MIN_ITERATIONS: u32 = 4_096;
const MAX_ITERATIONS: u32 = 100_000;
const CACHE_ENTRIES: usize = 2;
const KDF_LANES: usize = 4;
const ENTRIES: usize = KDF_LANES + CACHE_ENTRIES;
type Index = array::Index<ENTRIES>;
const LANES: [bounded::U32<0, 6>; ENTRIES] = [
    bounded::U32::exact::<0>(),
    bounded::U32::exact::<1>(),
    bounded::U32::exact::<2>(),
    bounded::U32::exact::<3>(),
    bounded::U32::exact::<4>(),
    bounded::U32::exact::<5>(),
];

fn lane(index: Option<Index>) -> bounded::U32<0, 6> {
    index.map_or(bounded::U32::exact::<6>(), |index| *index.get(&LANES))
}

#[derive(PartialEq, Eq)]
pub(in crate::protocol) struct KdfKey {
    pub(super) first: secret32::Secret32,
    pub(super) iterations: bounded::NonZeroU32<MIN_ITERATIONS, MAX_ITERATIONS>,
}

pub(super) fn try_iterations(
    rounds: num::NonZero<u32>,
) -> Result<bounded::NonZeroU32<MIN_ITERATIONS, MAX_ITERATIONS>, crate::Error> {
    bounded::NonZeroU32::new(rounds.get()).ok_or(crate::Error::ScramIterations {
        actual: rounds.get(),
        minimum: MIN_ITERATIONS,
        maximum: MAX_ITERATIONS,
    })
}

struct Kdf {
    prf: [Zeroizing<[u32; 8]>; 2],
    previous: secret32::Secret32,
    output: secret32::Secret32,
    remaining: u32,
}

impl Kdf {
    fn advance(&mut self) -> Option<&[u8; 32]> {
        let rounds = self.remaining.min(ITERATIONS_PER_POLL);
        let mut block = Zeroizing::new([0u8; 64]);
        let mut state = Zeroizing::new([0u32; 8]);
        block[..32].copy_from_slice(self.previous.as_array());
        scram::pad(&mut state, &mut block, 96);
        for _ in 0..rounds {
            for seed in &self.prf {
                *state = **seed;
                compress256(&mut state, std::slice::from_ref(&*block));
                scram::write_state(&state, &mut block.as_chunks_mut::<32>().0[0]);
            }
            for (output, word) in iter::zip(
                self.output.as_array_mut().as_chunks_mut::<4>().0,
                state.iter(),
            ) {
                *output = (u32::from_be_bytes(*output) ^ *word).to_be_bytes();
            }
        }
        self.previous.as_array_mut().copy_from_slice(&block[..32]);
        self.remaining -= rounds;
        (self.remaining == 0).then(|| self.output.as_array())
    }
}

enum State {
    Computing(Kdf),
    Ready(scram::Keys),
}

struct Entry {
    key: KdfKey,
    state: State,
}

#[derive(Default)]
struct Coordinator {
    entries: [Option<Entry>; ENTRIES],
    cached: [Option<Index>; CACHE_ENTRIES],
    second: bool,
}

impl Coordinator {
    fn select(&mut self, key: &KdfKey, password: Option<&[u8]>) -> Option<Index> {
        for index in self.cached.iter().copied().flatten() {
            if index
                .get(&self.entries)
                .as_ref()
                .is_some_and(|entry| &entry.key == key)
            {
                return Some(index);
            }
        }
        let mut empty = None;
        let mut computing = 0;
        for index in Index::iter() {
            if self.cached.contains(&Some(index)) {
                continue;
            }
            let Some(entry) = index.get(&self.entries) else {
                empty = Some(index);
                continue;
            };
            if &entry.key == key {
                return Some(index);
            }
            computing += usize::from(matches!(entry.state, State::Computing(_)));
        }
        let password = password?;
        if computing == KDF_LANES {
            return None;
        }
        let index = match empty {
            Some(index) => index,
            None => self.cached[usize::from(self.second)]
                .take()
                .or_else(|| self.cached[usize::from(!self.second)].take())?,
        };
        let Entry { key: owned, state } = index.get_mut(&mut self.entries).insert(Entry {
            key: KdfKey {
                first: secret32::Secret32::new([0; 32]),
                iterations: key.iterations,
            },
            state: State::Computing(Kdf {
                prf: [Zeroizing::new(scram::IV), Zeroizing::new(scram::IV)],
                previous: secret32::Secret32::new([0; 32]),
                output: secret32::Secret32::new([0; 32]),
                remaining: key.iterations.get() - 1,
            }),
        });
        let State::Computing(kdf) = state else {
            unreachable!("kdf")
        };
        let [inner, outer] = &mut kdf.prf;
        scram::hmac(password, inner, outer);
        for output in [&mut owned.first, &mut kdf.previous, &mut kdf.output] {
            output.as_array_mut().copy_from_slice(key.first.as_array());
        }
        Some(index)
    }

    fn cache(&mut self, index: Index) {
        let previous = self.cached[usize::from(self.second)].replace(index);
        self.second = !self.second;
        if let Some(previous) = previous {
            *previous.get_mut(&mut self.entries) = None;
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct Member<'d, const ID: u8> {
    owner: conn::Id<'d, route::KeyTag<ID>>,
    job: Option<Index>,
}

pub(crate) struct Runtime<'d, const ID: u8> {
    waiters: values::Values<Member<'d, ID>, 1, { ENTRIES + 1 }>,
    state: region::Value<region::Identity<'d>, Coordinator>,
}

impl<'d, const ID: u8> Runtime<'d, ID> {
    pub(crate) fn try_new(
        cap: Capacity,
        proof: region::Identity<'d>,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            waiters: values::Values::try_fixed(cap.entries().get())?,
            state: proof.bind(Coordinator::default()),
        })
    }

    pub(super) fn try_entry(
        &self,
        owner: conn::Id<'d, route::KeyTag<ID>>,
    ) -> Result<Ticket<'_, 'd, ID>, shared::Bounds> {
        let waiter =
            self.waiters
                .try_slot(owner.index().into_usize())
                .map_err(|error| match error {
                    round::Bounds::Slot { index, capacity } => shared::Bounds { index, capacity },
                    round::Bounds::Lane { .. } => unreachable!("slot"),
                })?;
        waiter.write(Member { owner, job: None });
        Ok(Ticket {
            runtime: self,
            waiter,
        })
    }

    fn front(&self, index: Option<Index>) -> Option<conn::Id<'d, route::KeyTag<ID>>> {
        self.waiters
            .lane(lane(index))
            .front()
            .map(|(_, member)| member.owner)
    }
}

pub(in crate::protocol) struct Ticket<'a, 'd, const ID: u8> {
    runtime: &'a Runtime<'d, ID>,
    waiter: values::Slot<'a, Member<'d, ID>, 1, { ENTRIES + 1 }>,
}

impl<'d, const ID: u8> Ticket<'_, 'd, ID> {
    pub(in crate::protocol) fn poll<R>(
        &self,
        key: &KdfKey,
        password: &[u8],
        token: &mut region::Token<region::Identity<'d>>,
        mut wake: impl FnMut(conn::Id<'d, route::KeyTag<ID>>),
        ready: impl FnOnce(&scram::Keys) -> R,
    ) -> Option<R> {
        let member = self.waiter.get()?;
        let coordinator = token.borrow_mut(&self.runtime.state);
        let index = match member.job {
            Some(index) => index,
            None => {
                let front = self.runtime.front(None);
                let admission = front
                    .is_none_or(|front| front == member.owner)
                    .then_some(password);
                let Some(index) = coordinator.select(key, admission) else {
                    self.waiter.lane(lane(None)).set(|_| Some(()));
                    return None;
                };
                index
            }
        };
        let Some(entry) = index.get_mut(&mut coordinator.entries) else {
            unreachable!("kdf")
        };
        let completed = if let State::Computing(kdf) = &mut entry.state {
            if member.job.is_none() {
                let front = self.runtime.front(None) == Some(member.owner);
                self.waiter.lane(lane(Some(index))).push(Member {
                    job: Some(index),
                    ..member
                });
                if front && let Some(next) = self.runtime.front(None) {
                    wake(next);
                }
            }
            if self.runtime.front(Some(index)) != Some(member.owner) {
                return None;
            }
            let Some(salted) = kdf.advance() else {
                wake(member.owner);
                return None;
            };
            let mut inner = Zeroizing::new(scram::IV);
            let mut outer = Zeroizing::new(scram::IV);
            scram::hmac(salted, &mut inner, &mut outer);
            entry.state = State::Ready(scram::Keys {
                client: secret32::Secret32::new([0; 32]),
                stored: secret32::Secret32::new([0; 32]),
                server: secret32::Secret32::new([0; 32]),
            });
            let State::Ready(keys) = &mut entry.state else {
                unreachable!("ready")
            };
            keys.write(&inner, &outer);
            true
        } else {
            false
        };
        let (next, remaining) = self.release(coordinator);
        if let Some(next) = next {
            wake(next);
        }
        if completed
            && remaining
            && let Some(next) = self.runtime.front(None)
        {
            wake(next);
        }
        let Some(Entry {
            state: State::Ready(keys),
            ..
        }) = index.get(&coordinator.entries)
        else {
            unreachable!("ready")
        };
        Some(ready(keys))
    }

    pub(in crate::protocol) fn cancel(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
    ) -> Option<conn::Id<'d, route::KeyTag<ID>>> {
        self.release(token.borrow_mut(&self.runtime.state)).0
    }

    fn release(
        &self,
        coordinator: &mut Coordinator,
    ) -> (Option<conn::Id<'d, route::KeyTag<ID>>>, bool) {
        let Some(member) = self.waiter.get() else {
            return (None, false);
        };
        let front = self.runtime.front(member.job) == Some(member.owner);
        self.waiter.take();
        if let Some(index) = member.job {
            let next = self.runtime.front(Some(index));
            if next.is_none() {
                if matches!(
                    index.get(&coordinator.entries),
                    Some(Entry {
                        state: State::Ready(_),
                        ..
                    })
                ) {
                    coordinator.cache(index);
                } else {
                    *index.get_mut(&mut coordinator.entries) = None;
                }
                return (self.runtime.front(None), false);
            }
            return (front.then_some(next).flatten(), true);
        }
        (front.then(|| self.runtime.front(None)).flatten(), false)
    }
}

const _: () = assert!(mem::size_of::<KdfKey>() == 36);
const _: () = {
    assert!(mem::size_of::<Ticket<'static, 'static, 0>>() == 3 * mem::size_of::<usize>());
    assert!(
        mem::size_of::<Option<Ticket<'static, 'static, 0>>>()
            == mem::size_of::<Ticket<'static, 'static, 0>>()
    );
};
