use crate::protocol::scram::{self, secret32};
use dope::{core::driver::route, manifold::client::conn};
use hmac::{HmacReset, Mac as _};
use o3::base::cell::region;
use o3::depot::queue::indexed::shared;
use sha2::Sha256;
use std::{mem, num};

const ITERATIONS_PER_TURN: u32 = 256;
const MIN_ITERATIONS: u32 = 4_096;
const MAX_ITERATIONS: u32 = 100_000;
const CACHE_ENTRIES: usize = 2;
const KDF_LANES: usize = 4;

#[derive(Clone, Copy, PartialEq, Eq)]
struct Iterations(num::NonZero<u32>);

impl Iterations {
    fn from_server(value: num::NonZero<u32>) -> Result<Self, crate::Error> {
        if !(MIN_ITERATIONS..=MAX_ITERATIONS).contains(&value.get()) {
            return Err(crate::Error::ScramIterations {
                actual: value.get(),
                minimum: MIN_ITERATIONS,
                maximum: MAX_ITERATIONS,
            });
        }
        Ok(Self(value))
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(in crate::protocol) struct KdfKey {
    first: secret32::Secret32,
    iterations: Iterations,
}

impl KdfKey {
    pub(super) fn from_server(
        first: &[u8; 32],
        rounds: num::NonZero<u32>,
    ) -> Result<Self, crate::Error> {
        let count = Iterations::from_server(rounds)?;
        Ok(Self {
            first: secret32::Secret32::copy(first),
            iterations: count,
        })
    }
}

#[repr(transparent)]
pub(super) struct Salted(secret32::Secret32);

impl Salted {
    pub(super) fn as_array(&self) -> &[u8; 32] {
        self.0.as_array()
    }
}

struct Kdf {
    prf: HmacReset<Sha256>,
    previous: secret32::Secret32,
    output: secret32::Secret32,
    remaining: u32,
}

impl Kdf {
    fn new(password: &[u8], key: &KdfKey) -> Self {
        Self {
            prf: scram::hmac(password),
            previous: key.first.clone(),
            output: key.first.clone(),
            remaining: key.iterations.0.get() - 1,
        }
    }

    fn advance(&mut self, budget: u32) -> Option<Salted> {
        for _ in 0..self.remaining.min(budget) {
            self.prf.update(self.previous.as_array());
            let output = self.prf.finalize_reset();
            self.previous = secret32::Secret32::copy(output.as_bytes().as_ref());
            for (output, next) in self.output.iter_mut().zip(self.previous.iter().copied()) {
                *output ^= next;
            }
            self.remaining -= 1;
        }
        (self.remaining == 0).then(|| Salted(self.output.clone()))
    }
}

struct CacheEntry {
    key: KdfKey,
    keys: scram::Keys,
}

struct Cache {
    entries: [Option<CacheEntry>; CACHE_ENTRIES],
    next: Next,
}

enum Next {
    First,
    Second,
}

impl Cache {
    const fn new() -> Self {
        Self {
            entries: [const { None }; CACHE_ENTRIES],
            next: Next::First,
        }
    }

    fn get(&self, key: &KdfKey) -> Option<scram::Keys> {
        self.entries
            .iter()
            .flatten()
            .find(|entry| &entry.key == key)
            .map(|entry| entry.keys.clone())
    }

    fn insert(&mut self, key: KdfKey, salted: Salted) -> scram::Keys {
        let keys = scram::Keys::new(salted);
        let entry = Some(CacheEntry {
            key,
            keys: keys.clone(),
        });
        match self.next {
            Next::First => {
                self.entries[0] = entry;
                self.next = Next::Second;
            }
            Next::Second => {
                self.entries[1] = entry;
                self.next = Next::First;
            }
        }
        keys
    }
}

struct Running<'d, const ID: u8> {
    owner: conn::Id<'d, route::KeyTag<ID>>,
    key: KdfKey,
    kdf: Kdf,
}

#[repr(transparent)]
struct Lanes<'d, const ID: u8>([Option<Running<'d, ID>>; KDF_LANES]);

impl<'d, const ID: u8> Lanes<'d, ID> {
    const fn new() -> Self {
        Self([const { None }; KDF_LANES])
    }

    fn remove(&mut self, owner: conn::Id<'d, route::KeyTag<ID>>) -> Option<Vacant<'_, 'd, ID>> {
        for slot in &mut self.0 {
            let Some(running) = slot else { continue };
            if running.owner == owner {
                *slot = None;
                return Some(Vacant(slot));
            }
        }
        None
    }

    fn vacant(&mut self) -> Option<Vacant<'_, 'd, ID>> {
        self.0.iter_mut().find(|slot| slot.is_none()).map(Vacant)
    }
}

struct Vacant<'a, 'd, const ID: u8>(&'a mut Option<Running<'d, ID>>);

impl<'d, const ID: u8> Vacant<'_, 'd, ID> {
    fn fill(self, running: Running<'d, ID>) {
        *self.0 = Some(running);
    }
}

pub(in crate::protocol) enum Drive<'d, const ID: u8> {
    Waiting,
    Running(conn::Id<'d, route::KeyTag<ID>>),
    Ready {
        keys: scram::Keys,
        wake: Option<conn::Id<'d, route::KeyTag<ID>>>,
    },
}

pub(super) struct Coordinator<'d, const ID: u8> {
    lanes: Lanes<'d, ID>,
    cache: Cache,
}

pub(in crate::protocol) struct Ticket<'a, 'd, const ID: u8> {
    pub(super) runtime: &'a scram::Runtime<'d, ID>,
    pub(super) waiter: shared::Slot<'a, conn::Id<'d, route::KeyTag<ID>>>,
    pub(super) owner: conn::Id<'d, route::KeyTag<ID>>,
}

impl<'d, const ID: u8> Coordinator<'d, ID> {
    pub(super) fn new() -> Self {
        Self {
            lanes: Lanes::new(),
            cache: Cache::new(),
        }
    }
}

impl<'d, const ID: u8> Ticket<'_, 'd, ID> {
    pub(in crate::protocol) fn poll(
        &self,
        key: &KdfKey,
        password: &[u8],
        token: &mut region::Token<region::Identity<'d>>,
    ) -> Drive<'d, ID> {
        let Coordinator { lanes, cache } = token.borrow_mut(&self.runtime.state);
        let conn = self.owner;
        if let Some(keys) = cache.get(key) {
            let front = self.runtime.waiters.front::<0>() == Some(conn);
            if self.waiter.get::<0>() == Some(conn) {
                self.waiter.clear::<0>();
            }
            let owned = lanes.remove(conn).is_some();
            let wake = (owned || (front && lanes.vacant().is_some()))
                .then(|| self.runtime.waiters.front::<0>())
                .flatten();
            return Drive::Ready { keys, wake };
        }
        for slot in &mut lanes.0 {
            let Some(running) = slot else { continue };
            if running.owner != conn {
                continue;
            }
            if &running.key != key {
                *slot = None;
                break;
            }
            let Some(salted) = running.kdf.advance(ITERATIONS_PER_TURN) else {
                return Drive::Running(conn);
            };
            let key = running.key.clone();
            *slot = None;
            let keys = cache.insert(key, salted);
            return Drive::Ready {
                keys,
                wake: self.runtime.waiters.front::<0>(),
            };
        }
        let Some(slot) = lanes.vacant() else {
            if self.waiter.get::<0>() != Some(conn) {
                self.waiter.push::<0>(conn);
            }
            return Drive::Waiting;
        };
        if let Some(front) = self.runtime.waiters.front::<0>() {
            if front != conn {
                if self.waiter.get::<0>() != Some(conn) {
                    self.waiter.push::<0>(conn);
                }
                return Drive::Waiting;
            }
            self.waiter.clear::<0>();
        }
        let mut kdf = Kdf::new(password, key);
        if let Some(salted) = kdf.advance(ITERATIONS_PER_TURN) {
            let keys = cache.insert(key.clone(), salted);
            return Drive::Ready {
                keys,
                wake: self.runtime.waiters.front::<0>(),
            };
        }
        slot.fill(Running {
            owner: conn,
            key: key.clone(),
            kdf,
        });
        Drive::Running(conn)
    }

    pub(in crate::protocol) fn cancel(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
    ) -> Option<conn::Id<'d, route::KeyTag<ID>>> {
        let state = token.borrow_mut(&self.runtime.state);
        let conn = self.owner;
        let front = self.runtime.waiters.front::<0>() == Some(conn);
        if self.waiter.get::<0>() == Some(conn) {
            self.waiter.clear::<0>();
        }
        let owned = state.lanes.remove(conn).is_some();
        (owned || (front && state.lanes.vacant().is_some()))
            .then(|| self.runtime.waiters.front::<0>())
            .flatten()
    }
}

const _: () = assert!(mem::size_of::<Salted>() == 32);
const _: () = assert!(mem::size_of::<KdfKey>() == 36);
const _: () = assert!(
    mem::size_of::<Drive<'static, 0>>()
        == mem::size_of::<scram::Keys>() + 3 * mem::size_of::<usize>()
);
const _: () = assert!(
    mem::size_of::<Lanes<'static, 0>>()
        == mem::size_of::<[Option<Running<'static, 0>>; KDF_LANES]>()
);

const _: () = {
    assert!(mem::size_of::<Ticket<'static, 'static, 0>>() == 5 * mem::size_of::<usize>());
    assert!(
        mem::size_of::<Option<Ticket<'static, 'static, 0>>>()
            == mem::size_of::<Ticket<'static, 'static, 0>>()
    );
};
