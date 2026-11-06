use o3::{alloc, mem::Rejected};
use std::{cell, marker, mem, num};

use crate::protocol::parser;
use parser::{capacity, limit};

mod cursors;
mod gate;
pub(in crate::protocol::parser) use cursors::{Array, Batch, Cursor, Read};
mod tape;
#[doc(hidden)]
pub use gate::Lane;
#[doc(hidden)]
pub use tape::Tape;

/// Fixed parser storage owned by the Redis port for its complete driver lifetime.
///
pub struct Storage<'d> {
    tapes: parser::Pool,
    gate: gate::Gate<'d>,
}

/// An in-progress tape whose nodes are already in their final retained storage.
pub(crate) struct Builder<'a, 'd> {
    pub(crate) access: gate::Access<'a, 'd>,
    tail: Option<parser::Key<parser::Occupied>>,
    len: u32,
}

pub(super) struct Stored<'a, 'b, 'd> {
    builder: &'a mut Builder<'b, 'd>,
    held: gate::Held<'b, 'd>,
    tail: parser::Key<parser::Occupied>,
    len: num::NonZero<u32>,
}

pub(super) struct Finished<'a, 'g> {
    pub(super) nodes: Tape<'a>,
    pub(super) lane: Lane<'a, 'g>,
}

pub(super) enum Error {
    Blocked(limit::Slot),
    Full(limit::Slot),
}

#[derive(Clone, Copy)]
pub(crate) enum Raw {}

#[derive(Clone, Copy)]
pub(crate) enum Maybe {}

#[derive(Clone, Copy)]
pub(crate) enum Scored {}

#[derive(Clone, Copy)]
pub(crate) enum Coordinate {}

#[derive(Clone, Copy)]
pub(crate) enum Bulk<const W: usize> {}

pub(crate) struct Region<'a, K> {
    tape: Tape<'a>,
    start: Cursor<K>,
    len: u32,
}

/// The exact bulk batch proven on this retained tape.
pub(crate) struct Exact<'a, const N: usize> {
    tape: Tape<'a>,
    batch: Batch<N, Bulk<1>>,
}

/// An array length that does not satisfy the requested width.
#[derive(Debug)]
pub struct Length {
    pub width: usize,
    pub actual: usize,
}

pub(crate) enum Mismatch<S = parser::Span> {
    Server(S),
    Value(parser::Value<S>),
}

pub(crate) enum CoordError<E, S = parser::Span> {
    Array(Mismatch<S>),
    Member(Mismatch<S>),
    Length { actual: usize },
    Number(E),
}

pub(crate) enum Group<S = parser::Span> {
    Length { width: usize, actual: usize },
    Mismatch(Mismatch<S>),
}

pub(crate) enum Score<E, S = parser::Span> {
    Length { width: usize, actual: usize },
    Server(S),
    Member(parser::Value<S>),
    Number(E),
}

impl<'d> Storage<'d> {
    #[doc(hidden)]
    pub fn try_new(capacity: capacity::Capacity) -> Result<Self, alloc::Error> {
        let conns = capacity.connections();
        Ok(Self {
            tapes: parser::Pool::try_new(capacity.get())?,
            gate: gate::Gate::try_new(conns, capacity.lanes())?,
        })
    }

    pub(crate) const NODE_SIZE: usize = mem::size_of::<cell::Cell<parser::Node>>();

    pub(super) fn builder(&self) -> Builder<'_, 'd> {
        Builder {
            access: gate::Access::new(self),
            tail: None,
            len: 0,
        }
    }
}

impl<'a, 'd> Builder<'a, 'd> {
    pub(super) fn len(&self) -> u32 {
        self.len
    }

    pub(super) fn try_push(
        &mut self,
        slot: limit::Slot,
        value: parser::Kind,
    ) -> Result<Stored<'_, 'a, 'd>, Error> {
        match self.access.acquire() {
            gate::Acquire::Acquired(held) => {
                let Some(free) = self.access.storage().tapes.reserve() else {
                    return Err(Error::Full(slot));
                };
                let (value, len) = slot.commit(value);
                let tail = free.append(self.tail, value);
                self.tail = Some(tail);
                self.len = len.get();
                Ok(Stored {
                    builder: self,
                    held,
                    tail,
                    len,
                })
            }
            gate::Acquire::Blocked => Err(Error::Blocked(slot)),
        }
    }

    pub(super) fn reset(&mut self) {
        let next = self.access.storage().builder();
        *self = next;
    }
}

impl<'a, 'g> Stored<'_, 'a, 'g> {
    pub(super) fn finish(self) -> Finished<'a, 'g> {
        self.builder.tail = None;
        self.builder.len = 0;
        let storage = self.builder.access.storage();
        Finished {
            nodes: Tape {
                pool: &storage.tapes,
                tail: self.tail,
                len: self.len,
            },
            lane: self.builder.access.finish(self.held),
        }
    }
}

impl Drop for Builder<'_, '_> {
    fn drop(&mut self) {
        self.access.storage().tapes.release(self.tail);
        self.access.release();
    }
}

impl<'a> Region<'a, Raw> {
    pub(in crate::protocol::parser) fn bulks<const W: usize>(
        self,
    ) -> Result<Region<'a, Bulk<W>>, Group> {
        let () = Batch::<W>::VALID;
        let actual = self.len as usize;
        if !actual.is_multiple_of(W) {
            return Err(Group::Length { width: W, actual });
        }
        let mut cursor = self.start;
        while let Some(batch) = cursor.batch::<1>() {
            let read = self
                .tape
                .take(batch)
                .map_err(|span| Group::Mismatch(Mismatch::Server(span)))?;
            if !matches!(read.value, parser::Value::Bulk(_)) {
                return Err(Group::Mismatch(Mismatch::Value(read.value)));
            }
            cursor = read.cursor;
        }
        Ok(Region {
            tape: self.tape,
            start: self.start.cast(),
            len: (actual / W) as u32,
        })
    }

    pub(in crate::protocol::parser) fn optional(self) -> Result<Region<'a, Maybe>, Mismatch> {
        let mut cursor = self.start;
        while let Some(batch) = cursor.batch::<1>() {
            let read = self.tape.take(batch).map_err(Mismatch::Server)?;
            if !matches!(read.value, parser::Value::Nil | parser::Value::Bulk(_)) {
                return Err(Mismatch::Value(read.value));
            }
            cursor = read.cursor;
        }
        Ok(Region {
            tape: self.tape,
            start: self.start.cast(),
            len: self.len,
        })
    }

    pub(in crate::protocol::parser) fn scored<E>(
        self,
        mut number: impl FnMut(parser::Value) -> Result<f64, E>,
    ) -> Result<Region<'a, Scored>, Score<E>> {
        let actual = self.len as usize;
        if !actual.is_multiple_of(2) {
            return Err(Score::Length { width: 2, actual });
        }
        let mut cursor = self.start;
        while let Some(batch) = cursor.batch::<2>() {
            let member = self.tape.pool.read(batch.next);
            let value = member.try_value().map_err(Score::Server)?;
            if !matches!(value, parser::Value::Bulk(_)) {
                return Err(Score::Member(value));
            }
            let key = member.next();
            let score = self.tape.pool.read(key);
            let value = score.try_value().map_err(Score::Server)?;
            let value = number(value).map_err(Score::Number)?;
            self.tape.pool.write(key, score.with_float(value));
            cursor = batch.finish(score.next());
        }
        Ok(Region {
            tape: self.tape,
            start: self.start.cast(),
            len: (actual / 2) as u32,
        })
    }
}

impl<K> Region<'_, K> {
    pub(in crate::protocol::parser) fn len(&self) -> usize {
        self.len as usize
    }

    pub(in crate::protocol::parser) fn cursor(&self) -> Cursor<K> {
        Cursor {
            next: self.start.next,
            remaining: self.start.remaining,
            state: marker::PhantomData,
        }
    }
}

impl<'a> Region<'a, Bulk<1>> {
    pub(in crate::protocol::parser) fn try_chunks<const N: usize>(
        self,
    ) -> Result<Region<'a, Bulk<N>>, Rejected<Self, Length>> {
        let () = Batch::<N>::VALID;
        let actual = self.len as usize;
        if !actual.is_multiple_of(N) {
            return Err(Rejected::new(self, Length { width: N, actual }));
        }
        Ok(Region {
            tape: self.tape,
            start: self.start.cast(),
            len: (actual / N) as u32,
        })
    }

    pub(in crate::protocol::parser) fn try_exact<const N: usize>(
        self,
    ) -> Result<Exact<'a, N>, Rejected<Self, Length>> {
        match self.start.batch_exact() {
            Ok(batch) => Ok(Exact {
                tape: self.tape,
                batch,
            }),
            Err(actual) => Err(Rejected::new(self, Length { width: N, actual })),
        }
    }
}

impl<'a, const W: usize> Region<'a, Bulk<W>> {
    pub(in crate::protocol::parser) fn into_bulks(self) -> Region<'a, Bulk<1>> {
        Region {
            tape: self.tape,
            start: self.start.cast(),
            len: self.start.remaining,
        }
    }

    pub(in crate::protocol::parser) fn bulks<const N: usize>(
        &self,
        batch: Batch<N, Bulk<W>>,
    ) -> Read<[parser::Span; N], Bulk<W>> {
        self.tape.bulks(batch)
    }
}

impl<'a, const N: usize> Exact<'a, N> {
    pub(in crate::protocol::parser) fn bulks(&self) -> [parser::Span; N] {
        self.tape.bulks(self.batch).value
    }

    pub(in crate::protocol::parser) fn into_bulks(self) -> Region<'a, Bulk<1>> {
        Region {
            tape: self.tape,
            start: Cursor {
                next: self.batch.next,
                remaining: N as u32,
                state: marker::PhantomData,
            },
            len: N as u32,
        }
    }
}

impl Region<'_, Maybe> {
    pub(in crate::protocol::parser) fn optionals<const N: usize>(
        &self,
        batch: Batch<N, Maybe>,
    ) -> Read<[Option<parser::Span>; N], Maybe> {
        let mut next = batch.next;
        let value = [(); N].map(|()| {
            let node = self.tape.pool.read(next);
            next = node.next();
            node.maybe_span()
        });
        Read {
            value,
            cursor: batch.finish(next),
        }
    }
}

impl Region<'_, Scored> {
    pub(in crate::protocol::parser) fn scored(
        &self,
        batch: Batch<2, Scored>,
    ) -> Read<(parser::Span, f64), Scored> {
        let member = self.tape.pool.read(batch.next);
        let score = self.tape.pool.read(member.next());
        Read {
            value: (
                parser::Span::from_stored(member.payload),
                f64::from_bits(score.payload),
            ),
            cursor: batch.finish(score.next()),
        }
    }
}

impl Region<'_, Coordinate> {
    /// Reads only shapes and float payloads established by `Tape::coordinates`.
    pub(in crate::protocol::parser) fn coordinate(
        &self,
        cursor: Cursor<Coordinate>,
    ) -> Option<Read<Option<[f64; 2]>, Coordinate>> {
        let batch = cursor.batch::<1>()?;
        let member = self.tape.pool.read(batch.next);
        let (value, next) = if matches!(member.meta.tag, parser::Tag::Nil) {
            (None, member.next())
        } else {
            let lon = self.tape.pool.read(member.next());
            let lat = self.tape.pool.read(lon.next());
            (
                Some([f64::from_bits(lon.payload), f64::from_bits(lat.payload)]),
                lat.next(),
            )
        };
        Some(Read {
            value,
            cursor: batch.finish(next),
        })
    }
}

const _: [(); 32] = [(); mem::size_of::<Region<'_, Raw>>()];
const _: [(); 24] = [(); mem::size_of::<Exact<'_, 1>>()];
const _: [(); 24] = [(); mem::size_of::<Option<Exact<'_, 1>>>()];
const _: [(); 4] = [(); mem::size_of::<Option<parser::Key<parser::Occupied>>>()];
const _: [(); 56] = [(); mem::size_of::<Builder<'_, '_>>()];
const _: [(); 16] = [(); mem::size_of::<Stored<'_, '_, '_>>()];
const _: [(); 16] = [(); mem::size_of::<Result<Stored<'_, '_, '_>, Error>>()];
const _: [(); 16] = [(); mem::size_of::<cell::Cell<parser::Node>>()];
