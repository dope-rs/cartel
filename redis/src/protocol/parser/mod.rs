use crate::protocol::{self, parser};
use dope::{core::io::transfer, manifold::client::session::codec};
pub use frame::Reader;
use o3::{
    alloc::{self, BoxSliceExt as _},
    buffer::{self, write::consume::scope},
};
use parser::limit::Body;
use std::{cell, marker, mem, num, ops};

pub mod capacity;
pub mod frame;
pub mod limit;
mod number;
mod state;
pub mod storage;

#[doc(hidden)]
pub const MAX_DEPTH: usize = 64;

#[derive(Clone, Copy)]
pub(crate) enum Kind {
    Nil,
    Ok,
    Status(Span),
    Error(Span),
    Integer(i64),
    Bulk(Span),
    Array { len: u32 },
}

#[derive(Clone, Copy)]
pub(crate) enum Value<S = Span> {
    Nil,
    Ok,
    Status(S),
    Integer(i64),
    Bulk(S),
    Array { len: u32 },
}

impl<S> Value<S> {
    pub(crate) fn map<T>(self, map: impl FnOnce(S) -> T) -> Value<T> {
        match self {
            Self::Nil => Value::Nil,
            Self::Ok => Value::Ok,
            Self::Status(span) => Value::Status(map(span)),
            Self::Integer(value) => Value::Integer(value),
            Self::Bulk(span) => Value::Bulk(map(span)),
            Self::Array { len } => Value::Array { len },
        }
    }
}

#[derive(Clone, Copy)]
#[repr(transparent)]
struct Key<S> {
    raw: num::NonZero<u32>,
    state: marker::PhantomData<S>,
}

#[derive(Clone, Copy)]
enum Vacant {}

#[derive(Clone, Copy)]
enum Occupied {}

impl<S> Key<S> {
    fn new(offset: u32) -> Self {
        let raw = num::NonZero::<u32>::MIN.saturating_add(offset);
        Self {
            raw,
            state: marker::PhantomData,
        }
    }

    const fn raw(self) -> num::NonZero<u32> {
        self.raw
    }

    fn offset(self) -> usize {
        self.raw.get() as usize - 1
    }
}

impl Key<Vacant> {
    const fn occupy(self) -> Key<Occupied> {
        Key {
            raw: self.raw,
            state: marker::PhantomData,
        }
    }
}

impl Key<Occupied> {
    const fn vacate(self) -> Key<Vacant> {
        Key {
            raw: self.raw,
            state: marker::PhantomData,
        }
    }
}

#[derive(Clone, Copy)]
#[repr(u8)]
enum Tag {
    Nil,
    Ok,
    Status,
    Error,
    Integer,
    Bulk,
    Array,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct Meta {
    next: num::NonZero<u32>,
    tag: Tag,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct Node {
    meta: Meta,
    payload: u64,
}

impl Node {
    const fn raw<S>(next: Key<S>, tag: Tag, data: u64) -> Self {
        Self {
            meta: Meta {
                next: next.raw(),
                tag,
            },
            payload: data,
        }
    }

    const fn vacant(next: Key<Vacant>) -> Self {
        Self::raw(next, Tag::Nil, 0)
    }

    fn new(next: Key<Occupied>, value: Kind) -> Self {
        match value {
            Kind::Nil => Self::raw(next, Tag::Nil, 0),
            Kind::Ok => Self::raw(next, Tag::Ok, 0),
            Kind::Status(span) => Self::raw(next, Tag::Status, span.stored()),
            Kind::Error(span) => Self::raw(next, Tag::Error, span.stored()),
            Kind::Integer(value) => Self::raw(next, Tag::Integer, value as u64),
            Kind::Bulk(span) => Self::raw(next, Tag::Bulk, span.stored()),
            Kind::Array { len } => Self::raw(next, Tag::Array, len as u64),
        }
    }

    const fn next(self) -> Key<Occupied> {
        Key {
            raw: self.meta.next,
            state: marker::PhantomData,
        }
    }

    const fn vacant_next(self) -> Key<Vacant> {
        Key {
            raw: self.meta.next,
            state: marker::PhantomData,
        }
    }

    const fn with_next(mut self, next: Key<Occupied>) -> Self {
        self.meta.next = next.raw();
        self
    }

    const fn with_free(mut self, next: Key<Vacant>) -> Self {
        self.meta.next = next.raw();
        self
    }

    fn with_float(mut self, value: f64) -> Self {
        self.payload = value.to_bits();
        self
    }

    fn kind(self) -> Kind {
        match self.meta.tag {
            Tag::Nil => Kind::Nil,
            Tag::Ok => Kind::Ok,
            Tag::Status => Kind::Status(Span::from_stored(self.payload)),
            Tag::Error => Kind::Error(Span::from_stored(self.payload)),
            Tag::Integer => Kind::Integer(self.payload as i64),
            Tag::Bulk => Kind::Bulk(Span::from_stored(self.payload)),
            Tag::Array => Kind::Array {
                len: self.payload as u32,
            },
        }
    }

    fn try_value(self) -> Result<Value, Span> {
        match self.meta.tag {
            Tag::Error => Err(Span::from_stored(self.payload)),
            Tag::Nil => Ok(Value::Nil),
            Tag::Ok => Ok(Value::Ok),
            Tag::Status => Ok(Value::Status(Span::from_stored(self.payload))),
            Tag::Integer => Ok(Value::Integer(self.payload as i64)),
            Tag::Bulk => Ok(Value::Bulk(Span::from_stored(self.payload))),
            Tag::Array => Ok(Value::Array {
                len: self.payload as u32,
            }),
        }
    }

    const fn maybe_span(self) -> Option<Span> {
        match self.meta.tag {
            Tag::Nil => None,
            Tag::Bulk => Some(Span::from_stored(self.payload)),
            _ => None,
        }
    }
}

pub struct Parser<'a, 'd> {
    state: state::State,
    pub(crate) builder: storage::Builder<'a, 'd>,
}

impl<'a, 'd> Parser<'a, 'd> {
    pub(super) fn new(store: &'a storage::Storage<'d>) -> Self {
        Self {
            state: state::State::new(),
            builder: store.builder(),
        }
    }

    pub(super) fn reset(&mut self) {
        self.state.reset();
        self.builder.reset();
    }

    pub(super) fn finish(
        &self,
        actual: num::NonZero<usize>,
        frame: limit::Bound<limit::Frame>,
    ) -> protocol::Failure {
        match frame.next(actual.get() - 1) {
            Ok(actual) => protocol::Failure::Invalid(self.state.finish(actual)),
            Err(error) => protocol::Failure::FrameCapacity(error),
        }
    }
}

pub(crate) enum Scan<'id, 'input, 'd, 'storage, 'g, R: buffer::Cursor> {
    Pending,
    Blocked,
    Complete(
        frame::Parsed<'input, 'd, 'storage, 'g, R>,
        scope::End<'id, { transfer::MAX_BYTES as u32 }>,
    ),
    Invalid(
        protocol::Error,
        scope::End<'id, { transfer::MAX_BYTES as u32 }>,
    ),
    FrameCapacity(
        limit::Exceeded<limit::Frame>,
        scope::End<'id, { transfer::MAX_BYTES as u32 }>,
    ),
    ValueCapacity(
        limit::Exceeded<limit::Value>,
        scope::End<'id, { transfer::MAX_BYTES as u32 }>,
    ),
    RetainCapacity,
    Source(scope::Error),
}

impl<'storage, 'g> Parser<'storage, 'g> {
    pub(crate) fn scan<'id, 'input, 'd, R: buffer::Cursor>(
        &mut self,
        buf: &codec::Input<'id, 'input, 'd, R>,
        frame: limit::Bound<limit::Frame>,
        value: limit::Bound<limit::Value>,
    ) -> Scan<'id, 'input, 'd, 'storage, 'g, R>
    where
        'd: 'input,
    {
        self.state.scan(&mut self.builder, buf, frame, value)
    }
}

const _: [(); 8] = [(); mem::size_of::<Span>()];
const _: [(); 8] = [(); mem::size_of::<Option<Span>>()];
const _: [(); 4] = [(); mem::size_of::<Key<Vacant>>()];
const _: [(); 4] = [(); mem::size_of::<Key<Occupied>>()];
const _: [(); 4] = [(); mem::size_of::<Option<Key<Vacant>>>()];
const _: [(); 4] = [(); mem::size_of::<Option<Key<Occupied>>>()];
const _: [(); 8] = [(); mem::size_of::<Meta>()];
const _: [(); 16] = [(); mem::size_of::<Kind>()];
const _: [(); 16] = [(); mem::size_of::<Value>()];
const _: [(); 16] = [(); mem::size_of::<Node>()];

pub(crate) struct Pool {
    nodes: Box<[cell::Cell<parser::Node>]>,
    free: cell::Cell<Option<parser::Key<parser::Vacant>>>,
}

pub(crate) struct Free<'d> {
    pool: &'d Pool,
    tail: parser::Key<parser::Vacant>,
}

impl Pool {
    fn try_new(capacity: usize) -> Result<Self, alloc::Error> {
        let nodes = Box::<[cell::Cell<parser::Node>]>::try_from_fn(capacity, |index| {
            let next = if index + 1 == capacity { 0 } else { index + 1 };
            cell::Cell::new(parser::Node::vacant(parser::Key::new(next as u32)))
        })?;
        Ok(Self {
            nodes,
            free: cell::Cell::new(
                capacity
                    .checked_sub(1)
                    .map(|offset| parser::Key::new(offset as u32)),
            ),
        })
    }

    /// Keys are minted from this allocation and remain inside its closed chain API.
    fn node<S>(&self, key: parser::Key<S>) -> &cell::Cell<parser::Node> {
        unsafe { self.nodes.get_unchecked(key.offset()) }
    }

    fn reserve(&self) -> Option<parser::Free<'_>> {
        self.free.get().map(|tail| Free { pool: self, tail })
    }

    fn release(&self, tail: Option<parser::Key<parser::Occupied>>) {
        let Some(tail) = tail else {
            return;
        };
        let Some(free) = self.free.get() else {
            self.free.set(Some(tail.vacate()));
            return;
        };
        let head = self.node(tail).get().next();
        let first = self.node(free).get().vacant_next();
        let tail_ref = self.node(tail);
        tail_ref.set(tail_ref.get().with_free(first));
        let free_ref = self.node(free);
        free_ref.set(free_ref.get().with_free(head.vacate()));
        self.free.set(Some(tail.vacate()));
    }

    fn head(&self, tail: parser::Key<parser::Occupied>) -> parser::Key<parser::Occupied> {
        self.node(tail).get().next()
    }

    fn read(&self, key: parser::Key<parser::Occupied>) -> parser::Node {
        self.node(key).get()
    }

    fn write(&self, key: parser::Key<parser::Occupied>, node: parser::Node) {
        self.node(key).set(node);
    }
}

impl Free<'_> {
    fn append(
        self,
        tail: Option<parser::Key<parser::Occupied>>,
        value: parser::Kind,
    ) -> parser::Key<parser::Occupied> {
        let key = self.pool.node(self.tail).get().vacant_next();
        if key.raw() == self.tail.raw() {
            self.pool.free.set(None);
        } else {
            let next = self.pool.node(key).get().vacant_next();
            let free = self.pool.node(self.tail);
            free.set(free.get().with_free(next));
        }
        let key = key.occupy();
        match tail {
            Some(tail) => {
                let head = self.pool.node(tail).get().next();
                self.pool.node(key).set(parser::Node::new(head, value));
                let tail = self.pool.node(tail);
                tail.set(tail.get().with_next(key));
            }
            None => self.pool.node(key).set(parser::Node::new(key, value)),
        }
        key
    }
}

/// A parser-validated byte range packed into one non-zero word.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub(crate) struct Span(num::NonZero<u64>);

impl Span {
    const fn terminated(body: Body) -> Self {
        Self(body.raw())
    }

    const fn from_stored(stored: u64) -> Self {
        Self(num::NonZero::<u64>::MIN.saturating_add(stored))
    }

    const fn stored(self) -> u64 {
        self.0.get() - 1
    }

    fn parts(self) -> (usize, usize) {
        let raw = self.0.get();
        (raw as u32 as usize, (raw >> 32) as u32 as usize)
    }

    fn range(self) -> ops::Range<usize> {
        let (start, end) = self.parts();
        start..end
    }
}

const _: [(); 16] = [(); mem::size_of::<Free<'_>>()];
