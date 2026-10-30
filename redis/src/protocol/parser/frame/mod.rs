use crate::protocol;
use crate::protocol::parser::{self, storage};
use crate::value::source;
use cartel_core::reply::usage;
use o3::buffer::{self, bytes::scope, hold, hold::scope::Deferred, resident};
use o3::mem::Rejected;
use std::{marker, num};

mod raw;
mod scopes;
pub(crate) use scopes::{Batch, Cursor, Nodes, Owner, Read, Selected, Selection};

/// A completed RESP frame that still borrows the receive cursor. Its parser
/// tape is already in final pool-backed storage and transfers without copying.
#[doc(hidden)]
pub struct Parsed<'input, 'd, 'storage, 'g, R: buffer::Cursor> {
    pub(super) frame: Deferred<'input, 'd, R>,
    pub(super) nodes: storage::Tape<'storage>,
    pub(super) lane: storage::Lane<'storage, 'g>,
}

impl<'input, 'd, 'storage, 'g, R: buffer::Cursor> Parsed<'input, 'd, 'storage, 'g, R> {
    pub(crate) fn as_slice(&self) -> &'input [u8] {
        self.frame.as_slice()
    }

    pub(crate) fn into_stored(
        self,
        policy: hold::Policy,
    ) -> Result<(Stored<'storage, 'd>, storage::Lane<'storage, 'g>), (Self, hold::Error)>
    where
        R: hold::raw::Retain<'d>,
    {
        let Self { frame, nodes, lane } = self;
        match frame.bind(policy).try_retain() {
            Ok(frame) => Ok((
                Stored {
                    frame,
                    nodes,
                    storage: marker::PhantomData,
                },
                lane,
            )),
            Err((frame, error)) => Err((
                Self {
                    frame: frame.defer(),
                    nodes,
                    lane,
                },
                error,
            )),
        }
    }
}

pub struct Stored<'a, 'd, N = storage::Tape<'a>> {
    frame: resident::Shared<'d>,
    nodes: N,
    storage: marker::PhantomData<&'a ()>,
}

impl<'a, 'd, N> Stored<'a, 'd, N> {
    pub fn as_slice(&self) -> &[u8] {
        self.frame.as_slice()
    }

    pub(crate) fn with<T>(
        self,
        run: impl for<'id> FnOnce(Owner<'id, 'd>, Nodes<'id, 'a, N>) -> T,
    ) -> T {
        scope::Owner::with(self.frame, |owner| {
            run(Owner { owner }, Nodes::new(self.nodes))
        })
    }
}

impl<'s, 'd, K> Stored<'s, 'd, storage::Region<'s, K>> {
    pub(crate) fn len(&self) -> usize {
        self.nodes.len()
    }

    pub(crate) fn iter(&self) -> Iter<'_, 's, 'd, K> {
        Iter {
            source: self,
            cursor: self.nodes.cursor(),
        }
    }
}

impl<'s, 'd> Stored<'s, 'd, storage::Region<'s, storage::Bulk<1>>> {
    pub(crate) fn try_chunks<const N: usize>(
        self,
    ) -> Result<
        Stored<'s, 'd, storage::Region<'s, storage::Bulk<N>>>,
        Rejected<Self, storage::Length>,
    > {
        match self.nodes.try_chunks() {
            Ok(nodes) => Ok(Stored {
                frame: self.frame,
                nodes,
                storage: self.storage,
            }),
            Err(rejected) => Err(rejected.map(|nodes| Stored {
                frame: self.frame,
                nodes,
                storage: self.storage,
            })),
        }
    }

    pub(crate) fn try_exact<const N: usize>(
        self,
    ) -> Result<Stored<'s, 'd, storage::Exact<'s, N>>, Rejected<Self, storage::Length>> {
        match self.nodes.try_exact() {
            Ok(nodes) => Ok(Stored {
                frame: self.frame,
                nodes,
                storage: self.storage,
            }),
            Err(rejected) => Err(rejected.map(|nodes| Stored {
                frame: self.frame,
                nodes,
                storage: self.storage,
            })),
        }
    }
}

impl<'s, 'd, const W: usize> Stored<'s, 'd, storage::Region<'s, storage::Bulk<W>>> {
    pub(crate) fn into_bulks(self) -> Stored<'s, 'd, storage::Region<'s, storage::Bulk<1>>> {
        Stored {
            frame: self.frame,
            nodes: self.nodes.into_bulks(),
            storage: self.storage,
        }
    }
}

impl<'s, 'd, const N: usize> Stored<'s, 'd, storage::Exact<'s, N>> {
    pub(crate) fn array(&self) -> [&[u8]; N] {
        scope::Owner::with(&self.frame, |owner| {
            self.nodes
                .bulks()
                .map(|span| owner.borrow().get(Selection::new(span)).0)
        })
    }

    pub(crate) fn into_bulks(self) -> Stored<'s, 'd, storage::Region<'s, storage::Bulk<1>>> {
        Stored {
            frame: self.frame,
            nodes: self.nodes.into_bulks(),
            storage: self.storage,
        }
    }
}

pub(crate) struct Iter<'a, 's, 'd, K> {
    source: &'a Stored<'s, 'd, storage::Region<'s, K>>,
    cursor: storage::Cursor<K>,
}

impl<K> Iter<'_, '_, '_, K> {
    pub(crate) fn remaining(&self) -> u32 {
        self.cursor.remaining()
    }
}

impl<'a, const W: usize> Iterator for Iter<'a, '_, '_, storage::Bulk<W>> {
    type Item = [&'a [u8]; W];

    fn next(&mut self) -> Option<Self::Item> {
        let read = self.source.nodes.bulks(self.cursor.batch::<W>()?);
        self.cursor = read.cursor;
        Some(scope::Owner::with(&self.source.frame, |owner| {
            read.value
                .map(|span| owner.borrow().get(Selection::new(span)).0)
        }))
    }
}

impl<'a> Iterator for Iter<'a, '_, '_, storage::Scored> {
    type Item = (&'a [u8], f64);

    fn next(&mut self) -> Option<Self::Item> {
        let read = self.source.nodes.scored(self.cursor.batch::<2>()?);
        self.cursor = read.cursor;
        Some(scope::Owner::with(&self.source.frame, |owner| {
            (
                owner.borrow().get(Selection::new(read.value.0)).0,
                read.value.1,
            )
        }))
    }
}

impl Iterator for Iter<'_, '_, '_, storage::Coordinate> {
    type Item = Option<[f64; 2]>;

    fn next(&mut self) -> Option<Self::Item> {
        let read = self.source.nodes.coordinate(self.cursor)?;
        self.cursor = read.cursor;
        Some(read.value)
    }
}

impl Stored<'_, '_, storage::Region<'_, storage::Maybe>> {
    pub(crate) fn try_array<const N: usize>(&self) -> Result<[Option<&[u8]>; N], storage::Length> {
        let batch = self
            .nodes
            .cursor()
            .batch_exact()
            .map_err(|actual| storage::Length { width: N, actual })?;
        let read = self.nodes.optionals(batch);
        Ok(scope::Owner::with(&self.frame, |owner| {
            read.value
                .map(|span| span.map(|span| owner.borrow().get(Selection::new(span)).0))
        }))
    }
}

impl<'a, 'd> Stored<'a, 'd> {
    pub fn value_count(&self) -> num::NonZero<u32> {
        self.nodes.len()
    }

    pub fn reader(&self) -> parser::Reader<'_, 'a> {
        Reader {
            frame: self.frame.as_slice(),
            nodes: &self.nodes,
            cursor: self.nodes.cursor(),
        }
    }
}

/// An allocation-free cursor over one retained RESP frame and parser tape.
pub struct Reader<'a, 'b> {
    frame: &'a [u8],
    nodes: &'a storage::Tape<'b>,
    cursor: storage::Cursor,
}

impl source::Sealed for Reader<'_, '_> {}

impl<'a> source::Reader<'a> for Reader<'a, '_> {
    fn next_event(&mut self) -> Option<crate::value::Event<'a>> {
        let read = self.nodes.event(self.cursor)?;
        self.cursor = read.cursor;
        Some(scope::Owner::with(self.frame, |owner| {
            let view = owner.borrow();
            match read.value {
                parser::Kind::Nil => crate::value::Event::Nil,
                parser::Kind::Ok => crate::value::Event::Ok,
                parser::Kind::Status(span) => {
                    crate::value::Event::Status(view.get(Selection::new(span)).0)
                }
                parser::Kind::Error(span) => {
                    crate::value::Event::Error(view.get(Selection::new(span)).0)
                }
                parser::Kind::Integer(value) => crate::value::Event::Integer(value),
                parser::Kind::Bulk(span) => {
                    crate::value::Event::Bulk(view.get(Selection::new(span)).0)
                }
                parser::Kind::Array { len } => crate::value::Event::Array { len: len as usize },
            }
        }))
    }

    fn remaining(&self) -> usize {
        self.cursor.remaining() as usize
    }

    fn finish(self) -> Result<(), protocol::Error> {
        match num::NonZero::<u32>::new(self.cursor.remaining()) {
            None => Ok(()),
            Some(left) => Err(protocol::Error::Values { remaining: left }),
        }
    }
}

impl usage::Footprint for Stored<'_, '_> {
    fn footprint(&self) -> usage::Usage {
        usage::Usage::new(
            usage::Bytes::new(self.frame.len()),
            usage::Values::new(self.nodes.len().get() as usize),
        )
    }
}

const _: () =
    assert!(std::mem::size_of::<Stored<'static, 'static>>() == 6 * std::mem::size_of::<usize>());
const _: [(); 4 * std::mem::size_of::<usize>()] = [(); std::mem::size_of::<Reader<'_, '_>>()];
