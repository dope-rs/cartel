use crate::protocol::parser::{self, frame::Stored, storage};
use o3::buffer::{bytes::scope, resident};
use o3::mem::rc;
use std::{marker, mem};

type Brand<'id> = marker::PhantomData<fn(&'id mut ()) -> &'id mut ()>;

/// A selection from the exact immutable frame paired with its parser tape.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub(crate) struct Selection<'id> {
    pub(super) span: parser::Span,
    brand: Brand<'id>,
}

impl<'id> Selection<'id> {
    pub(super) fn new(span: parser::Span) -> Self {
        Self {
            span,
            brand: marker::PhantomData,
        }
    }
}

pub(crate) struct Owner<'id, 'd> {
    pub(super) owner: scope::Owner<'id, resident::Shared<'d>>,
}

pub(crate) struct Nodes<'id, 's, N = storage::Tape<'s>> {
    pub(super) nodes: N,
    brand: Brand<'id>,
    storage: marker::PhantomData<&'s ()>,
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(crate) struct Cursor<'id, K = storage::Raw> {
    cursor: storage::Cursor<K>,
    brand: Brand<'id>,
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(crate) struct Batch<'id, const N: usize, K = storage::Raw> {
    batch: storage::Batch<N, K>,
    brand: Brand<'id>,
}

pub(crate) struct Read<'id, T, K = storage::Raw> {
    pub(crate) value: T,
    pub(crate) cursor: Cursor<'id, K>,
}

pub(crate) struct Array<'id> {
    array: storage::Array,
    brand: Brand<'id>,
}

impl<'id, K> Cursor<'id, K> {
    fn new(cursor: storage::Cursor<K>) -> Self {
        Self {
            cursor,
            brand: marker::PhantomData,
        }
    }

    pub(crate) fn batch<const N: usize>(self) -> Option<Batch<'id, N, K>> {
        Some(Batch {
            batch: self.cursor.batch()?,
            brand: self.brand,
        })
    }

    pub(crate) fn remaining(&self) -> u32 {
        self.cursor.remaining()
    }
}

impl<'id> Array<'id> {
    pub(crate) fn exact<const N: usize>(self) -> Result<Batch<'id, N>, storage::Length> {
        Ok(Batch {
            batch: self.array.exact()?,
            brand: self.brand,
        })
    }
}

impl<'id, 'd> Owner<'id, 'd> {
    pub(crate) fn slice(&self, selected: Selection<'id>) -> &[u8] {
        self.owner.view().get(selected).0
    }

    pub(crate) fn project(self, selected: Selection<'id>) -> resident::Shared<'d> {
        let range = self.owner.view().get(selected).1;
        self.owner.project(range)
    }

    pub(crate) fn into_inner(self) -> resident::Shared<'d> {
        self.owner.into_inner()
    }

    pub(crate) fn try_clone(&self) -> Result<Self, rc::Error> {
        Ok(Self {
            owner: self.owner.try_clone()?,
        })
    }

    pub(crate) fn store<'s, N>(self, nodes: Nodes<'id, 's, N>) -> Stored<'s, 'd, N> {
        Stored {
            frame: self.owner.into_inner(),
            nodes: nodes.nodes,
            storage: marker::PhantomData,
        }
    }

    pub(crate) fn select<const N: usize>(self, selected: [Selection<'id>; N]) -> Selected<'d, N> {
        Selected {
            frame: self.owner.into_inner(),
            spans: selected.map(|selected| selected.span),
        }
    }
}

pub(crate) struct Selected<'d, const N: usize> {
    frame: resident::Shared<'d>,
    spans: [parser::Span; N],
}

impl<const N: usize> Selected<'_, N> {
    pub(crate) fn values(&self) -> [&[u8]; N] {
        scope::Owner::with(&self.frame, |owner| {
            self.spans
                .map(|span| owner.borrow().get(Selection::new(span)).0)
        })
    }
}

impl<'id, 's, N> Nodes<'id, 's, N> {
    pub(super) fn new(nodes: N) -> Self {
        Self {
            nodes,
            brand: marker::PhantomData,
            storage: marker::PhantomData,
        }
    }
}

impl<'id, 's> Nodes<'id, 's> {
    pub(crate) fn root(&self) -> Batch<'id, 1> {
        Batch {
            batch: self.nodes.root(),
            brand: self.brand,
        }
    }

    pub(crate) fn cursor(&self) -> Cursor<'id> {
        Cursor::new(self.nodes.cursor())
    }

    pub(crate) fn next(
        &self,
        batch: Batch<'id, 1>,
    ) -> Result<Read<'id, parser::Value<Selection<'id>>>, Selection<'id>> {
        let read = self.nodes.take(batch.batch).map_err(Selection::new)?;
        Ok(Read {
            value: read.value.map(Selection::new),
            cursor: Cursor::new(read.cursor),
        })
    }

    pub(crate) fn pair(
        &self,
        batch: Batch<'id, 2>,
    ) -> Read<'id, [Result<parser::Value<Selection<'id>>, Selection<'id>>; 2]> {
        let read = self.nodes.pair(batch.batch);
        Read {
            value: read.value.map(|value| {
                value
                    .map(|value| value.map(Selection::new))
                    .map_err(Selection::new)
            }),
            cursor: Cursor::new(read.cursor),
        }
    }

    pub(crate) fn array(
        &self,
        batch: Batch<'id, 1>,
    ) -> Result<Array<'id>, storage::Mismatch<Selection<'id>>> {
        Ok(Array {
            array: self.nodes.array(batch.batch).map_err(mismatch)?,
            brand: self.brand,
        })
    }

    pub(crate) fn scan(
        &self,
        batch: Batch<'id, 2>,
    ) -> (
        Result<parser::Value<Selection<'id>>, Selection<'id>>,
        Result<Array<'id>, storage::Mismatch<Selection<'id>>>,
    ) {
        let (first, second) = self.nodes.scan(batch.batch);
        (
            first
                .map(|value| value.map(Selection::new))
                .map_err(Selection::new),
            second
                .map(|array| Array {
                    array,
                    brand: self.brand,
                })
                .map_err(mismatch),
        )
    }

    pub(crate) fn region(
        self,
        array: Array<'id>,
    ) -> Nodes<'id, 's, storage::Region<'s, storage::Raw>> {
        Nodes::new(self.nodes.region(array.array))
    }

    pub(crate) fn coordinates<E>(
        self,
        mut number: impl FnMut(parser::Value<Selection<'id>>) -> Result<f64, E>,
    ) -> Result<
        Nodes<'id, 's, storage::Region<'s, storage::Coordinate>>,
        storage::CoordError<E, Selection<'id>>,
    > {
        self.nodes
            .coordinates(|value| number(value.map(Selection::new)))
            .map(Nodes::new)
            .map_err(|error| match error {
                storage::CoordError::Array(error) => storage::CoordError::Array(mismatch(error)),
                storage::CoordError::Member(error) => storage::CoordError::Member(mismatch(error)),
                storage::CoordError::Length { actual } => storage::CoordError::Length { actual },
                storage::CoordError::Number(error) => storage::CoordError::Number(error),
                storage::CoordError::Remaining(left) => storage::CoordError::Remaining(left),
            })
    }
}

fn mismatch<'id>(error: storage::Mismatch) -> storage::Mismatch<Selection<'id>> {
    match error {
        storage::Mismatch::Server(span) => storage::Mismatch::Server(Selection::new(span)),
        storage::Mismatch::Value(value) => storage::Mismatch::Value(value.map(Selection::new)),
    }
}

impl<'id, 's> Nodes<'id, 's, storage::Region<'s, storage::Raw>> {
    pub(crate) fn bulks<const W: usize>(
        self,
    ) -> Result<Nodes<'id, 's, storage::Region<'s, storage::Bulk<W>>>, storage::Group<Selection<'id>>>
    {
        self.nodes
            .bulks()
            .map(Nodes::new)
            .map_err(|error| match error {
                storage::Group::Length { width, actual } => {
                    storage::Group::Length { width, actual }
                }
                storage::Group::Mismatch(error) => storage::Group::Mismatch(mismatch(error)),
            })
    }

    pub(crate) fn optional(
        self,
    ) -> Result<
        Nodes<'id, 's, storage::Region<'s, storage::Maybe>>,
        storage::Mismatch<Selection<'id>>,
    > {
        self.nodes.optional().map(Nodes::new).map_err(mismatch)
    }

    pub(crate) fn scored<E>(
        self,
        mut number: impl FnMut(parser::Value<Selection<'id>>) -> Result<f64, E>,
    ) -> Result<
        Nodes<'id, 's, storage::Region<'s, storage::Scored>>,
        storage::Score<E, Selection<'id>>,
    > {
        self.nodes
            .scored(|value| number(value.map(Selection::new)))
            .map(Nodes::new)
            .map_err(|error| match error {
                storage::Score::Length { width, actual } => {
                    storage::Score::Length { width, actual }
                }
                storage::Score::Server(span) => storage::Score::Server(Selection::new(span)),
                storage::Score::Member(value) => storage::Score::Member(value.map(Selection::new)),
                storage::Score::Number(error) => storage::Score::Number(error),
            })
    }
}

impl<'id, 's, K> Nodes<'id, 's, storage::Region<'s, K>> {
    pub(crate) fn cursor(&self) -> Cursor<'id, K> {
        Cursor::new(self.nodes.cursor())
    }
}

impl<'id, 's, const W: usize> Nodes<'id, 's, storage::Region<'s, storage::Bulk<W>>> {
    pub(crate) fn read<const N: usize>(
        &self,
        batch: Batch<'id, N, storage::Bulk<W>>,
    ) -> Read<'id, [Selection<'id>; N], storage::Bulk<W>> {
        let read = self.nodes.bulks(batch.batch);
        Read {
            value: read.value.map(Selection::new),
            cursor: Cursor::new(read.cursor),
        }
    }
}

impl<'id, 's, const N: usize> Nodes<'id, 's, storage::Exact<'s, N>> {
    pub(crate) fn read(&self) -> [Selection<'id>; N] {
        self.nodes.bulks().map(Selection::new)
    }
}

const _: () = {
    assert!(mem::size_of::<Selection<'static>>() == 8);
    assert!(mem::size_of::<Cursor<'static>>() == 8);
    assert!(mem::size_of::<Batch<'static, 1>>() == 8);
};
