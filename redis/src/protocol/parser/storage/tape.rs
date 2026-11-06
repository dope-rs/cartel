use std::{marker, mem, num};

use crate::protocol::parser;
use crate::protocol::parser::storage::{
    Array, Batch, Bulk, CoordError, Coordinate, Cursor, Mismatch, Raw, Read, Region,
};

/// One retained parser tape branded by its port-owned storage.
#[doc(hidden)]
pub struct Tape<'a> {
    pub(super) pool: &'a parser::Pool,
    pub(super) tail: parser::Key<parser::Occupied>,
    pub(super) len: num::NonZero<u32>,
}

impl<'a> Tape<'a> {
    pub(in crate::protocol::parser) const fn len(&self) -> num::NonZero<u32> {
        self.len
    }

    pub(in crate::protocol::parser) fn cursor(&self) -> Cursor {
        Cursor {
            next: self.pool.head(self.tail),
            remaining: self.len.get(),
            state: marker::PhantomData,
        }
    }

    pub(in crate::protocol::parser) fn root(&self) -> Batch<1> {
        Batch {
            next: self.pool.head(self.tail),
            left: self.len,
            state: marker::PhantomData,
        }
    }

    pub(in crate::protocol::parser) fn event(&self, cursor: Cursor) -> Option<Read<parser::Kind>> {
        let batch = cursor.batch::<1>()?;
        let node = self.pool.read(batch.next);
        Some(Read {
            value: node.kind(),
            cursor: batch.finish(node.next()),
        })
    }

    pub(in crate::protocol::parser) fn take(
        &self,
        batch: Batch<1>,
    ) -> Result<Read<parser::Value>, parser::Span> {
        let node = self.pool.read(batch.next);
        let value = node.try_value()?;
        Ok(Read {
            value,
            cursor: batch.finish(node.next()),
        })
    }

    pub(in crate::protocol::parser) fn pair(
        &self,
        batch: Batch<2>,
    ) -> Read<[Result<parser::Value, parser::Span>; 2]> {
        let first = self.pool.read(batch.next);
        let second = self.pool.read(first.next());
        Read {
            value: [first.try_value(), second.try_value()],
            cursor: batch.finish(second.next()),
        }
    }

    pub(in crate::protocol::parser) fn array(&self, batch: Batch<1>) -> Result<Array, Mismatch> {
        let read = self.take(batch).map_err(Mismatch::Server)?;
        array(read.value, read.cursor)
    }

    pub(in crate::protocol::parser) fn scan(
        &self,
        batch: Batch<2>,
    ) -> (Result<parser::Value, parser::Span>, Result<Array, Mismatch>) {
        let read = self.pair(batch);
        let [first, second] = read.value;
        (
            first,
            second
                .map_err(Mismatch::Server)
                .and_then(|value| array(value, read.cursor)),
        )
    }

    pub(super) fn bulks<const N: usize, const W: usize>(
        &self,
        batch: Batch<N, Bulk<W>>,
    ) -> Read<[parser::Span; N], Bulk<W>> {
        let mut next = batch.next;
        let value = [(); N].map(|()| {
            let node = self.pool.read(next);
            next = node.next();
            parser::Span::from_stored(node.payload)
        });
        Read {
            value,
            cursor: batch.finish(next),
        }
    }

    pub(in crate::protocol::parser) fn region(self, array: Array) -> Region<'a, Raw> {
        let Array { cursor: start, len } = array;
        Region {
            tape: self,
            start: Cursor {
                remaining: len,
                ..start
            },
            len,
        }
    }

    /// Validates each optional pair once and stores its numbers in the same tape.
    pub(in crate::protocol::parser) fn coordinates<E>(
        self,
        mut number: impl FnMut(parser::Value) -> Result<f64, E>,
    ) -> Result<Region<'a, Coordinate>, CoordError<E>> {
        let outer = self.array(self.root()).map_err(CoordError::Array)?;
        let len = outer.len;
        let mut cursor = outer.cursor;
        for _ in 0..len {
            let member = self
                .take(cursor.children::<1>())
                .map_err(Mismatch::Server)
                .map_err(CoordError::Member)?;
            cursor = member.cursor;
            match member.value {
                parser::Value::Nil => continue,
                parser::Value::Array { len: 2 } => {}
                parser::Value::Array { len } => {
                    return Err(CoordError::Length {
                        actual: len as usize,
                    });
                }
                actual => return Err(CoordError::Member(Mismatch::Value(actual))),
            }
            let batch = cursor.children::<2>();
            let mut next = batch.next;
            for _ in 0..2 {
                let node = self.pool.read(next);
                let value = node
                    .try_value()
                    .map_err(|span| CoordError::Member(Mismatch::Server(span)))?;
                let value = number(value).map_err(CoordError::Number)?;
                self.pool.write(next, node.with_float(value));
                next = node.next();
            }
            cursor = batch.finish(next);
        }
        Ok(Region {
            tape: self,
            start: Cursor {
                next: outer.cursor.next,
                remaining: len,
                state: marker::PhantomData,
            },
            len,
        })
    }
}

fn array(value: parser::Value, cursor: Cursor) -> Result<Array, Mismatch> {
    let len = match value {
        parser::Value::Array { len } => len,
        parser::Value::Nil => 0,
        actual => return Err(Mismatch::Value(actual)),
    };
    Ok(Array { cursor, len })
}

impl Drop for Tape<'_> {
    fn drop(&mut self) {
        self.pool.release(Some(self.tail));
    }
}

const _: [(); 16] = [(); mem::size_of::<Tape<'_>>()];
