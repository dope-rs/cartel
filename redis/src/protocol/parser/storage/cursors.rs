use crate::protocol::parser::{self, storage::Length, storage::Raw};
use std::{marker, mem, num};

#[derive(Clone, Copy)]
pub(in crate::protocol::parser) struct Cursor<K = Raw> {
    pub(super) next: parser::Key<parser::Occupied>,
    pub(super) remaining: u32,
    pub(super) state: marker::PhantomData<K>,
}

#[derive(Clone, Copy)]
pub(in crate::protocol::parser) struct Batch<const N: usize, K = Raw> {
    pub(super) next: parser::Key<parser::Occupied>,
    pub(super) left: num::NonZero<u32>,
    pub(super) state: marker::PhantomData<K>,
}

pub(in crate::protocol::parser) struct Read<T, K = Raw> {
    pub(in crate::protocol::parser) value: T,
    pub(in crate::protocol::parser) cursor: Cursor<K>,
}

/// The children of one array header in a completed, retained parser tape.
pub(in crate::protocol::parser) struct Array {
    pub(super) cursor: Cursor,
    pub(super) len: u32,
}

impl Array {
    pub(in crate::protocol::parser) fn exact<const N: usize>(self) -> Result<Batch<N>, Length> {
        if self.len as usize != N {
            return Err(Length {
                width: N,
                actual: self.len as usize,
            });
        }
        Ok(self.cursor.children())
    }
}

impl<K> Cursor<K> {
    /// The completed parent shape proves that this many following nodes exist.
    pub(super) fn children<const N: usize>(self) -> Batch<N, K> {
        let () = Batch::<N, K>::VALID;
        Batch {
            next: self.next,
            left: num::NonZero::<u32>::MIN.saturating_add(self.remaining - N as u32),
            state: marker::PhantomData,
        }
    }

    pub(in crate::protocol::parser) fn batch<const N: usize>(self) -> Option<Batch<N, K>> {
        let () = Batch::<N, K>::VALID;
        (self.remaining >= N as u32).then(|| self.children())
    }

    pub(in crate::protocol::parser) fn batch_exact<const N: usize>(
        self,
    ) -> Result<Batch<N, K>, usize> {
        let () = Batch::<N, K>::VALID;
        let actual = self.remaining as usize;
        if actual != N {
            return Err(actual);
        }
        Ok(self.children())
    }

    pub(in crate::protocol::parser) fn remaining(&self) -> u32 {
        self.remaining
    }

    pub(super) fn cast<L>(self) -> Cursor<L> {
        Cursor {
            next: self.next,
            remaining: self.remaining,
            state: marker::PhantomData,
        }
    }
}

impl<const N: usize, K> Batch<N, K> {
    pub(super) const VALID: () = assert!(N != 0 && N <= u32::MAX as usize);

    pub(super) fn finish(self, next: parser::Key<parser::Occupied>) -> Cursor<K> {
        Cursor {
            next,
            remaining: self.left.get() - 1,
            state: marker::PhantomData,
        }
    }
}

const _: [(); 8] = [(); mem::size_of::<Cursor>()];
const _: [(); 8] = [(); mem::size_of::<Batch<1>>()];
const _: [(); 8] = [(); mem::size_of::<Option<Batch<1>>>()];
const _: [(); 8] = [(); mem::size_of::<Read<()>>()];
