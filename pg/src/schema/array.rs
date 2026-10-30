use o3::alloc::array;
use std::{marker, mem};

use crate::protocol::{Length, Reason, Wire};

pub struct Array<'row, T> {
    bytes: Wire<'row>,
    len: usize,
    marker: marker::PhantomData<T>,
}

impl<T> Copy for Array<'_, T> {}

impl<T> Clone for Array<'_, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'row, T> Array<'row, T>
where
    T: Element<'row>,
{
    pub(crate) const fn new(bytes: Wire<'row>, len: usize) -> Self {
        Self {
            bytes,
            len,
            marker: marker::PhantomData,
        }
    }

    pub const fn len(self) -> usize {
        self.len
    }

    pub const fn is_empty(self) -> bool {
        self.len == 0
    }

    pub fn try_collect(self) -> Result<Vec<T::Owned>, crate::Error> {
        let Self { mut bytes, len, .. } = self;
        if len == 0 {
            finish(bytes)?;
            return Ok(Vec::new());
        }
        let values = array::Block::<mem::MaybeUninit<T::Owned>, usize>::try_new(len, ())
            .map_err(crate::Error::Allocation)?
            .try_init(|_| T::__read(&mut bytes)?.__own())?;
        finish(bytes)?;
        Ok(values.into_vec())
    }
}

pub struct Iter<'row, T> {
    state: State<'row>,
    marker: marker::PhantomData<T>,
}

enum State<'row> {
    Reading { bytes: Wire<'row>, remaining: usize },
    Done,
}

impl<'row, T> Iterator for Iter<'row, T>
where
    T: Element<'row>,
{
    type Item = Result<T, crate::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let State::Reading {
            bytes,
            remaining: left,
        } = &mut self.state
        else {
            return None;
        };
        if *left == 0 {
            let result = finish(*bytes).err().map(Err);
            self.state = State::Done;
            return result;
        }
        *left -= 1;
        match T::__read(bytes) {
            Ok(value) => Some(Ok(value)),
            Err(error) => {
                self.state = State::Done;
                Some(Err(error))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.state {
            State::Reading {
                remaining: left, ..
            } => (0, left.checked_add(1)),
            State::Done => (0, Some(0)),
        }
    }
}

const _: () = assert!(mem::size_of::<Iter<'static, i32>>() == 3 * mem::size_of::<usize>());

impl<'row, T> IntoIterator for Array<'row, T>
where
    T: Element<'row>,
{
    type Item = Result<T, crate::Error>;
    type IntoIter = Iter<'row, T>;

    fn into_iter(self) -> Self::IntoIter {
        Iter {
            state: State::Reading {
                bytes: self.bytes,
                remaining: self.len,
            },
            marker: marker::PhantomData,
        }
    }
}

#[doc(hidden)]
pub trait Element<'row>: Sized {
    type Owned;

    fn __read(bytes: &mut Wire<'row>) -> Result<Self, crate::Error>;

    fn __own(self) -> Result<Self::Owned, crate::Error>;
}

fn finish(bytes: Wire<'_>) -> Result<(), crate::Error> {
    if bytes.is_empty() {
        Ok(())
    } else {
        Err(crate::Error::Protocol(Reason::ArrayTrailing {
            actual: bytes.length(),
        }))
    }
}

fn element<'row>(bytes: &mut Wire<'row>) -> Result<Wire<'row>, crate::Error> {
    let Some((head, tail)) = bytes.split_array::<4>() else {
        return Err(crate::Error::Protocol(Reason::ArrayLength {
            actual: bytes.length(),
        }));
    };
    let len = i32::from_be_bytes(*head);
    *bytes = tail;
    if len == -1 {
        return Err(crate::Error::Protocol(Reason::ArrayNull));
    }
    let Ok(len) = u32::try_from(len) else {
        return Err(crate::Error::Protocol(Reason::NegativeArrayLength {
            actual: len,
        }));
    };
    let Some((head, tail)) = bytes.split_at(len as usize) else {
        return Err(crate::Error::Protocol(Reason::ArrayBytes {
            actual: bytes.length(),
            required: Length::from_u32(len),
        }));
    };
    *bytes = tail;
    Ok(head)
}

impl<'row> Element<'row> for i32 {
    type Owned = Self;

    fn __read(bytes: &mut Wire<'row>) -> Result<Self, crate::Error> {
        let bytes = element(bytes)?;
        let Ok(value) = bytes.as_slice().try_into() else {
            return Err(crate::Error::Protocol(Reason::ArrayI32Width {
                actual: bytes.length(),
            }));
        };
        Ok(Self::from_be_bytes(value))
    }

    fn __own(self) -> Result<Self::Owned, crate::Error> {
        Ok(self)
    }
}

impl<'row> Element<'row> for i64 {
    type Owned = Self;

    fn __read(bytes: &mut Wire<'row>) -> Result<Self, crate::Error> {
        let bytes = element(bytes)?;
        let Ok(value) = bytes.as_slice().try_into() else {
            return Err(crate::Error::Protocol(Reason::ArrayI64Width {
                actual: bytes.length(),
            }));
        };
        Ok(Self::from_be_bytes(value))
    }

    fn __own(self) -> Result<Self::Owned, crate::Error> {
        Ok(self)
    }
}

impl<'row> Element<'row> for &'row str {
    type Owned = String;

    fn __read(bytes: &mut Wire<'row>) -> Result<Self, crate::Error> {
        match element(bytes)?.try_text() {
            Ok(value) => Ok(value.as_str()),
            Err(error) => Err(crate::Error::Protocol(Reason::ArrayTextUtf8 {
                valid: error.valid,
                error: error.error,
            })),
        }
    }

    fn __own(self) -> Result<Self::Owned, crate::Error> {
        crate::Error::try_string(self)
    }
}
