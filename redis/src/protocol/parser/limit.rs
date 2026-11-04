use crate::protocol;
use o3::depot::capacity::Limit;
use std::{marker, mem, num};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Frame;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Value;

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub struct Bound<K> {
    value: Limit<{ u32::MAX as usize }>,
    brand: marker::PhantomData<K>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(C)]
pub struct Exceeded<K> {
    low: u64,
    capacity: num::NonZero<u32>,
    overflow: bool,
    brand: marker::PhantomData<K>,
}

#[derive(Clone, Copy)]
pub(crate) struct End(num::NonZero<u32>);

#[derive(Clone, Copy)]
pub(crate) struct Body(num::NonZero<u64>);

#[derive(Clone, Copy)]
pub(crate) struct Line<'a> {
    value: &'a [u8],
    start: End,
}

#[derive(Clone, Copy)]
pub(crate) struct Count(Option<num::NonZero<u32>>);

pub(crate) struct Slot(num::NonZero<u32>);

pub(crate) enum Invalid {
    Zero,
    Above { actual: usize, maximum: usize },
}

impl<K> Bound<K> {
    pub(crate) fn try_new(value: usize) -> Result<Self, Invalid> {
        let Some(value) = Limit::new(value) else {
            return Err(if value == 0 {
                Invalid::Zero
            } else {
                Invalid::Above {
                    actual: value,
                    maximum: u32::MAX as usize,
                }
            });
        };
        Ok(Self {
            value,
            brand: marker::PhantomData,
        })
    }

    pub const fn get(self) -> usize {
        self.value.get()
    }

    pub(super) const fn consumed(self) -> num::NonZero<usize> {
        num::NonZero::<usize>::MIN.saturating_add(self.value.get() - 1)
    }

    fn exceeded(self, low: u64, overflow: bool) -> Exceeded<K> {
        Exceeded {
            low,
            capacity: self.value.count(),
            overflow,
            brand: marker::PhantomData,
        }
    }

    /// Admits a count together with the caller's nonzero framing overhead.
    fn count<T>(self, len: u64, added: u64, map: impl FnOnce(u32) -> T) -> Result<T, Exceeded<K>> {
        let capacity = self.value.get() as u64;
        if len > capacity {
            let (low, overflow) = len.overflowing_add(added - 1);
            return Err(self.exceeded(low, overflow));
        }
        let low = len + (added - 1);
        if low >= capacity {
            return Err(self.exceeded(low, false));
        }
        Ok(map(len as u32))
    }
}

impl<K> Exceeded<K> {
    pub const fn required(self) -> num::NonZero<u128> {
        let required = self.low as u128 | ((self.overflow as u128) << 64);
        num::NonZero::<u128>::MIN.saturating_add(required)
    }

    pub const fn capacity(self) -> num::NonZero<u32> {
        self.capacity
    }
}

impl Bound<Frame> {
    pub(super) fn check(self, required: usize) -> Result<(), Exceeded<Frame>> {
        if required <= self.value.get() {
            return Ok(());
        }
        Err(self.exceeded(required as u64 - 1, false))
    }

    pub(super) fn next(self, used: usize) -> Result<End, Exceeded<Frame>> {
        if used >= self.value.get() {
            return Err(self.exceeded(used as u64, false));
        }
        Ok(End(num::NonZero::<u32>::MIN.saturating_add(used as u32)))
    }

    pub(super) fn line(self, line: Line<'_>) -> Result<(End, Body), Exceeded<Frame>> {
        let last = line.end() - 1;
        if last >= self.value.get() {
            return Err(self.exceeded(last as u64, false));
        }
        let end = End(num::NonZero::<u32>::MIN.saturating_add(last as u32));
        let body = Body::new(line.start, line.stop() as u32);
        Ok((end, body))
    }

    #[inline]
    pub(super) fn body(self, start: End, length: u64) -> Result<Body, Exceeded<Frame>> {
        self.count(length, start.get() as u64 + 2, move |length| {
            Body::new(start, start.count().get() + length)
        })
    }
}

impl Bound<Value> {
    pub(super) fn next(self, used: u32) -> Result<Slot, Exceeded<Value>> {
        let required = u64::from(used) + 1;
        if required > self.value.get() as u64 {
            return Err(self.exceeded(required - 1, false));
        }
        let slot = num::NonZero::<u32>::MIN.saturating_add(used);
        Ok(Slot(slot))
    }

    #[inline]
    pub(super) fn array(self, used: u32, declared: u64) -> Result<Count, Exceeded<Value>> {
        self.count(declared, u64::from(used) + 1, |declared| {
            Count(num::NonZero::<u32>::new(declared))
        })
    }
}

impl End {
    pub(super) const fn count(self) -> num::NonZero<u32> {
        self.0
    }

    pub(super) const fn get(self) -> usize {
        self.0.get() as usize
    }

    pub(super) fn consumed(self) -> num::NonZero<usize> {
        num::NonZero::<usize>::MIN.saturating_add(self.0.get() as usize - 1)
    }
}

impl Body {
    fn new(start: End, stop: u32) -> Self {
        let start = num::NonZero::<u64>::from(start.0);
        Self(start | (u64::from(stop) << 32))
    }

    pub(super) const fn start(self) -> u32 {
        self.0.get() as u32
    }

    pub(super) const fn get(self) -> usize {
        self.stop() as usize + 2
    }

    /// Both constructors admit the trailing CRLF within the u32 frame bound.
    pub(super) const fn end(self) -> End {
        End(num::NonZero::<u32>::MIN.saturating_add(self.stop() + 1))
    }

    pub(super) const fn stop(self) -> u32 {
        (self.0.get() >> 32) as u32
    }

    pub(super) fn consumed(self) -> num::NonZero<usize> {
        self.end().consumed()
    }

    pub(super) const fn raw(self) -> num::NonZero<u64> {
        self.0
    }
}

impl<'a> Line<'a> {
    pub(super) fn find(
        bytes: &'a [u8],
        start: End,
        scan: usize,
        kind: protocol::Line,
    ) -> Result<Option<Self>, (usize, u8, u8)> {
        let begin = start.get();
        let scan = scan.max(begin);
        let Some(tail) = bytes.get(scan..) else {
            return Ok(None);
        };
        let offset = match kind {
            protocol::Line::Status | protocol::Line::Server => {
                let Some(offset) = tail.iter().position(|byte| matches!(byte, b'\r' | b'\n'))
                else {
                    return Ok(None);
                };
                if tail[offset] == b'\n' {
                    return Err((scan + offset, b'\r', b'\n'));
                }
                let Some(&actual) = tail.get(offset + 1) else {
                    return Ok(None);
                };
                if actual != b'\n' {
                    return Err((scan + offset + 1, b'\n', actual));
                }
                offset
            }
            protocol::Line::Integer | protocol::Line::Bulk | protocol::Line::Array => {
                let Some(offset) = tail.windows(2).position(|pair| pair == b"\r\n") else {
                    return Ok(None);
                };
                offset
            }
        };
        Ok(bytes
            .get(begin..scan + offset)
            .map(|value| Self { value, start }))
    }

    pub(super) const fn value(self) -> &'a [u8] {
        self.value
    }

    pub(super) const fn end(self) -> usize {
        self.stop() + 2
    }

    const fn stop(self) -> usize {
        self.start.get() + self.value.len()
    }
}

impl Count {
    pub(super) const fn get(self) -> u32 {
        match self.0 {
            Some(value) => value.get(),
            None => 0,
        }
    }

    pub(super) const fn nonzero(self) -> Option<num::NonZero<u32>> {
        self.0
    }
}

impl Slot {
    pub(super) fn commit<T>(self, value: T) -> (T, num::NonZero<u32>) {
        (value, self.0)
    }
}

pub(super) const DEPTH: num::NonZero<u8> = {
    assert!(super::MAX_DEPTH != 0 && super::MAX_DEPTH < u8::MAX as usize);
    num::NonZero::<u8>::MIN.saturating_add(super::MAX_DEPTH as u8 - 1)
};

pub(super) const OVER_DEPTH: num::NonZero<u8> = DEPTH.saturating_add(1);

const _: [(); 4] = [(); mem::size_of::<Bound<Frame>>()];
const _: [(); 4] = [(); mem::size_of::<Bound<Value>>()];
const _: [(); 4] = [(); mem::size_of::<End>()];
const _: [(); 8] = [(); mem::size_of::<Body>()];
const _: [(); 24] = [(); mem::size_of::<Line<'_>>()];
const _: [(); 4] = [(); mem::size_of::<Count>()];
const _: [(); 4] = [(); mem::size_of::<Slot>()];
const _: [(); 16] = [(); mem::size_of::<Exceeded<Frame>>()];
const _: [(); 16] = [(); mem::size_of::<Exceeded<Value>>()];
