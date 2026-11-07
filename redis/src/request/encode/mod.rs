use std::num;

use cartel_core::frame::{Buffer, Sensitivity};
use dope::net::link::egress;
use o3::buffer::pool;

use crate::request;

pub struct Encoder {
    frame: pool::Span,
}

pub struct Complete(egress::NonEmpty<Buffer>);

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Length(num::NonZero<usize>);

pub struct Writer {
    out: pool::Limit<pool::Span, Sensitivity>,
}

#[derive(Clone, Copy)]
struct Number(u64);

pub(crate) trait Arguments {
    const WIDTH: num::NonZero<usize>;

    fn values(&self) -> impl Iterator<Item = &[u8]>;
}

impl<K: AsRef<[u8]>> Arguments for [K; 1] {
    const WIDTH: num::NonZero<usize> = num::NonZero::<usize>::MIN;

    fn values(&self) -> impl Iterator<Item = &[u8]> {
        [self[0].as_ref()].into_iter()
    }
}

impl<K: AsRef<[u8]>, V: AsRef<[u8]>> Arguments for (K, V) {
    const WIDTH: num::NonZero<usize> = num::NonZero::<usize>::MIN.saturating_add(1);

    fn values(&self) -> impl Iterator<Item = &[u8]> {
        [self.0.as_ref(), self.1.as_ref()].into_iter()
    }
}

impl Number {
    fn new(value: usize) -> Result<Self, request::Error> {
        if value > i64::MAX as usize {
            return Err(request::Error::Length {
                actual: value,
                maximum: i64::MAX as usize,
            });
        }
        Ok(Self(value as u64))
    }

    #[inline]
    fn digits(self) -> usize {
        self.0.checked_ilog10().unwrap_or(0) as usize + 1
    }
}

impl Encoder {
    pub(crate) const fn new(frame: pool::Span) -> Self {
        Self { frame }
    }

    pub(crate) fn command<const N: usize>(
        self,
        args: &[&[u8]; N],
    ) -> Result<Complete, request::Error> {
        self.encode(args, [], Sensitivity::Plain)
    }

    pub(crate) fn secret<'a, const N: usize>(
        self,
        prefix: &[&[u8]; N],
        tail: impl AsRef<[&'a [u8]]>,
    ) -> Result<Complete, request::Error> {
        self.encode(prefix, tail, Sensitivity::Secret)
    }

    pub(crate) fn slices<'a, const N: usize>(
        self,
        prefix: &[&[u8]; N],
        tail: impl AsRef<[&'a [u8]]>,
    ) -> Result<Complete, request::Error> {
        self.encode(prefix, tail, Sensitivity::Plain)
    }

    pub(crate) fn range<const N: usize>(
        self,
        verb: &[u8],
        suffix: &[&[u8]; N],
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Result<Complete, request::Error> {
        let mut head = core::fmt::NumBuffer::new();
        let start = start.format_into(&mut head).as_bytes();
        let mut tail = core::fmt::NumBuffer::new();
        let stop = stop.format_into(&mut tail).as_bytes();
        self.slices(&[verb, key, start, stop], suffix)
    }

    fn encode<'a, const N: usize>(
        self,
        prefix: &[&[u8]; N],
        tail: impl AsRef<[&'a [u8]]>,
        kind: Sensitivity,
    ) -> Result<Complete, request::Error> {
        let tail = tail.as_ref();
        let count = add_count(N, tail.len())?;
        let mut length = Length::array(count)?;
        for argument in prefix.iter().chain(tail) {
            length = length.bulk(argument.len())?;
        }
        self.admit_with(length, kind, |out| {
            out.array(count)?;
            for argument in prefix.iter().chain(tail) {
                out.bulk(*argument)?;
            }
            Ok(())
        })
    }

    pub(crate) fn sequence<I, const P: usize>(
        self,
        prefix: &[&[u8]; P],
        declared: usize,
        items: I,
    ) -> Result<Complete, request::Error>
    where
        I: Iterator + Clone,
        I::Item: Arguments,
    {
        let count = add_group_count(P, declared, I::Item::WIDTH)?;
        let mut length = Length::array(count)?;
        for value in prefix {
            length = length.bulk(value.len())?;
        }
        let mut actual = 0;
        for item in items.clone() {
            actual = next_item(declared, actual)?;
            for value in item.values() {
                length = length.bulk(value.len())?;
            }
        }
        check_items(declared, actual)?;
        self.admit_with(length, Sensitivity::Plain, |out| {
            out.array(count)?;
            for value in prefix {
                out.bulk(*value)?;
            }
            let mut actual = 0;
            for item in items {
                actual = next_item(declared, actual)?;
                for value in item.values() {
                    out.bulk(value)?;
                }
            }
            check_items(declared, actual)
        })
    }

    pub(crate) fn admit_with(
        self,
        length: Length,
        kind: Sensitivity,
        encode: impl FnOnce(&mut Writer) -> Result<(), request::Error>,
    ) -> Result<Complete, request::Error> {
        let out = pool::Limit::try_from(self.frame, length.get(), kind)
            .map_err(|(_, _, cause)| request::Error::Capacity(cause))?;
        Complete::encode(out, encode)
    }
}

impl Complete {
    pub(crate) fn encode(
        out: pool::Limit<pool::Span, Sensitivity>,
        encode: impl FnOnce(&mut Writer) -> Result<(), request::Error>,
    ) -> Result<Self, request::Error> {
        let mut out = Writer { out };
        let result = encode(&mut out);
        let remaining = out.out.remaining();
        let (span, kind) = out.out.into_parts();
        let frame = Buffer::new(span, kind);
        result?;
        if let Some(remaining) = num::NonZero::new(remaining) {
            return Err(request::Error::Incomplete { remaining });
        }
        egress::NonEmpty::try_new(frame)
            .map(Self)
            .map_err(request::Error::Egress)
    }

    pub(crate) fn into_frame(self) -> egress::NonEmpty<Buffer> {
        self.0
    }
}

impl Length {
    #[inline]
    fn array(count: usize) -> Result<Self, request::Error> {
        let count = Number::new(count)?;
        Ok(Self(
            num::NonZero::<usize>::MIN.saturating_add(2 + count.digits()),
        ))
    }

    #[inline(always)]
    fn bulk(self, len: usize) -> Result<Self, request::Error> {
        let length = Number::new(len)?;
        self.add(5)?.add(length.digits())?.add(len)
    }

    fn add(self, amount: usize) -> Result<Self, request::Error> {
        self.0
            .checked_add(amount)
            .map(Self)
            .ok_or(request::Error::Add {
                left: self.get(),
                right: amount,
            })
    }

    pub(crate) const fn get(self) -> usize {
        self.0.get()
    }

    pub(crate) const fn nonzero(self) -> num::NonZero<usize> {
        self.0
    }
}

impl Writer {
    #[inline]
    fn extend(&mut self, bytes: &[u8]) -> Result<(), request::Error> {
        self.out.try_extend(bytes).map_err(request::Error::Write)
    }

    fn bulk(&mut self, payload: &(impl request::Bulk + ?Sized)) -> Result<(), request::Error> {
        let declared = payload.len();
        let mut header = [0; 23];
        self.extend(format_header_into(&mut header, b'$', declared as u64))?;
        let mut written = Ok(0usize);
        payload.visit(|chunk| {
            let Ok(current) = written else {
                return;
            };
            let Some(actual) = current.checked_add(chunk.len()) else {
                written = Err(request::Error::Add {
                    left: current,
                    right: chunk.len(),
                });
                return;
            };
            if actual > declared {
                written = Err(request::Error::BulkLength { declared, actual });
                return;
            }
            written = self.extend(chunk).and(Ok(actual));
        });
        let actual = written?;
        if actual != declared {
            return Err(request::Error::BulkLength { declared, actual });
        }
        self.extend(b"\r\n")
    }

    #[inline]
    fn array(&mut self, count: usize) -> Result<(), request::Error> {
        let mut header = [0; 23];
        self.extend(format_header_into(&mut header, b'*', count as u64))
    }
}

#[inline]
fn format_header_into(buffer: &mut [u8; 23], marker: u8, value: u64) -> &[u8] {
    let [_, digits @ .., cr, lf] = buffer;
    *cr = b'\r';
    *lf = b'\n';
    let start = cartel_core::write_digits(digits, value);
    buffer[start] = marker;
    &buffer[start..]
}

pub(crate) fn format_float(buffer: &mut ryu::Buffer, value: f64) -> &[u8] {
    if value == 0.0 {
        return b"0";
    }
    if value.is_nan() {
        return b"nan";
    }
    if value.is_infinite() {
        return if value > 0.0 { b"inf" } else { b"-inf" };
    }
    buffer.format_finite(value).as_bytes()
}

fn add_count(base: usize, len: usize) -> Result<usize, request::Error> {
    base.checked_add(len).ok_or(request::Error::Add {
        left: base,
        right: len,
    })
}

fn add_group_count(
    base: usize,
    len: usize,
    width: num::NonZero<usize>,
) -> Result<usize, request::Error> {
    let count = len.checked_mul(width.get()).ok_or(request::Error::Mul {
        left: len,
        right: width.get(),
    })?;
    add_count(base, count)
}

fn next_item(declared: usize, actual: usize) -> Result<usize, request::Error> {
    let actual = add_count(actual, 1)?;
    if actual > declared {
        return Err(request::Error::Items { declared, actual });
    }
    Ok(actual)
}

fn check_items(declared: usize, actual: usize) -> Result<(), request::Error> {
    if actual != declared {
        return Err(request::Error::Items { declared, actual });
    }
    Ok(())
}

const _: () = assert!(usize::BITS <= u64::BITS);
const _: () = assert!(std::mem::size_of::<Length>() == std::mem::size_of::<usize>());
const _: () = assert!(std::mem::size_of::<Writer>() == 3 * std::mem::size_of::<usize>());
const _: () = assert!(
    std::mem::size_of::<Complete>()
        == std::mem::size_of::<Buffer>() + std::mem::size_of::<egress::Geometry>()
);
const _: () = assert!(std::mem::size_of::<Option<Complete>>() == std::mem::size_of::<Complete>());

pub(crate) mod write;
