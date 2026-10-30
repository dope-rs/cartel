use std::num;

use cartel_core::frame::{Buffer, Sensitivity};
use dope::net::link::egress;
use o3::{
    buffer::pool,
    mem::{self, lease},
};

use crate::request;

pub struct Encoder {
    frame: pool::Cursor,
}

mod draft;
pub struct Complete(egress::NonEmpty<Buffer>);

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Length(usize);

pub struct Writer {
    out: mem::Lease<draft::Draft, lease::Open>,
}

#[derive(Clone, Copy)]
struct Number(u64);

trait Arguments {
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

    fn digits(self) -> usize {
        self.0.checked_ilog10().unwrap_or(0) as usize + 1
    }
}

impl Encoder {
    pub(crate) const fn new(frame: pool::Cursor) -> Self {
        Self { frame }
    }

    pub(crate) fn command<const N: usize>(
        self,
        args: &[&[u8]; N],
    ) -> Result<Complete, request::Error> {
        self.encode(args, [], Sensitivity::Plain)
    }

    fn secret<const N: usize>(self, args: &[&[u8]; N]) -> Result<Complete, request::Error> {
        self.encode(args, [], Sensitivity::Secret)
    }

    pub(crate) fn slices<'a, const N: usize>(
        self,
        prefix: &[&[u8]; N],
        tail: impl AsRef<[&'a [u8]]>,
    ) -> Result<Complete, request::Error> {
        self.encode(prefix, tail, Sensitivity::Plain)
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
            length = length.bulk(*argument)?;
        }
        let mut out = self.admit_with(length, kind)?;
        out.array(count)?;
        for argument in prefix.iter().chain(tail) {
            out.bulk(*argument)?;
        }
        out.complete()
    }

    fn sequence<I, const P: usize>(
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
            length = length.bulk(*value)?;
        }
        let mut actual = 0;
        for item in items.clone() {
            actual = next_item(declared, actual)?;
            for value in item.values() {
                length = length.bulk(value)?;
            }
        }
        check_items(declared, actual)?;
        let mut out = self.admit(length)?;
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
        check_items(declared, actual)?;
        out.complete()
    }

    fn admit(self, length: Length) -> Result<Writer, request::Error> {
        self.admit_with(length, Sensitivity::Plain)
    }

    fn admit_with(self, length: Length, kind: Sensitivity) -> Result<Writer, request::Error> {
        let out = draft::Draft::try_new(self.frame, length.0, kind)?;
        Ok(Writer { out })
    }
}

impl Complete {
    pub(crate) fn into_frame(self) -> egress::NonEmpty<Buffer> {
        self.0
    }
}

impl Length {
    fn array(count: usize) -> Result<Self, request::Error> {
        let count = Number::new(count)?;
        Self(0).add(3)?.add(count.digits())
    }

    fn bulk(self, payload: &(impl request::Bulk + ?Sized)) -> Result<Self, request::Error> {
        let len = payload.len();
        let length = Number::new(len)?;
        self.add(5)?.add(length.digits())?.add(len)
    }

    fn add(self, amount: usize) -> Result<Self, request::Error> {
        self.0
            .checked_add(amount)
            .map(Self)
            .ok_or(request::Error::Add {
                left: self.0,
                right: amount,
            })
    }

    pub(crate) const fn get(self) -> usize {
        self.0
    }
}

impl Writer {
    pub(crate) fn try_acquire(pool: &pool::Pool, length: Length) -> Result<Self, request::Error> {
        let out = draft::Draft::try_acquire(pool, length.0)?;
        Ok(Self { out })
    }

    fn extend(&mut self, bytes: &(impl AsRef<[u8]> + ?Sized)) -> Result<(), request::Error> {
        let bytes = bytes.as_ref();
        self.out
            .parts_mut()
            .0
            .out
            .try_extend(bytes)
            .map_err(request::Error::Write)?;
        Ok(())
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
        self.extend(b"\r\n")?;
        Ok(())
    }

    fn array(&mut self, count: usize) -> Result<(), request::Error> {
        let mut header = [0; 23];
        self.extend(format_header_into(&mut header, b'*', count as u64))
    }

    fn complete(self) -> Result<Complete, request::Error> {
        if let Some(left) = num::NonZero::new(self.out.sink().out.remaining()) {
            return Err(request::Error::Incomplete { remaining: left });
        }
        let (out, ()) = self.out.into_parts();
        let frame = egress::NonEmpty::try_new(out.finish()).map_err(request::Error::Egress)?;
        Ok(Complete(frame))
    }
}

fn format_header_into(buffer: &mut [u8; 23], marker: u8, mut value: u64) -> &[u8] {
    let mut start = buffer.len() - 2;
    buffer[start] = b'\r';
    buffer[start + 1] = b'\n';
    loop {
        start -= 1;
        buffer[start] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    start -= 1;
    buffer[start] = marker;
    &buffer[start..]
}

fn format_int_into(buffer: &mut [u8; 21], value: i64) -> &[u8] {
    let mut index = buffer.len();
    let negative = value < 0;
    let mut unsigned = value.unsigned_abs();
    loop {
        index -= 1;
        buffer[index] = b'0' + (unsigned % 10) as u8;
        unsigned /= 10;
        if unsigned == 0 {
            break;
        }
    }
    if negative {
        index -= 1;
        buffer[index] = b'-';
    }
    &buffer[index..]
}

fn format_uint_into(buffer: &mut [u8; 20], mut value: u64) -> &[u8] {
    let mut index = buffer.len();
    loop {
        index -= 1;
        buffer[index] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    &buffer[index..]
}

fn format_float(buffer: &mut ryu::Buffer, value: f64) -> &[u8] {
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
const _: () = assert!(std::mem::size_of::<Writer>() == 4 * std::mem::size_of::<usize>());
const _: () = assert!(
    std::mem::size_of::<Complete>()
        == std::mem::size_of::<Buffer>() + std::mem::size_of::<egress::Geometry>()
);
const _: () = assert!(std::mem::size_of::<Option<Complete>>() == std::mem::size_of::<Complete>());

pub(crate) mod write;
