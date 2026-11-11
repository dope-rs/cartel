use crate::{
    protocol::encode,
    schema::{desc, sealed},
};
use o3::buffer::write::Sink as _;

fn wire_length(value: usize) -> Result<i32, encode::Error> {
    const MAXIMUM: usize = i32::MAX as usize;
    if value > MAXIMUM {
        Err(encode::Error::ValueLength { actual: value })
    } else {
        Ok(value as i32)
    }
}

fn count32(value: usize) -> Result<i32, encode::Error> {
    const MAXIMUM: usize = i32::MAX as usize;
    if value > MAXIMUM {
        Err(encode::Error::ArrayElements { actual: value })
    } else {
        Ok(value as i32)
    }
}

#[inline]
fn write_with_len(out: &mut encode::Writer<'_>, payload: &[u8]) -> Result<(), encode::Error> {
    out.write_slices([&wire_length(payload.len())?.to_be_bytes(), payload])
}

fn jsonb_payload(out: &mut encode::Writer<'_>, value: &str) -> Result<(), encode::Error> {
    let len = value.len() + 1;
    out.write_slices([&wire_length(len)?.to_be_bytes(), &[0x01], value.as_bytes()])
}

/// Encoding retains the exact parameter domains admitted for its query type.
pub struct BindWriter<'a, 'out> {
    out: &'a mut encode::Writer<'out>,
    oids: &'a [u32],
    formats: &'a [u16],
    index: usize,
}

impl<'a, 'out> BindWriter<'a, 'out> {
    pub(crate) fn new<Q: crate::schema::TypedQuery>(
        out: &'a mut encode::Writer<'out>,
        _: &crate::schema::Admission<Q>,
    ) -> Self {
        Self {
            out,
            oids: Q::PARAM_OIDS,
            formats: Q::PARAM_FORMAT_CODES,
            index: 0,
        }
    }

    pub fn write_null(&mut self) -> Result<(), encode::Error> {
        self.check_any()?;
        self.out.write_slice(&(-1i32).to_be_bytes())?;
        self.index += 1;
        Ok(())
    }

    pub fn param<P: Param + ?Sized>(&mut self, value: &P) -> Result<(), encode::Error> {
        self.write(P::OID, P::FORMAT, |out| value.payload(out))
    }

    pub fn write_jsonb(&mut self, value: &str) -> Result<(), encode::Error> {
        self.write(crate::Jsonb::OID, crate::Jsonb::FORMAT, |out| {
            jsonb_payload(out, value)
        })
    }

    pub fn write_ltree(&mut self, value: &str) -> Result<(), encode::Error> {
        self.write(crate::Ltree::OID, crate::Ltree::FORMAT, |out| {
            write_with_len(out, value.as_bytes())
        })
    }

    pub(crate) fn finish(self) -> Result<(), encode::Error> {
        if self.index == self.oids.len() {
            Ok(())
        } else {
            Err(encode::Error::ParameterValueCount {
                expected: self.oids.len(),
                actual: self.index,
            })
        }
    }

    fn write(
        &mut self,
        oid: u32,
        format: u16,
        write: impl FnOnce(&mut encode::Writer<'_>) -> Result<(), encode::Error>,
    ) -> Result<(), encode::Error> {
        self.check(oid, format)?;
        write(self.out)?;
        self.index += 1;
        Ok(())
    }

    #[inline]
    fn check(&self, oid: u32, format: u16) -> Result<(), encode::Error> {
        let index = self.index;
        let Some(expected) = self.oids.get(index).copied() else {
            return Err(encode::Error::ParameterValueCount {
                expected: self.oids.len(),
                actual: self.oids.len() + 1,
            });
        };
        if oid != expected {
            return Err(encode::Error::ParameterOid {
                index,
                expected,
                actual: oid,
            });
        }
        let expected = match self.formats {
            [] => 0,
            [format] => *format,
            formats => {
                // SAFETY: admission binds these formats to the exact OID count;
                // the same retained OID slice admitted this index above.
                unsafe { *formats.get_unchecked(index) }
            }
        };
        if format != expected {
            return Err(encode::Error::ParameterEncoding {
                index,
                expected,
                actual: format,
            });
        }
        Ok(())
    }

    fn check_any(&self) -> Result<(), encode::Error> {
        if self.index >= self.oids.len() {
            return Err(encode::Error::ParameterValueCount {
                expected: self.oids.len(),
                actual: self.oids.len() + 1,
            });
        }
        Ok(())
    }
}

/// One supported parameter's static wire type and length-prefixed payload.
#[doc(hidden)]
pub trait Param: sealed::Sealed {
    const OID: u32;
    const FORMAT: u16 = 1;

    fn payload(&self, out: &mut encode::Writer<'_>) -> Result<(), encode::Error>;
}

impl sealed::Sealed for str {}
impl sealed::Sealed for [u8] {}
impl sealed::Sealed for &str {}
impl<T: Element> sealed::Sealed for [T] {}

impl Param for bool {
    const OID: u32 = desc::BOOL;

    #[inline]
    fn payload(&self, out: &mut encode::Writer<'_>) -> Result<(), encode::Error> {
        write_with_len(out, &[*self as u8])
    }
}

impl Param for i16 {
    const OID: u32 = desc::INT2;

    #[inline]
    fn payload(&self, out: &mut encode::Writer<'_>) -> Result<(), encode::Error> {
        write_with_len(out, &self.to_be_bytes())
    }
}

impl Param for i32 {
    const OID: u32 = desc::INT4;

    #[inline]
    fn payload(&self, out: &mut encode::Writer<'_>) -> Result<(), encode::Error> {
        write_with_len(out, &self.to_be_bytes())
    }
}

impl Param for i64 {
    const OID: u32 = desc::INT8;

    #[inline]
    fn payload(&self, out: &mut encode::Writer<'_>) -> Result<(), encode::Error> {
        write_with_len(out, &self.to_be_bytes())
    }
}

impl Param for f32 {
    const OID: u32 = desc::FLOAT4;

    #[inline]
    fn payload(&self, out: &mut encode::Writer<'_>) -> Result<(), encode::Error> {
        write_with_len(out, &self.to_be_bytes())
    }
}

impl Param for f64 {
    const OID: u32 = desc::FLOAT8;

    #[inline]
    fn payload(&self, out: &mut encode::Writer<'_>) -> Result<(), encode::Error> {
        write_with_len(out, &self.to_be_bytes())
    }
}

impl Param for str {
    const OID: u32 = desc::TEXT;

    #[inline]
    fn payload(&self, out: &mut encode::Writer<'_>) -> Result<(), encode::Error> {
        write_with_len(out, self.as_bytes())
    }
}

impl Param for [u8] {
    const OID: u32 = desc::BYTEA;

    #[inline]
    fn payload(&self, out: &mut encode::Writer<'_>) -> Result<(), encode::Error> {
        write_with_len(out, self)
    }
}

impl Param for String {
    const OID: u32 = desc::TEXT;

    #[inline]
    fn payload(&self, out: &mut encode::Writer<'_>) -> Result<(), encode::Error> {
        self.as_str().payload(out)
    }
}

impl Param for crate::Text {
    const OID: u32 = desc::TEXT;

    #[inline]
    fn payload(&self, out: &mut encode::Writer<'_>) -> Result<(), encode::Error> {
        self.as_ref().payload(out)
    }
}

impl Param for crate::Jsonb {
    const OID: u32 = desc::JSONB;

    #[inline]
    fn payload(&self, out: &mut encode::Writer<'_>) -> Result<(), encode::Error> {
        jsonb_payload(out, self.as_ref())
    }
}

impl Param for crate::Ltree {
    const OID: u32 = desc::TEXT;
    const FORMAT: u16 = 0;

    #[inline]
    fn payload(&self, out: &mut encode::Writer<'_>) -> Result<(), encode::Error> {
        self.as_ref().payload(out)
    }
}

impl Param for Vec<u8> {
    const OID: u32 = desc::BYTEA;

    #[inline]
    fn payload(&self, out: &mut encode::Writer<'_>) -> Result<(), encode::Error> {
        self.as_slice().payload(out)
    }
}

impl Param for crate::Uuid {
    const OID: u32 = desc::UUID;

    #[inline]
    fn payload(&self, out: &mut encode::Writer<'_>) -> Result<(), encode::Error> {
        write_with_len(out, self.as_bytes())
    }
}

impl Param for crate::Timestamp {
    const OID: u32 = desc::TIMESTAMP;

    #[inline]
    fn payload(&self, out: &mut encode::Writer<'_>) -> Result<(), encode::Error> {
        write_with_len(out, &self.0.to_be_bytes())
    }
}

impl Param for crate::Date {
    const OID: u32 = desc::DATE;

    #[inline]
    fn payload(&self, out: &mut encode::Writer<'_>) -> Result<(), encode::Error> {
        write_with_len(out, &self.0.to_be_bytes())
    }
}

impl Param for &str {
    const OID: u32 = desc::TEXT;

    #[inline]
    fn payload(&self, out: &mut encode::Writer<'_>) -> Result<(), encode::Error> {
        (*self).payload(out)
    }
}

trait Element: Param {
    const ARRAY_OID: u32;
}

impl Element for bool {
    const ARRAY_OID: u32 = 1000;
}

impl Element for i16 {
    const ARRAY_OID: u32 = 1005;
}

impl Element for i32 {
    const ARRAY_OID: u32 = desc::INT4_ARRAY;
}

impl Element for i64 {
    const ARRAY_OID: u32 = desc::INT8_ARRAY;
}

impl Element for f32 {
    const ARRAY_OID: u32 = 1021;
}

impl Element for f64 {
    const ARRAY_OID: u32 = 1022;
}

impl Element for &str {
    const ARRAY_OID: u32 = desc::TEXT_ARRAY;
}

impl<T: Element> Param for [T] {
    const OID: u32 = T::ARRAY_OID;

    fn payload(&self, out: &mut encode::Writer<'_>) -> Result<(), encode::Error> {
        let count = count32(self.len())?;
        out.patch::<false, _>(|out| {
            out.write_slices([
                &1i32.to_be_bytes(),
                &0i32.to_be_bytes(),
                &T::OID.to_be_bytes(),
                &count.to_be_bytes(),
                &1i32.to_be_bytes(),
            ])?;
            for value in self {
                value.payload(out)?;
            }
            Ok(())
        })
    }
}

const _: () = assert!(size_of::<BindWriter<'_, '_>>() == 6 * size_of::<usize>());
