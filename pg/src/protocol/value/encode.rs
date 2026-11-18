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

/// A statically ordered borrow of supported parameter values.
pub trait Arguments: arguments::Sealed {}

impl<T: arguments::Sealed> Arguments for T {}

#[inline]
pub(crate) fn bind<Q: crate::schema::TypedQuery>(params: &Q::Params<'_>) -> impl Arguments {
    admitted::<Q, _>(Q::bind(params))
}

#[inline]
fn admitted<Q: crate::schema::TypedQuery, A: Arguments>(values: A) -> A {
    const {
        match arguments::count::<A>(Q::PARAM_OIDS.len()) {
            Ok(value) => value,
            Err(_) => panic!("parameters"),
        }
    }
    values
}

pub(crate) use arguments::Sealed as WriteArguments;

mod arguments {
    use super::{Param, encode};
    use crate::schema::TypedQuery;

    pub trait Sealed {
        const COUNT: usize;

        fn write<Q: TypedQuery>(&self, out: &mut encode::Writer<'_>) -> Result<(), encode::Error>;
    }

    pub(super) const fn count<A: Sealed>(expected: usize) -> Result<(), encode::Error> {
        if A::COUNT != expected {
            return Err(encode::Error::ParameterValueCount {
                expected,
                actual: A::COUNT,
            });
        }
        Ok(())
    }

    const fn parameter<Q: TypedQuery, P: Param + ?Sized>(index: usize) -> Result<(), encode::Error> {
        let expected = Q::PARAM_OIDS[index];
        if expected != P::OID {
            return Err(encode::Error::ParameterOid {
                index,
                expected,
                actual: P::OID,
            });
        }
        let expected = match Q::PARAM_FORMAT_CODES {
            [] => 0,
            [format] => *format,
            formats => formats[index],
        };
        if expected != P::FORMAT {
            return Err(encode::Error::ParameterEncoding {
                index,
                expected,
                actual: P::FORMAT,
            });
        }
        Ok(())
    }

    impl Sealed for () {
        const COUNT: usize = 0;

        fn write<Q: TypedQuery>(&self, _: &mut encode::Writer<'_>) -> Result<(), encode::Error> {
            Ok(())
        }
    }

    impl<P: Param + ?Sized, T: Sealed> Sealed for (&P, T) {
        const COUNT: usize = T::COUNT + 1;

        #[inline]
        fn write<Q: TypedQuery>(&self, out: &mut encode::Writer<'_>) -> Result<(), encode::Error> {
            const {
                match parameter::<Q, P>(Q::PARAM_OIDS.len() - Self::COUNT) {
                    Ok(value) => value,
                    Err(_) => panic!("parameters"),
                }
            }
            self.0.payload(out)?;
            self.1.write::<Q>(out)
        }
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
        out.write_words(&[4i32.to_be_bytes(), self.to_be_bytes()])
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
        out.write_words(&[4i32.to_be_bytes(), self.to_be_bytes()])
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
        self.0.payload(out)
    }
}

impl Param for crate::Date {
    const OID: u32 = desc::DATE;

    #[inline]
    fn payload(&self, out: &mut encode::Writer<'_>) -> Result<(), encode::Error> {
        self.0.payload(out)
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
            out.write_words(&[
                1i32.to_be_bytes(),
                0i32.to_be_bytes(),
                T::OID.to_be_bytes(),
                count.to_be_bytes(),
                1i32.to_be_bytes(),
            ])?;
            for value in self {
                value.payload(out)?;
            }
            Ok(())
        })
    }
}
