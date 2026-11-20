use crate::protocol::{Reason, value::decode, value::encode::Param};
use crate::schema::{array, desc};
use o3::base::marker::ThreadBound;

fn own<T>(value: &str, make: fn(crate::TextStorage) -> T) -> Result<T, crate::Error> {
    Ok(make((crate::Error::try_string(value)?, ThreadBound::NEW)))
}

pub trait Row: desc::Validate + Sized {
    type View<'row>;

    fn decode<'row>(reader: &mut decode::RowReader<'row>)
    -> Result<Self::View<'row>, crate::Error>;

    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error>;
}

impl Row for () {
    type View<'row> = ();

    fn decode<'row>(_: &mut decode::RowReader<'row>) -> Result<Self::View<'row>, crate::Error> {
        Ok(())
    }

    fn decode_owned(_: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        Ok(())
    }
}

impl<A: Row, B: Row> Row for (A, B) {
    type View<'row> = (A::View<'row>, B::View<'row>);

    #[inline(always)]
    fn decode<'row>(
        reader: &mut decode::RowReader<'row>,
    ) -> Result<Self::View<'row>, crate::Error> {
        Ok((A::decode(reader)?, B::decode(reader)?))
    }

    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        Ok((A::decode_owned(reader)?, B::decode_owned(reader)?))
    }
}

impl<A: Row, B: Row, C: Row> Row for (A, B, C) {
    type View<'row> = (A::View<'row>, B::View<'row>, C::View<'row>);

    fn decode<'row>(
        reader: &mut decode::RowReader<'row>,
    ) -> Result<Self::View<'row>, crate::Error> {
        Ok((A::decode(reader)?, B::decode(reader)?, C::decode(reader)?))
    }

    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        Ok((
            A::decode_owned(reader)?,
            B::decode_owned(reader)?,
            C::decode_owned(reader)?,
        ))
    }
}

impl<A: Row, B: Row, C: Row, D: Row> Row for (A, B, C, D) {
    type View<'row> = (A::View<'row>, B::View<'row>, C::View<'row>, D::View<'row>);

    fn decode<'row>(
        reader: &mut decode::RowReader<'row>,
    ) -> Result<Self::View<'row>, crate::Error> {
        Ok((
            A::decode(reader)?,
            B::decode(reader)?,
            C::decode(reader)?,
            D::decode(reader)?,
        ))
    }

    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        Ok((
            A::decode_owned(reader)?,
            B::decode_owned(reader)?,
            C::decode_owned(reader)?,
            D::decode_owned(reader)?,
        ))
    }
}

#[doc(hidden)]
pub trait Field: sealed::Sealed + Sized {
    type View<'row>;
    const TARGET: desc::Target;
    const FORMAT: u16 = 1;
    const DYNAMIC_TYPES: u8 = 0;

    fn read<'row>(reader: &mut decode::RowReader<'row>) -> Result<Self::View<'row>, Reason>;
    fn into_owned<'row>(value: Self::View<'row>) -> Result<Self, crate::Error>;
}

pub(crate) use sealed::Nullable;

pub(crate) mod sealed {
    pub trait Sealed {}
    pub(crate) trait Nullable {}

    impl<T: Nullable> Sealed for T {}
    impl Sealed for Vec<i32> {}
    impl Sealed for Vec<i64> {}
    impl Sealed for Vec<String> {}

    impl<T: super::Scalar> Nullable for T {}
    impl Nullable for String {}
    impl Nullable for crate::Text {}
    impl Nullable for crate::Jsonb {}
    impl Nullable for crate::Ltree {}
    impl Nullable for Vec<u8> {}
}

impl<T: Field> Row for T {
    type View<'row> = T::View<'row>;

    #[inline]
    fn decode<'row>(reader: &mut decode::RowReader<'row>) -> Result<T::View<'row>, crate::Error> {
        T::read(reader).map_err(crate::Error::Protocol)
    }

    #[inline]
    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        T::into_owned(T::decode(reader)?)
    }
}

impl<T: Field + Nullable> Row for Option<T> {
    type View<'row> = Option<T::View<'row>>;

    #[inline]
    fn decode<'row>(
        reader: &mut decode::RowReader<'row>,
    ) -> Result<Self::View<'row>, crate::Error> {
        match T::read(reader) {
            Ok(value) => Ok(Some(value)),
            Err(Reason::ColumnNull { .. }) => Ok(None),
            Err(reason) => Err(crate::Error::Protocol(reason)),
        }
    }

    #[inline]
    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        Self::decode(reader)?.map(T::into_owned).transpose()
    }
}

trait Scalar: Copy {
    fn read(reader: &mut decode::RowReader<'_>) -> Result<Self, Reason>;
}

impl<T: Scalar + Param> Field for T {
    type View<'row> = Self;
    const TARGET: desc::Target = desc::Target::Exact(T::OID);

    #[inline]
    fn read<'row>(reader: &mut decode::RowReader<'row>) -> Result<Self, Reason> {
        <T as Scalar>::read(reader)
    }

    #[inline]
    fn into_owned<'row>(value: Self) -> Result<Self, crate::Error> {
        Ok(value)
    }
}

impl Scalar for bool {
    #[inline]
    fn read(reader: &mut decode::RowReader<'_>) -> Result<Self, Reason> {
        reader.read_bool()
    }
}

impl Scalar for i16 {
    #[inline]
    fn read(reader: &mut decode::RowReader<'_>) -> Result<Self, Reason> {
        Ok(Self::from_be_bytes(reader.fixed()?))
    }
}

impl Scalar for i32 {
    #[inline]
    fn read(reader: &mut decode::RowReader<'_>) -> Result<Self, Reason> {
        Ok(Self::from_be_bytes(reader.fixed()?))
    }
}

impl Scalar for i64 {
    #[inline]
    fn read(reader: &mut decode::RowReader<'_>) -> Result<Self, Reason> {
        Ok(Self::from_be_bytes(reader.fixed()?))
    }
}

impl Scalar for f32 {
    #[inline]
    fn read(reader: &mut decode::RowReader<'_>) -> Result<Self, Reason> {
        Ok(Self::from_be_bytes(reader.fixed()?))
    }
}

impl Scalar for f64 {
    #[inline]
    fn read(reader: &mut decode::RowReader<'_>) -> Result<Self, Reason> {
        Ok(Self::from_be_bytes(reader.fixed()?))
    }
}

impl Field for String {
    type View<'row> = &'row str;
    const TARGET: desc::Target = desc::Target::String;

    #[inline]
    fn read<'row>(reader: &mut decode::RowReader<'row>) -> Result<&'row str, Reason> {
        reader.column()?.text()
    }

    #[inline]
    fn into_owned<'row>(value: &'row str) -> Result<Self, crate::Error> {
        crate::Error::try_string(value)
    }
}

impl Field for crate::Text {
    type View<'row> = &'row str;
    const TARGET: desc::Target = desc::Target::String;

    #[inline]
    fn read<'row>(reader: &mut decode::RowReader<'row>) -> Result<&'row str, Reason> {
        reader.column()?.text()
    }

    #[inline]
    fn into_owned<'row>(value: &'row str) -> Result<Self, crate::Error> {
        own(value, Self)
    }
}

impl Field for crate::Jsonb {
    type View<'row> = &'row str;
    const TARGET: desc::Target = desc::Target::Exact(desc::JSONB);

    #[inline]
    fn read<'row>(reader: &mut decode::RowReader<'row>) -> Result<&'row str, Reason> {
        reader.column()?.json()
    }

    #[inline]
    fn into_owned<'row>(value: &'row str) -> Result<Self, crate::Error> {
        own(value, Self)
    }
}

impl Field for crate::Ltree {
    type View<'row> = &'row str;
    const TARGET: desc::Target = desc::Target::Ltree;
    const FORMAT: u16 = 0;
    const DYNAMIC_TYPES: u8 = desc::DYNAMIC_LTREE;

    #[inline]
    fn read<'row>(reader: &mut decode::RowReader<'row>) -> Result<&'row str, Reason> {
        reader.column()?.text()
    }

    #[inline]
    fn into_owned<'row>(value: &'row str) -> Result<Self, crate::Error> {
        own(value, Self)
    }
}

impl Field for Vec<u8> {
    type View<'row> = &'row [u8];
    const TARGET: desc::Target = desc::Target::Exact(desc::BYTEA);

    #[inline]
    fn read<'row>(reader: &mut decode::RowReader<'row>) -> Result<&'row [u8], Reason> {
        Ok(reader.column()?.bytes())
    }

    #[inline]
    fn into_owned<'row>(value: &'row [u8]) -> Result<Self, crate::Error> {
        crate::Error::try_copy(value)
    }
}

impl Scalar for crate::Uuid {
    #[inline]
    fn read(reader: &mut decode::RowReader<'_>) -> Result<Self, Reason> {
        Ok(Self::from_bytes(reader.fixed()?))
    }
}

impl Scalar for crate::Timestamp {
    #[inline]
    fn read(reader: &mut decode::RowReader<'_>) -> Result<Self, Reason> {
        Ok(Self(i64::from_be_bytes(reader.fixed()?)))
    }
}

impl Scalar for crate::Date {
    #[inline]
    fn read(reader: &mut decode::RowReader<'_>) -> Result<Self, Reason> {
        Ok(Self(i32::from_be_bytes(reader.fixed()?)))
    }
}

impl Field for Vec<i32> {
    type View<'row> = array::Array<'row, i32>;
    const TARGET: desc::Target = desc::Target::Exact(desc::INT4_ARRAY);

    #[inline]
    fn read<'row>(reader: &mut decode::RowReader<'row>) -> Result<array::Array<'row, i32>, Reason> {
        reader.column()?.array(decode::ArrayElement::Exact(desc::INT4))
    }

    #[inline]
    fn into_owned<'row>(value: array::Array<'row, i32>) -> Result<Self, crate::Error> {
        value.try_collect()
    }
}

impl Field for Vec<i64> {
    type View<'row> = array::Array<'row, i64>;
    const TARGET: desc::Target = desc::Target::Exact(desc::INT8_ARRAY);

    #[inline]
    fn read<'row>(reader: &mut decode::RowReader<'row>) -> Result<array::Array<'row, i64>, Reason> {
        reader.column()?.array(decode::ArrayElement::Exact(desc::INT8))
    }

    #[inline]
    fn into_owned<'row>(value: array::Array<'row, i64>) -> Result<Self, crate::Error> {
        value.try_collect()
    }
}

impl Field for Vec<String> {
    type View<'row> = array::Array<'row, &'row str>;
    const TARGET: desc::Target = desc::Target::StringArray;

    #[inline]
    fn read<'row>(
        reader: &mut decode::RowReader<'row>,
    ) -> Result<array::Array<'row, &'row str>, Reason> {
        reader.column()?.array(decode::ArrayElement::String)
    }

    #[inline]
    fn into_owned<'row>(value: array::Array<'row, &'row str>) -> Result<Self, crate::Error> {
        value.try_collect()
    }
}
