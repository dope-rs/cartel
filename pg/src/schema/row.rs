use crate::protocol::value::decode;
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

    fn read<'row>(column: decode::Column<'row>) -> Result<Self::View<'row>, crate::Error>;
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

    impl Nullable for bool {}
    impl Nullable for i16 {}
    impl Nullable for i32 {}
    impl Nullable for i64 {}
    impl Nullable for f32 {}
    impl Nullable for f64 {}
    impl Nullable for String {}
    impl Nullable for crate::Text {}
    impl Nullable for crate::Jsonb {}
    impl Nullable for crate::Ltree {}
    impl Nullable for Vec<u8> {}
    impl Nullable for crate::Uuid {}
    impl Nullable for crate::Timestamp {}
    impl Nullable for crate::Date {}
}

impl<T: Field> Row for T {
    type View<'row> = T::View<'row>;

    #[inline]
    fn decode<'row>(reader: &mut decode::RowReader<'row>) -> Result<T::View<'row>, crate::Error> {
        T::read(reader.require()?)
    }

    #[inline]
    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        T::into_owned(T::read(reader.require()?)?)
    }
}

impl<T: Field + Nullable> Row for Option<T> {
    type View<'row> = Option<T::View<'row>>;

    #[inline]
    fn decode<'row>(
        reader: &mut decode::RowReader<'row>,
    ) -> Result<Self::View<'row>, crate::Error> {
        match reader.take_column()? {
            decode::Field::Data(column) => T::read(column).map(Some),
            decode::Field::Null { .. } => Ok(None),
        }
    }

    #[inline]
    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        Self::decode(reader)?.map(T::into_owned).transpose()
    }
}

impl Field for bool {
    type View<'row> = Self;
    const TARGET: desc::Target = desc::Target::Exact(desc::BOOL);

    #[inline]
    fn read<'row>(column: decode::Column<'row>) -> Result<Self, crate::Error> {
        column.read_bool()
    }

    #[inline]
    fn into_owned<'row>(value: Self) -> Result<Self, crate::Error> {
        Ok(value)
    }
}

impl Field for i16 {
    type View<'row> = Self;
    const TARGET: desc::Target = desc::Target::Exact(desc::INT2);

    #[inline]
    fn read<'row>(column: decode::Column<'row>) -> Result<Self, crate::Error> {
        Ok(Self::from_be_bytes(column.fixed()?))
    }

    #[inline]
    fn into_owned<'row>(value: Self) -> Result<Self, crate::Error> {
        Ok(value)
    }
}

impl Field for i32 {
    type View<'row> = Self;
    const TARGET: desc::Target = desc::Target::Exact(desc::INT4);

    #[inline]
    fn read<'row>(column: decode::Column<'row>) -> Result<Self, crate::Error> {
        Ok(Self::from_be_bytes(column.fixed()?))
    }

    #[inline]
    fn into_owned<'row>(value: Self) -> Result<Self, crate::Error> {
        Ok(value)
    }
}

impl Field for i64 {
    type View<'row> = Self;
    const TARGET: desc::Target = desc::Target::Exact(desc::INT8);

    #[inline]
    fn read<'row>(column: decode::Column<'row>) -> Result<Self, crate::Error> {
        Ok(Self::from_be_bytes(column.fixed()?))
    }

    #[inline]
    fn into_owned<'row>(value: Self) -> Result<Self, crate::Error> {
        Ok(value)
    }
}

impl Field for f32 {
    type View<'row> = Self;
    const TARGET: desc::Target = desc::Target::Exact(desc::FLOAT4);

    #[inline]
    fn read<'row>(column: decode::Column<'row>) -> Result<Self, crate::Error> {
        Ok(Self::from_be_bytes(column.fixed()?))
    }

    #[inline]
    fn into_owned<'row>(value: Self) -> Result<Self, crate::Error> {
        Ok(value)
    }
}

impl Field for f64 {
    type View<'row> = Self;
    const TARGET: desc::Target = desc::Target::Exact(desc::FLOAT8);

    #[inline]
    fn read<'row>(column: decode::Column<'row>) -> Result<Self, crate::Error> {
        Ok(Self::from_be_bytes(column.fixed()?))
    }

    #[inline]
    fn into_owned<'row>(value: Self) -> Result<Self, crate::Error> {
        Ok(value)
    }
}

impl Field for String {
    type View<'row> = &'row str;
    const TARGET: desc::Target = desc::Target::String;

    #[inline]
    fn read<'row>(column: decode::Column<'row>) -> Result<&'row str, crate::Error> {
        column.text()
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
    fn read<'row>(column: decode::Column<'row>) -> Result<&'row str, crate::Error> {
        column.text()
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
    fn read<'row>(column: decode::Column<'row>) -> Result<&'row str, crate::Error> {
        column.json()
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
    fn read<'row>(column: decode::Column<'row>) -> Result<&'row str, crate::Error> {
        column.text()
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
    fn read<'row>(column: decode::Column<'row>) -> Result<&'row [u8], crate::Error> {
        Ok(column.bytes())
    }

    #[inline]
    fn into_owned<'row>(value: &'row [u8]) -> Result<Self, crate::Error> {
        crate::Error::try_copy(value)
    }
}

impl Field for crate::Uuid {
    type View<'row> = Self;
    const TARGET: desc::Target = desc::Target::Exact(desc::UUID);

    #[inline]
    fn read<'row>(column: decode::Column<'row>) -> Result<Self, crate::Error> {
        Ok(Self::from_bytes(column.fixed()?))
    }

    #[inline]
    fn into_owned<'row>(value: Self) -> Result<Self, crate::Error> {
        Ok(value)
    }
}

impl Field for crate::Timestamp {
    type View<'row> = Self;
    const TARGET: desc::Target = desc::Target::Exact(desc::TIMESTAMP);

    #[inline]
    fn read<'row>(column: decode::Column<'row>) -> Result<Self, crate::Error> {
        Ok(Self(i64::from_be_bytes(column.fixed()?)))
    }

    #[inline]
    fn into_owned<'row>(value: Self) -> Result<Self, crate::Error> {
        Ok(value)
    }
}

impl Field for crate::Date {
    type View<'row> = Self;
    const TARGET: desc::Target = desc::Target::Exact(desc::DATE);

    #[inline]
    fn read<'row>(column: decode::Column<'row>) -> Result<Self, crate::Error> {
        Ok(Self(i32::from_be_bytes(column.fixed()?)))
    }

    #[inline]
    fn into_owned<'row>(value: Self) -> Result<Self, crate::Error> {
        Ok(value)
    }
}

impl Field for Vec<i32> {
    type View<'row> = array::Array<'row, i32>;
    const TARGET: desc::Target = desc::Target::Exact(desc::INT4_ARRAY);

    #[inline]
    fn read<'row>(column: decode::Column<'row>) -> Result<array::Array<'row, i32>, crate::Error> {
        column.array(decode::ArrayElement::Exact(desc::INT4))
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
    fn read<'row>(column: decode::Column<'row>) -> Result<array::Array<'row, i64>, crate::Error> {
        column.array(decode::ArrayElement::Exact(desc::INT8))
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
        column: decode::Column<'row>,
    ) -> Result<array::Array<'row, &'row str>, crate::Error> {
        column.array(decode::ArrayElement::String)
    }

    #[inline]
    fn into_owned<'row>(value: array::Array<'row, &'row str>) -> Result<Self, crate::Error> {
        value.try_collect()
    }
}
