use crate::protocol::value::decode;
use crate::schema::{array, desc};
use o3::base::marker::ThreadBound;
fn own<T>(value: &str, make: fn(crate::TextStorage) -> T) -> Result<T, crate::Error> {
    Ok(make((crate::Error::try_string(value)?, ThreadBound::NEW)))
}

fn own_text<T>(
    value: Option<&str>,
    make: fn(crate::TextStorage) -> T,
) -> Result<Option<T>, crate::Error> {
    value.map(|value| own(value, make)).transpose()
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

impl Row for bool {
    type View<'row> = Self;

    fn decode<'row>(reader: &mut decode::RowReader<'row>) -> Result<Self, crate::Error> {
        reader.scalar().read_bool()
    }

    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        reader.scalar().read_bool()
    }
}

impl Row for i16 {
    type View<'row> = Self;

    fn decode<'row>(reader: &mut decode::RowReader<'row>) -> Result<Self, crate::Error> {
        reader.scalar().read_i16()
    }

    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        reader.scalar().read_i16()
    }
}

impl Row for i32 {
    type View<'row> = Self;

    #[inline]
    fn decode<'row>(reader: &mut decode::RowReader<'row>) -> Result<Self, crate::Error> {
        reader.read_i32()
    }

    #[inline]
    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        reader.read_i32()
    }
}

impl Row for i64 {
    type View<'row> = Self;

    #[inline]
    fn decode<'row>(reader: &mut decode::RowReader<'row>) -> Result<Self, crate::Error> {
        reader.scalar().read_i64()
    }

    #[inline]
    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        reader.scalar().read_i64()
    }
}

impl Row for f32 {
    type View<'row> = Self;

    fn decode<'row>(reader: &mut decode::RowReader<'row>) -> Result<Self, crate::Error> {
        reader.scalar().read_f32()
    }

    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        reader.scalar().read_f32()
    }
}

impl Row for f64 {
    type View<'row> = Self;

    #[inline]
    fn decode<'row>(reader: &mut decode::RowReader<'row>) -> Result<Self, crate::Error> {
        reader.scalar().read_f64()
    }

    #[inline]
    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        reader.scalar().read_f64()
    }
}

impl Row for String {
    type View<'row> = &'row str;

    #[inline]
    fn decode<'row>(reader: &mut decode::RowReader<'row>) -> Result<&'row str, crate::Error> {
        reader.data().read_text()
    }

    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        crate::Error::try_string(reader.data().read_text()?)
    }
}

impl Row for Option<String> {
    type View<'row> = Option<&'row str>;

    fn decode<'row>(
        reader: &mut decode::RowReader<'row>,
    ) -> Result<Self::View<'row>, crate::Error> {
        reader.optional().read_text()
    }

    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        reader
            .optional()
            .read_text()?
            .map(crate::Error::try_string)
            .transpose()
    }
}

impl Row for crate::Text {
    type View<'row> = &'row str;

    fn decode<'row>(reader: &mut decode::RowReader<'row>) -> Result<&'row str, crate::Error> {
        reader.data().read_text()
    }

    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        own(reader.data().read_text()?, Self)
    }
}

impl Row for crate::Jsonb {
    type View<'row> = &'row str;

    fn decode<'row>(reader: &mut decode::RowReader<'row>) -> Result<&'row str, crate::Error> {
        reader.data().read_json()
    }

    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        own(reader.data().read_json()?, Self)
    }
}

impl Row for crate::Ltree {
    type View<'row> = &'row str;

    fn decode<'row>(reader: &mut decode::RowReader<'row>) -> Result<&'row str, crate::Error> {
        reader.data().read_text()
    }

    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        own(reader.data().read_text()?, Self)
    }
}

impl Row for Option<crate::Text> {
    type View<'row> = Option<&'row str>;

    fn decode<'row>(
        reader: &mut decode::RowReader<'row>,
    ) -> Result<Self::View<'row>, crate::Error> {
        reader.optional().read_text()
    }

    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        own_text(reader.optional().read_text()?, crate::Text)
    }
}

impl Row for Option<crate::Jsonb> {
    type View<'row> = Option<&'row str>;

    fn decode<'row>(
        reader: &mut decode::RowReader<'row>,
    ) -> Result<Self::View<'row>, crate::Error> {
        reader.optional().read_json()
    }

    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        own_text(reader.optional().read_json()?, crate::Jsonb)
    }
}

impl Row for Option<crate::Ltree> {
    type View<'row> = Option<&'row str>;

    fn decode<'row>(
        reader: &mut decode::RowReader<'row>,
    ) -> Result<Self::View<'row>, crate::Error> {
        reader.optional().read_text()
    }

    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        own_text(reader.optional().read_text()?, crate::Ltree)
    }
}

impl Row for Vec<u8> {
    type View<'row> = &'row [u8];

    fn decode<'row>(reader: &mut decode::RowReader<'row>) -> Result<&'row [u8], crate::Error> {
        reader.data().read_bytes()
    }

    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        crate::Error::try_copy(reader.data().read_bytes()?)
    }
}

impl Row for Option<Vec<u8>> {
    type View<'row> = Option<&'row [u8]>;

    fn decode<'row>(
        reader: &mut decode::RowReader<'row>,
    ) -> Result<Self::View<'row>, crate::Error> {
        reader.optional().read_bytes()
    }

    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        reader
            .optional()
            .read_bytes()?
            .map(crate::Error::try_copy)
            .transpose()
    }
}

impl Row for crate::Uuid {
    type View<'row> = Self;

    fn decode<'row>(reader: &mut decode::RowReader<'row>) -> Result<Self, crate::Error> {
        reader.scalar().read_fixed::<16>().map(Self::from_bytes)
    }

    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        reader.scalar().read_fixed::<16>().map(Self::from_bytes)
    }
}

impl Row for crate::Timestamp {
    type View<'row> = Self;

    fn decode<'row>(reader: &mut decode::RowReader<'row>) -> Result<Self, crate::Error> {
        reader.scalar().read_i64().map(Self)
    }

    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        reader.scalar().read_i64().map(Self)
    }
}

impl Row for crate::Date {
    type View<'row> = Self;

    fn decode<'row>(reader: &mut decode::RowReader<'row>) -> Result<Self, crate::Error> {
        reader.read_i32().map(Self)
    }

    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        reader.read_i32().map(Self)
    }
}

impl Row for Option<bool> {
    type View<'row> = Self;

    fn decode<'row>(reader: &mut decode::RowReader<'row>) -> Result<Self, crate::Error> {
        reader.optional().read_bool()
    }

    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        reader.optional().read_bool()
    }
}

impl Row for Option<i16> {
    type View<'row> = Self;

    fn decode<'row>(reader: &mut decode::RowReader<'row>) -> Result<Self, crate::Error> {
        reader.optional().read_i16()
    }

    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        reader.optional().read_i16()
    }
}

impl Row for Option<i32> {
    type View<'row> = Self;

    fn decode<'row>(reader: &mut decode::RowReader<'row>) -> Result<Self, crate::Error> {
        reader.optional().read_i32()
    }

    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        reader.optional().read_i32()
    }
}

impl Row for Option<i64> {
    type View<'row> = Self;

    fn decode<'row>(reader: &mut decode::RowReader<'row>) -> Result<Self, crate::Error> {
        reader.optional().read_i64()
    }

    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        reader.optional().read_i64()
    }
}

impl Row for Option<f32> {
    type View<'row> = Self;

    fn decode<'row>(reader: &mut decode::RowReader<'row>) -> Result<Self, crate::Error> {
        reader.optional().read_f32()
    }

    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        reader.optional().read_f32()
    }
}

impl Row for Option<f64> {
    type View<'row> = Self;

    fn decode<'row>(reader: &mut decode::RowReader<'row>) -> Result<Self, crate::Error> {
        reader.optional().read_f64()
    }

    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        reader.optional().read_f64()
    }
}

impl Row for Option<crate::Uuid> {
    type View<'row> = Self;

    fn decode<'row>(reader: &mut decode::RowReader<'row>) -> Result<Self, crate::Error> {
        reader
            .optional()
            .read_uuid()
            .map(|value| value.map(crate::Uuid::from_bytes))
    }

    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        Self::decode(reader)
    }
}

impl Row for Option<crate::Timestamp> {
    type View<'row> = Self;

    fn decode<'row>(reader: &mut decode::RowReader<'row>) -> Result<Self, crate::Error> {
        reader
            .optional()
            .read_i64()
            .map(|value| value.map(crate::Timestamp))
    }

    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        Self::decode(reader)
    }
}

impl Row for Option<crate::Date> {
    type View<'row> = Self;

    fn decode<'row>(reader: &mut decode::RowReader<'row>) -> Result<Self, crate::Error> {
        reader
            .optional()
            .read_i32()
            .map(|value| value.map(crate::Date))
    }

    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        Self::decode(reader)
    }
}

impl Row for Vec<i32> {
    type View<'row> = array::Array<'row, i32>;

    fn decode<'row>(
        reader: &mut decode::RowReader<'row>,
    ) -> Result<Self::View<'row>, crate::Error> {
        reader.arrays().read_i32()
    }

    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        reader.arrays().read_i32()?.try_collect()
    }
}

impl Row for Vec<i64> {
    type View<'row> = array::Array<'row, i64>;

    fn decode<'row>(
        reader: &mut decode::RowReader<'row>,
    ) -> Result<Self::View<'row>, crate::Error> {
        reader.arrays().read_i64()
    }

    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        reader.arrays().read_i64()?.try_collect()
    }
}

impl Row for Vec<String> {
    type View<'row> = array::Array<'row, &'row str>;

    fn decode<'row>(
        reader: &mut decode::RowReader<'row>,
    ) -> Result<Self::View<'row>, crate::Error> {
        reader.arrays().read_text()
    }

    fn decode_owned(reader: &mut decode::RowReader<'_>) -> Result<Self, crate::Error> {
        reader.arrays().read_text()?.try_collect()
    }
}
