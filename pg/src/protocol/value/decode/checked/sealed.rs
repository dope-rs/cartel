use crate::{protocol::value::decode::RowReader, schema::Row};

pub trait Sealed: Row {
    fn validate(reader: &mut RowReader<'_>) -> Result<(), crate::Error> {
        Self::decode(reader).map(|_| ())
    }
}

impl Sealed for bool {}
impl Sealed for i32 {}
impl Sealed for i64 {}
impl Sealed for String {}
impl Sealed for crate::Jsonb {}
impl<A: Sealed> Sealed for (A, ()) {
    fn validate(reader: &mut RowReader<'_>) -> Result<(), crate::Error> {
        <A as Sealed>::validate(reader)
    }
}

impl<A: Sealed, B: Sealed> Sealed for (A, B) {
    fn validate(reader: &mut RowReader<'_>) -> Result<(), crate::Error> {
        <A as Sealed>::validate(reader)?;
        <B as Sealed>::validate(reader)
    }
}
