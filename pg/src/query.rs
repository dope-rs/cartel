use crate::Error;
use crate::value::{BindWriter, RowReader};
use crate::wire::Sink;

#[derive(Clone, Copy)]
pub struct QueryMeta {
    pub name: &'static str,
    pub sql: &'static str,
    pub param_oids: &'static [u32],
}

pub trait TypedQuery {
    type Params<'p>;
    type Row: 'static;
    type Group;

    const STATEMENT_NAME: &'static str;
    const SQL: &'static str;
    const PARAM_OIDS: &'static [u32];
    const N_PARAMS: u16;
    const N_RESULT_COLS: u16;

    const PARAM_FORMAT_CODES: &'static [u16] = &[1];
    const RESULT_FORMAT_CODES: &'static [u16] = &[1];

    fn encode_params<S: Sink>(params: Self::Params<'_>, w: &mut BindWriter<'_, S>);
    fn decode_row(r: &mut RowReader<'_>) -> Result<Self::Row, Error>;
}

pub trait QueryGroup {
    const QUERIES: &'static [QueryMeta];
}

pub trait HasGroup<G: QueryGroup> {}

pub trait QuerySet: 'static {
    const GROUPS: &'static [&'static [QueryMeta]];
}

pub trait Row: Sized {
    fn decode(r: &mut RowReader<'_>) -> Result<Self, Error>;
}

impl<A: Row, B: Row> Row for (A, B) {
    fn decode(r: &mut RowReader<'_>) -> Result<Self, Error> {
        let a = A::decode(r)?;
        let b = B::decode(r)?;
        Ok((a, b))
    }
}

impl<A: Row, B: Row, C: Row> Row for (A, B, C) {
    fn decode(r: &mut RowReader<'_>) -> Result<Self, Error> {
        let a = A::decode(r)?;
        let b = B::decode(r)?;
        let c = C::decode(r)?;
        Ok((a, b, c))
    }
}

impl<A: Row, B: Row, C: Row, D: Row> Row for (A, B, C, D) {
    fn decode(r: &mut RowReader<'_>) -> Result<Self, Error> {
        let a = A::decode(r)?;
        let b = B::decode(r)?;
        let c = C::decode(r)?;
        let d = D::decode(r)?;
        Ok((a, b, c, d))
    }
}

impl Row for bool {
    fn decode(r: &mut RowReader<'_>) -> Result<Self, Error> {
        r.read_bool()
    }
}

impl Row for i16 {
    fn decode(r: &mut RowReader<'_>) -> Result<Self, Error> {
        r.read_i16()
    }
}

impl Row for i32 {
    fn decode(r: &mut RowReader<'_>) -> Result<Self, Error> {
        r.read_i32()
    }
}

impl Row for i64 {
    fn decode(r: &mut RowReader<'_>) -> Result<Self, Error> {
        r.read_i64()
    }
}

impl Row for f32 {
    fn decode(r: &mut RowReader<'_>) -> Result<Self, Error> {
        r.read_f32()
    }
}

impl Row for f64 {
    fn decode(r: &mut RowReader<'_>) -> Result<Self, Error> {
        r.read_f64()
    }
}

impl Row for String {
    fn decode(r: &mut RowReader<'_>) -> Result<Self, Error> {
        r.read_text().map(str::to_owned)
    }
}

impl Row for Option<bool> {
    fn decode(r: &mut RowReader<'_>) -> Result<Self, Error> {
        r.read_opt_bool()
    }
}

impl Row for Option<i32> {
    fn decode(r: &mut RowReader<'_>) -> Result<Self, Error> {
        r.read_opt_i32()
    }
}

impl Row for Option<i64> {
    fn decode(r: &mut RowReader<'_>) -> Result<Self, Error> {
        r.read_opt_i64()
    }
}

impl Row for Option<String> {
    fn decode(r: &mut RowReader<'_>) -> Result<Self, Error> {
        r.read_opt_text().map(|o| o.map(str::to_owned))
    }
}

impl Row for Option<i16> {
    fn decode(r: &mut RowReader<'_>) -> Result<Self, Error> {
        r.read_opt_i16()
    }
}

impl Row for Option<f32> {
    fn decode(r: &mut RowReader<'_>) -> Result<Self, Error> {
        r.read_opt_f32()
    }
}

impl Row for Option<f64> {
    fn decode(r: &mut RowReader<'_>) -> Result<Self, Error> {
        r.read_opt_f64()
    }
}

impl Row for crate::Ltree {
    fn decode(r: &mut RowReader<'_>) -> Result<Self, Error> {
        r.read_text().map(|s| Self(s.to_owned()))
    }
}

impl Row for Option<crate::Ltree> {
    fn decode(r: &mut RowReader<'_>) -> Result<Self, Error> {
        r.read_opt_text()
            .map(|o| o.map(|s| crate::Ltree(s.to_owned())))
    }
}

impl Row for Vec<u8> {
    fn decode(r: &mut RowReader<'_>) -> Result<Self, Error> {
        r.read_bytes().map(<[u8]>::to_vec)
    }
}

impl Row for Option<Vec<u8>> {
    fn decode(r: &mut RowReader<'_>) -> Result<Self, Error> {
        r.read_opt_bytes().map(|o| o.map(<[u8]>::to_vec))
    }
}

impl Row for crate::Uuid {
    fn decode(r: &mut RowReader<'_>) -> Result<Self, Error> {
        r.read_uuid().map(crate::Uuid::from_bytes)
    }
}

impl Row for Option<crate::Uuid> {
    fn decode(r: &mut RowReader<'_>) -> Result<Self, Error> {
        r.read_opt_uuid().map(|o| o.map(crate::Uuid::from_bytes))
    }
}

impl Row for crate::Timestamp {
    fn decode(r: &mut RowReader<'_>) -> Result<Self, Error> {
        r.read_timestamp().map(crate::Timestamp)
    }
}

impl Row for Option<crate::Timestamp> {
    fn decode(r: &mut RowReader<'_>) -> Result<Self, Error> {
        r.read_opt_timestamp().map(|o| o.map(crate::Timestamp))
    }
}

impl Row for crate::Date {
    fn decode(r: &mut RowReader<'_>) -> Result<Self, Error> {
        r.read_date().map(crate::Date)
    }
}

impl Row for Option<crate::Date> {
    fn decode(r: &mut RowReader<'_>) -> Result<Self, Error> {
        r.read_opt_date().map(|o| o.map(crate::Date))
    }
}

impl Row for Vec<i32> {
    fn decode(r: &mut RowReader<'_>) -> Result<Self, Error> {
        r.read_array_i32()
    }
}

impl Row for Vec<i64> {
    fn decode(r: &mut RowReader<'_>) -> Result<Self, Error> {
        r.read_array_i64()
    }
}

impl Row for Vec<String> {
    fn decode(r: &mut RowReader<'_>) -> Result<Self, Error> {
        r.read_array_text()
    }
}
