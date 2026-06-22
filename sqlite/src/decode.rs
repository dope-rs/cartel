use rusqlite::{Result, Row};

pub trait Decode: Sized {
    const N_COLS: usize;
    fn decode_at(row: &Row<'_>, col_offset: usize) -> Result<Self>;
    fn decode(row: &Row<'_>) -> Result<Self> {
        Self::decode_at(row, 0)
    }
}

impl Decode for i8 {
    const N_COLS: usize = 1;
    fn decode_at(row: &Row<'_>, off: usize) -> Result<Self> {
        row.get(off)
    }
}
impl Decode for i16 {
    const N_COLS: usize = 1;
    fn decode_at(row: &Row<'_>, off: usize) -> Result<Self> {
        row.get(off)
    }
}
impl Decode for i32 {
    const N_COLS: usize = 1;
    fn decode_at(row: &Row<'_>, off: usize) -> Result<Self> {
        row.get(off)
    }
}
impl Decode for i64 {
    const N_COLS: usize = 1;
    fn decode_at(row: &Row<'_>, off: usize) -> Result<Self> {
        row.get(off)
    }
}
impl Decode for isize {
    const N_COLS: usize = 1;
    fn decode_at(row: &Row<'_>, off: usize) -> Result<Self> {
        row.get(off)
    }
}
impl Decode for u8 {
    const N_COLS: usize = 1;
    fn decode_at(row: &Row<'_>, off: usize) -> Result<Self> {
        row.get(off)
    }
}
impl Decode for u16 {
    const N_COLS: usize = 1;
    fn decode_at(row: &Row<'_>, off: usize) -> Result<Self> {
        row.get(off)
    }
}
impl Decode for u32 {
    const N_COLS: usize = 1;
    fn decode_at(row: &Row<'_>, off: usize) -> Result<Self> {
        row.get(off)
    }
}
impl Decode for u64 {
    const N_COLS: usize = 1;
    fn decode_at(row: &Row<'_>, off: usize) -> Result<Self> {
        row.get(off)
    }
}
impl Decode for f32 {
    const N_COLS: usize = 1;
    fn decode_at(row: &Row<'_>, off: usize) -> Result<Self> {
        row.get(off)
    }
}
impl Decode for f64 {
    const N_COLS: usize = 1;
    fn decode_at(row: &Row<'_>, off: usize) -> Result<Self> {
        row.get(off)
    }
}
impl Decode for bool {
    const N_COLS: usize = 1;
    fn decode_at(row: &Row<'_>, off: usize) -> Result<Self> {
        row.get(off)
    }
}
impl Decode for String {
    const N_COLS: usize = 1;
    fn decode_at(row: &Row<'_>, off: usize) -> Result<Self> {
        row.get(off)
    }
}
impl Decode for Vec<u8> {
    const N_COLS: usize = 1;
    fn decode_at(row: &Row<'_>, off: usize) -> Result<Self> {
        row.get(off)
    }
}

impl<T: rusqlite::types::FromSql> Decode for Option<T> {
    const N_COLS: usize = 1;
    fn decode_at(row: &Row<'_>, off: usize) -> Result<Self> {
        row.get(off)
    }
}

impl<A: Decode, B: Decode> Decode for (A, B) {
    const N_COLS: usize = A::N_COLS + B::N_COLS;
    fn decode_at(row: &Row<'_>, off: usize) -> Result<Self> {
        let a = A::decode_at(row, off)?;
        let b = B::decode_at(row, off + A::N_COLS)?;
        Ok((a, b))
    }
}
impl<A: Decode, B: Decode, C: Decode> Decode for (A, B, C) {
    const N_COLS: usize = A::N_COLS + B::N_COLS + C::N_COLS;
    fn decode_at(row: &Row<'_>, off: usize) -> Result<Self> {
        let a = A::decode_at(row, off)?;
        let b = B::decode_at(row, off + A::N_COLS)?;
        let c = C::decode_at(row, off + A::N_COLS + B::N_COLS)?;
        Ok((a, b, c))
    }
}
impl<A: Decode, B: Decode, C: Decode, D: Decode> Decode for (A, B, C, D) {
    const N_COLS: usize = A::N_COLS + B::N_COLS + C::N_COLS + D::N_COLS;
    fn decode_at(row: &Row<'_>, off: usize) -> Result<Self> {
        let a = A::decode_at(row, off)?;
        let b = B::decode_at(row, off + A::N_COLS)?;
        let c = C::decode_at(row, off + A::N_COLS + B::N_COLS)?;
        let d = D::decode_at(row, off + A::N_COLS + B::N_COLS + C::N_COLS)?;
        Ok((a, b, c, d))
    }
}
