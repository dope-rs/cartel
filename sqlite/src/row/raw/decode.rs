use super::reader::{Presence, Value};
use crate::row::{self, modes};
use std::{result, str};

/// # Safety
/// An allocation-free borrowed row consumes exactly `N_COLS` on success, at most `N_COLS` on every path.
/// Read only that span; `Retention` must preserve every borrow without allocating.
pub unsafe trait Decode: Sized {
    const N_COLS: usize;
    type Borrowed<'row>: Copy;
    type Retention: modes::Kind;

    fn borrow<'row>(
        row: &mut row::raw::Reader<'row>,
    ) -> result::Result<Self::Borrowed<'row>, crate::Error>;

    fn retain<'source, 'arena, T: Copy + 'arena>(
        value: Self::Borrowed<'source>,
        state: &mut <Self::Retention as modes::Kind>::State<'arena, T>,
    ) -> result::Result<Self::Borrowed<'arena>, crate::Error>;
}

fn reject(column: i32, expected: row::Class, actual: &Value<'_>) -> crate::Error {
    crate::Error::ColumnType {
        column,
        expected,
        actual: actual.kind(),
    }
}

fn integer(row: &mut row::raw::Reader<'_>, column: i32) -> result::Result<i64, crate::Error> {
    let value = match row.value()? {
        Value::Integer(value) => value,
        actual => return Err(reject(column, row::Class::Integer, &actual)),
    };
    Ok(value)
}

unsafe impl Decode for i32 {
    const N_COLS: usize = 1;
    type Borrowed<'row> = Self;
    type Retention = modes::Plain;

    fn borrow<'row>(row: &mut row::raw::Reader<'row>) -> result::Result<Self, crate::Error> {
        let column = row.column();
        let value = integer(row, column)?;
        if value < i64::from(Self::MIN) || value > i64::from(Self::MAX) {
            return Err(crate::Error::ColumnI32 { column, value });
        }
        Ok(value as Self)
    }

    fn retain<'source, 'arena, T: Copy + 'arena>(
        value: Self,
        _state: &mut <Self::Retention as modes::Kind>::State<'arena, T>,
    ) -> result::Result<Self, crate::Error> {
        Ok(value)
    }
}

unsafe impl Decode for i64 {
    const N_COLS: usize = 1;
    type Borrowed<'row> = Self;
    type Retention = modes::Plain;

    fn borrow<'row>(row: &mut row::raw::Reader<'row>) -> result::Result<Self, crate::Error> {
        let column = row.column();
        integer(row, column)
    }

    fn retain<'source, 'arena, T: Copy + 'arena>(
        value: Self,
        _state: &mut <Self::Retention as modes::Kind>::State<'arena, T>,
    ) -> result::Result<Self, crate::Error> {
        Ok(value)
    }
}

unsafe impl Decode for f64 {
    const N_COLS: usize = 1;
    type Borrowed<'row> = Self;
    type Retention = modes::Plain;

    fn borrow<'row>(
        row: &mut row::raw::Reader<'row>,
    ) -> result::Result<Self::Borrowed<'row>, crate::Error> {
        let column = row.column();
        match row.value()? {
            Value::Integer(value) => Ok(value as Self),
            Value::Real(value) => Ok(value),
            actual => Err(reject(column, row::Class::Number, &actual)),
        }
    }

    fn retain<'source, 'arena, T: Copy + 'arena>(
        value: Self::Borrowed<'source>,
        _state: &mut <Self::Retention as modes::Kind>::State<'arena, T>,
    ) -> result::Result<Self::Borrowed<'arena>, crate::Error> {
        Ok(value)
    }
}

unsafe impl Decode for bool {
    const N_COLS: usize = 1;
    type Borrowed<'row> = Self;
    type Retention = modes::Plain;

    fn borrow<'row>(
        row: &mut row::raw::Reader<'row>,
    ) -> result::Result<Self::Borrowed<'row>, crate::Error> {
        let column = row.column();
        match row.value()? {
            Value::Integer(value) => Ok(value != 0),
            actual => Err(reject(column, row::Class::Integer, &actual)),
        }
    }

    fn retain<'source, 'arena, T: Copy + 'arena>(
        value: Self::Borrowed<'source>,
        _state: &mut <Self::Retention as modes::Kind>::State<'arena, T>,
    ) -> result::Result<Self::Borrowed<'arena>, crate::Error> {
        Ok(value)
    }
}

unsafe impl Decode for String {
    const N_COLS: usize = 1;
    type Borrowed<'row> = &'row str;
    type Retention = modes::Arena;

    fn borrow<'row>(
        row: &mut row::raw::Reader<'row>,
    ) -> result::Result<Self::Borrowed<'row>, crate::Error> {
        let column = row.column();
        match row.value()? {
            Value::Text(bytes) => {
                str::from_utf8(bytes).map_err(|error| crate::Error::ColumnUtf8 { column, error })
            }
            actual => Err(reject(column, row::Class::Text, &actual)),
        }
    }

    fn retain<'source, 'arena, T: Copy + 'arena>(
        value: Self::Borrowed<'source>,
        state: &mut <Self::Retention as modes::Kind>::State<'arena, T>,
    ) -> result::Result<Self::Borrowed<'arena>, crate::Error> {
        state.retainer().text(value)
    }
}

unsafe impl Decode for Vec<u8> {
    const N_COLS: usize = 1;
    type Borrowed<'row> = &'row [u8];
    type Retention = modes::Arena;

    fn borrow<'row>(
        row: &mut row::raw::Reader<'row>,
    ) -> result::Result<Self::Borrowed<'row>, crate::Error> {
        let column = row.column();
        match row.value()? {
            Value::Blob(bytes) => Ok(bytes),
            actual => Err(reject(column, row::Class::Blob, &actual)),
        }
    }

    fn retain<'source, 'arena, T: Copy + 'arena>(
        value: Self::Borrowed<'source>,
        state: &mut <Self::Retention as modes::Kind>::State<'arena, T>,
    ) -> result::Result<Self::Borrowed<'arena>, crate::Error> {
        state.retainer().bytes(value)
    }
}

unsafe impl<R: Decode> Decode for Option<R> {
    const N_COLS: usize = R::N_COLS;
    type Borrowed<'row> = Option<R::Borrowed<'row>>;
    type Retention = R::Retention;

    fn borrow<'row>(
        row: &mut row::raw::Reader<'row>,
    ) -> result::Result<Self::Borrowed<'row>, crate::Error> {
        match row.take_nulls(R::N_COLS) {
            Presence::Null => Ok(None),
            Presence::Value => R::borrow(row).map(Some),
        }
    }

    fn retain<'source, 'arena, T: Copy + 'arena>(
        value: Self::Borrowed<'source>,
        state: &mut <Self::Retention as modes::Kind>::State<'arena, T>,
    ) -> result::Result<Self::Borrowed<'arena>, crate::Error> {
        match value {
            Some(value) => R::retain(value, state).map(Some),
            None => Ok(None),
        }
    }
}

unsafe impl<A: Decode, B: Decode> Decode for (A, B)
where
    <<A as Decode>::Retention as modes::Kind>::With<<B as Decode>::Retention>:
        modes::Contains<<A as Decode>::Retention>,
    <<A as Decode>::Retention as modes::Kind>::With<<B as Decode>::Retention>:
        modes::Contains<<B as Decode>::Retention>,
{
    const N_COLS: usize = A::N_COLS + B::N_COLS;
    type Borrowed<'row> = (A::Borrowed<'row>, B::Borrowed<'row>);
    type Retention = <<A as Decode>::Retention as modes::Kind>::With<<B as Decode>::Retention>;

    fn borrow<'row>(
        row: &mut row::raw::Reader<'row>,
    ) -> result::Result<Self::Borrowed<'row>, crate::Error> {
        Ok((A::borrow(row)?, B::borrow(row)?))
    }

    fn retain<'source, 'arena, T: Copy + 'arena>(
        value: Self::Borrowed<'source>,
        state: &mut <Self::Retention as modes::Kind>::State<'arena, T>,
    ) -> result::Result<Self::Borrowed<'arena>, crate::Error> {
        Ok((
            A::retain(
                value.0,
                <Self::Retention as modes::Contains<<A as Decode>::Retention>>::get(state),
            )?,
            B::retain(
                value.1,
                <Self::Retention as modes::Contains<<B as Decode>::Retention>>::get(state),
            )?,
        ))
    }
}

unsafe impl<A: Decode, B: Decode, C: Decode> Decode for (A, B, C,)
where
    <<<A as Decode>::Retention as modes::Kind>::With<<B as Decode>::Retention> as modes::Kind>::With<<C as Decode>::Retention>: modes::Contains<<A as Decode>::Retention>,
    <<<A as Decode>::Retention as modes::Kind>::With<<B as Decode>::Retention> as modes::Kind>::With<<C as Decode>::Retention>: modes::Contains<<B as Decode>::Retention>,
    <<<A as Decode>::Retention as modes::Kind>::With<<B as Decode>::Retention> as modes::Kind>::With<<C as Decode>::Retention>: modes::Contains<<C as Decode>::Retention>,
{
    const N_COLS: usize = A::N_COLS + B::N_COLS + C::N_COLS;
    type Borrowed<'row> = (A::Borrowed<'row>, B::Borrowed<'row>, C::Borrowed<'row>,);
    type Retention = <<<A as Decode>::Retention as modes::Kind>::With<<B as Decode>::Retention> as modes::Kind>::With<<C as Decode>::Retention>;

    fn borrow<'row>(
        row: &mut row::raw::Reader<'row>,
    ) -> result::Result<Self::Borrowed<'row>, crate::Error> {
        Ok((
            A::borrow(row)?,
            B::borrow(row)?,
            C::borrow(row)?,
        ))
    }

    fn retain<'source, 'arena, T: Copy + 'arena>(
        value: Self::Borrowed<'source>,
        state: &mut <Self::Retention as modes::Kind>::State<'arena, T>,
    ) -> result::Result<Self::Borrowed<'arena>, crate::Error> {
        Ok((
            A::retain(
                value.0,
                <Self::Retention as modes::Contains<<A as Decode>::Retention>>::get(state),
            )?,
            B::retain(
                value.1,
                <Self::Retention as modes::Contains<<B as Decode>::Retention>>::get(state),
            )?,
            C::retain(
                value.2,
                <Self::Retention as modes::Contains<<C as Decode>::Retention>>::get(state),
            )?,
        ))
    }
}

unsafe impl<A: Decode, B: Decode, C: Decode, D: Decode> Decode for (A, B, C, D,)
where
    <<<<A as Decode>::Retention as modes::Kind>::With<<B as Decode>::Retention> as modes::Kind>::With<<C as Decode>::Retention> as modes::Kind>::With<<D as Decode>::Retention>: modes::Contains<<A as Decode>::Retention>,
    <<<<A as Decode>::Retention as modes::Kind>::With<<B as Decode>::Retention> as modes::Kind>::With<<C as Decode>::Retention> as modes::Kind>::With<<D as Decode>::Retention>: modes::Contains<<B as Decode>::Retention>,
    <<<<A as Decode>::Retention as modes::Kind>::With<<B as Decode>::Retention> as modes::Kind>::With<<C as Decode>::Retention> as modes::Kind>::With<<D as Decode>::Retention>: modes::Contains<<C as Decode>::Retention>,
    <<<<A as Decode>::Retention as modes::Kind>::With<<B as Decode>::Retention> as modes::Kind>::With<<C as Decode>::Retention> as modes::Kind>::With<<D as Decode>::Retention>: modes::Contains<<D as Decode>::Retention>,
{
    const N_COLS: usize = A::N_COLS + B::N_COLS + C::N_COLS + D::N_COLS;
    type Borrowed<'row> = (A::Borrowed<'row>, B::Borrowed<'row>, C::Borrowed<'row>, D::Borrowed<'row>,);
    type Retention = <<<<A as Decode>::Retention as modes::Kind>::With<<B as Decode>::Retention> as modes::Kind>::With<<C as Decode>::Retention> as modes::Kind>::With<<D as Decode>::Retention>;

    fn borrow<'row>(
        row: &mut row::raw::Reader<'row>,
    ) -> result::Result<Self::Borrowed<'row>, crate::Error> {
        Ok((
            A::borrow(row)?,
            B::borrow(row)?,
            C::borrow(row)?,
            D::borrow(row)?,
        ))
    }

    fn retain<'source, 'arena, T: Copy + 'arena>(
        value: Self::Borrowed<'source>,
        state: &mut <Self::Retention as modes::Kind>::State<'arena, T>,
    ) -> result::Result<Self::Borrowed<'arena>, crate::Error> {
        Ok((
            A::retain(
                value.0,
                <Self::Retention as modes::Contains<<A as Decode>::Retention>>::get(state),
            )?,
            B::retain(
                value.1,
                <Self::Retention as modes::Contains<<B as Decode>::Retention>>::get(state),
            )?,
            C::retain(
                value.2,
                <Self::Retention as modes::Contains<<C as Decode>::Retention>>::get(state),
            )?,
            D::retain(
                value.3,
                <Self::Retention as modes::Contains<<D as Decode>::Retention>>::get(state),
            )?,
        ))
    }
}
