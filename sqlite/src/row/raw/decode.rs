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

fn integer(value: Value<'_>, column: i32) -> result::Result<i64, crate::Error> {
    match value {
        Value::Integer(value) => Ok(value),
        actual => Err(reject(column, row::Class::Integer, &actual)),
    }
}

trait Scalar: Copy + 'static {
    fn read(value: Value<'_>, column: i32) -> result::Result<Self, crate::Error>;
}

// SAFETY: read receives one admitted value and returns a scalar independent of the row.
unsafe impl<S: Scalar> Decode for S {
    const N_COLS: usize = 1;
    type Borrowed<'row> = Self;
    type Retention = modes::Plain;

    fn borrow<'row>(row: &mut row::raw::Reader<'row>) -> result::Result<Self, crate::Error> {
        let column = row.column();
        S::read(row.value()?, column)
    }

    fn retain<'source, 'arena, T: Copy + 'arena>(
        value: Self,
        _state: &mut <Self::Retention as modes::Kind>::State<'arena, T>,
    ) -> result::Result<Self, crate::Error> {
        Ok(value)
    }
}

impl Scalar for i32 {
    fn read(value: Value<'_>, column: i32) -> result::Result<Self, crate::Error> {
        let value = integer(value, column)?;
        Self::try_from(value).map_err(|_| crate::Error::ColumnI32 { column, value })
    }
}

impl Scalar for i64 {
    fn read(value: Value<'_>, column: i32) -> result::Result<Self, crate::Error> {
        integer(value, column)
    }
}

impl Scalar for f64 {
    fn read(value: Value<'_>, column: i32) -> result::Result<Self, crate::Error> {
        match value {
            Value::Integer(value) => Ok(value as Self),
            Value::Real(value) => Ok(value),
            actual => Err(reject(column, row::Class::Number, &actual)),
        }
    }
}

impl Scalar for bool {
    fn read(value: Value<'_>, column: i32) -> result::Result<Self, crate::Error> {
        Ok(integer(value, column)? != 0)
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

type Joined<L, R> = <L as modes::Kind>::With<R>;

fn retain_field<'source, 'arena, R: Decode, K: modes::Contains<R::Retention>, T: Copy + 'arena>(
    value: R::Borrowed<'source>,
    state: &mut K::State<'arena, T>,
) -> result::Result<R::Borrowed<'arena>, crate::Error> {
    R::retain::<T>(value, K::get::<T>(state))
}

unsafe impl<A: Decode, B: Decode> Decode for (A, B)
where
    Joined<A::Retention, B::Retention>:
        modes::Contains<A::Retention> + modes::Contains<B::Retention>,
{
    const N_COLS: usize = A::N_COLS + B::N_COLS;
    type Borrowed<'row> = (A::Borrowed<'row>, B::Borrowed<'row>);
    type Retention = Joined<A::Retention, B::Retention>;

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
            retain_field::<A, Self::Retention, T>(value.0, state)?,
            retain_field::<B, Self::Retention, T>(value.1, state)?,
        ))
    }
}

unsafe impl<A: Decode, B: Decode, C: Decode> Decode for (A, B, C)
where
    Joined<Joined<A::Retention, B::Retention>, C::Retention>: modes::Contains<A::Retention>
        + modes::Contains<B::Retention>
        + modes::Contains<C::Retention>,
{
    const N_COLS: usize = A::N_COLS + B::N_COLS + C::N_COLS;
    type Borrowed<'row> = (A::Borrowed<'row>, B::Borrowed<'row>, C::Borrowed<'row>);
    type Retention = Joined<Joined<A::Retention, B::Retention>, C::Retention>;

    fn borrow<'row>(
        row: &mut row::raw::Reader<'row>,
    ) -> result::Result<Self::Borrowed<'row>, crate::Error> {
        Ok((A::borrow(row)?, B::borrow(row)?, C::borrow(row)?))
    }

    fn retain<'source, 'arena, T: Copy + 'arena>(
        value: Self::Borrowed<'source>,
        state: &mut <Self::Retention as modes::Kind>::State<'arena, T>,
    ) -> result::Result<Self::Borrowed<'arena>, crate::Error> {
        Ok((
            retain_field::<A, Self::Retention, T>(value.0, state)?,
            retain_field::<B, Self::Retention, T>(value.1, state)?,
            retain_field::<C, Self::Retention, T>(value.2, state)?,
        ))
    }
}

unsafe impl<A: Decode, B: Decode, C: Decode, D: Decode> Decode for (A, B, C, D)
where
    Joined<Joined<Joined<A::Retention, B::Retention>, C::Retention>, D::Retention>: modes::Contains<A::Retention>
        + modes::Contains<B::Retention>
        + modes::Contains<C::Retention>
        + modes::Contains<D::Retention>,
{
    const N_COLS: usize = A::N_COLS + B::N_COLS + C::N_COLS + D::N_COLS;
    type Borrowed<'row> = (
        A::Borrowed<'row>,
        B::Borrowed<'row>,
        C::Borrowed<'row>,
        D::Borrowed<'row>,
    );
    type Retention = Joined<Joined<Joined<A::Retention, B::Retention>, C::Retention>, D::Retention>;

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
            retain_field::<A, Self::Retention, T>(value.0, state)?,
            retain_field::<B, Self::Retention, T>(value.1, state)?,
            retain_field::<C, Self::Retention, T>(value.2, state)?,
            retain_field::<D, Self::Retention, T>(value.3, state)?,
        ))
    }
}
