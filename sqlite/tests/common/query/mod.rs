use cartel_sqlite::{Action, Code, Error, row};

pub mod analytic;

pub mod dispatch;

pub mod filter;

pub mod item;

pub mod mutation;

pub mod relation;

pub mod stream;

#[derive(Clone, Copy)]
#[repr(align(128))]
pub struct StepError;

// SAFETY: errors consume no columns and retention keeps no borrowed storage.
unsafe impl row::raw::Decode for StepError {
    const N_COLS: usize = 1;
    type Borrowed<'row> = Self;
    type Retention = row::modes::Plain;

    fn borrow<'row>(_: &mut row::raw::Reader<'row>) -> Result<Self, Error> {
        Err(Error::Sqlite {
            action: Action::Step,
            code: Code::new(libsqlite3_sys::SQLITE_CONSTRAINT),
        })
    }

    fn retain<'source, 'arena, T: Copy + 'arena>(
        value: Self,
        _: &mut <Self::Retention as row::modes::Kind>::State<'arena, T>,
    ) -> Result<Self, Error> {
        Ok(value)
    }
}
