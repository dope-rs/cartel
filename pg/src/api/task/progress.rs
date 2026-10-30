use crate::reply::Error;

/// Borrowed admission progress or one completed response.
pub(in crate::api) enum Progress<'a, 'd, T, C, const ID: u8> {
    Admitted(&'a C),
    Complete {
        outcome: Result<T, Error<'d, ID>>,
        committed: C,
    },
}
