use crate::reply::Error;

/// Terminal admission or response result with its committed lease intact.
pub(in crate::api) enum Done<'d, T, C, const ID: u8> {
    AdmissionFailed(Error<'d, ID>),
    Response {
        outcome: Result<T, Error<'d, ID>>,
        committed: C,
    },
}
