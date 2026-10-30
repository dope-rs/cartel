use crate::schema::Row;

/// Constructs a named view from its statically described wire fields.
pub trait Project: Row {
    type Fields: Row;

    fn project<'row>(fields: <Self::Fields as Row>::View<'row>) -> Self::View<'row>;
}
