use crate::schema::{self, Prepare};

/// Generated wire bound to the exact metadata used to encode it.
#[doc(hidden)]
pub struct Wire<'a, const N: usize> {
    bytes: [u8; N],
    statements: u32,
    groups: &'a [&'a [schema::QueryMeta<'a>]],
    flags: u8,
}

impl<'a, const N: usize> Wire<'a, N> {
    pub(super) const fn new(
        bytes: [u8; N],
        stmts: u32,
        groups: &'a [&'a [schema::QueryMeta<'a>]],
        flags: u8,
    ) -> Self {
        Self {
            bytes,
            statements: stmts,
            groups,
            flags,
        }
    }

    pub const fn prepare<'wire>(&'wire self) -> Prepare<'wire>
    where
        'a: 'wire,
    {
        Prepare {
            wire: &self.bytes,
            statements: self.statements,
            groups: self.groups,
            flags: self.flags,
        }
    }
}
