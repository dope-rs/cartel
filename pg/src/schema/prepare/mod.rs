use std::mem;

pub(super) mod catalog;
mod frames;
use frames::{add, statement_lengths, write_query, write_slice, write_statement};

use crate::{
    protocol::encode,
    schema::{self, desc},
};

/// Exact, compile-time PostgreSQL preparation workload for one query set.
#[derive(Clone, Copy)]
pub struct Prepare<'a> {
    pub(super) wire: &'a [u8],
    pub(super) statements: u32,
    pub(super) groups: &'a [&'a [schema::QueryMeta<'a>]],
    pub(super) flags: u8,
}

const _: () = assert!(mem::size_of::<Prepare<'_>>() <= 5 * mem::size_of::<usize>());

const LTREE_PROBE_SQL: &str = "SELECT NULL::ltree";

const fn count(groups: &[&[schema::QueryMeta<'_>]]) -> Result<u32, encode::Error> {
    let mut total = 0u32;
    let mut group_i = 0;
    while group_i < groups.len() {
        let len = groups[group_i].len();
        if len > u32::MAX as usize {
            return Err(encode::Error::StatementCount {
                current: total,
                additional: len,
            });
        }
        let group = len as u32;
        let Some(next) = total.checked_add(group) else {
            return Err(encode::Error::StatementCount {
                current: total,
                additional: len,
            });
        };
        total = next;
        group_i += 1;
    }
    Ok(total)
}

const fn dynamic(groups: &[&[schema::QueryMeta<'_>]]) -> u8 {
    let mut dynamic = 0u8;
    let mut group_i = 0;
    while group_i < groups.len() {
        let group = groups[group_i];
        let mut query_i = 0;
        while query_i < group.len() {
            dynamic |= group[query_i].dynamic_types;
            query_i += 1;
        }
        group_i += 1;
    }
    dynamic
}

const fn details(groups: &[&[schema::QueryMeta<'_>]]) -> Result<(u32, u8), encode::Error> {
    if groups.len() > u32::MAX as usize {
        return Err(encode::Error::GroupCount {
            actual: groups.len(),
        });
    }
    let mut stmts = match count(groups) {
        Ok(stmts) => stmts,
        Err(error) => return Err(error),
    };
    if let Err(error) = unique(groups) {
        return Err(error);
    }
    let flags = dynamic(groups);
    if flags & desc::DYNAMIC_LTREE != 0 {
        let Some(total) = stmts.checked_add(1) else {
            return Err(encode::Error::StatementCount {
                current: stmts,
                additional: 1,
            });
        };
        stmts = total;
    }
    Ok((stmts, flags))
}

const fn unique(groups: &[&[schema::QueryMeta<'_>]]) -> Result<(), encode::Error> {
    let mut lg = 0;
    while lg < groups.len() {
        let mut lq = 0;
        while lq < groups[lg].len() {
            let name = groups[lg][lq].name;
            let mut rg = lg;
            let mut rq = lq + 1;
            while rg < groups.len() {
                while rq < groups[rg].len() {
                    if crate::sql::raw::Parts::new(&[name]).matches(groups[rg][rq].name) {
                        return Err(encode::Error::DuplicateStatement {
                            first: encode::Position {
                                group: lg as u32,
                                query: lq as u32,
                            },
                            second: encode::Position {
                                group: rg as u32,
                                query: rq as u32,
                            },
                        });
                    }
                    rq += 1;
                }
                rg += 1;
                rq = 0;
            }
            lq += 1;
        }
        lg += 1;
    }
    Ok(())
}

impl<'a> Prepare<'a> {
    pub const EMPTY: Self = Self {
        wire: encode::SYNC,
        statements: 0,
        groups: &[],
        flags: 0,
    };

    /// Computes the static wire bound; overflow fails compilation.
    pub const fn fixed_len<I: schema::QuerySet>() -> usize {
        const {
            match Self::try_wire_len(I::QUERIES) {
                Ok(length) => length,
                Err(_) => panic!("prepare"),
            }
        }
    }

    /// Encodes one static query set; invalid metadata fails compilation.
    pub const fn fixed<I: schema::QuerySet, const N: usize>() -> schema::Wire<'a, N> {
        const {
            match Self::try_build::<N>(I::QUERIES) {
                Ok(wire) => wire,
                Err(_) => panic!("prepare"),
            }
        }
    }

    pub const fn try_wire_len(groups: &[&[schema::QueryMeta<'_>]]) -> Result<usize, encode::Error> {
        let mut total = encode::SYNC.len();
        if dynamic(groups) & desc::DYNAMIC_LTREE != 0 {
            let lengths = match statement_lengths(schema::LTREE_PROBE_NAME, LTREE_PROBE_SQL, &[]) {
                Ok(lengths) => lengths,
                Err(error) => return Err(error),
            };
            total = match add(encode::Scope::Preparation, total, lengths.wire) {
                Ok(total) => total,
                Err(error) => return Err(error),
            };
        }
        let mut group_i = 0;
        while group_i < groups.len() {
            let group = groups[group_i];
            let mut query_i = 0;
            while query_i < group.len() {
                let query = &group[query_i];
                let lengths = match statement_lengths(query.name, query.sql, query.param_oids) {
                    Ok(lengths) => lengths,
                    Err(error) => return Err(error),
                };
                total = match add(encode::Scope::Preparation, total, lengths.wire) {
                    Ok(total) => total,
                    Err(error) => return Err(error),
                };
                query_i += 1;
            }
            group_i += 1;
        }
        Ok(total)
    }

    pub const fn try_build<const N: usize>(
        groups: &'a [&'a [schema::QueryMeta<'a>]],
    ) -> Result<schema::Wire<'a, N>, encode::Error> {
        let (stmts, flags) = match details(groups) {
            Ok(details) => details,
            Err(error) => return Err(error),
        };
        let expected = match Self::try_wire_len(groups) {
            Ok(expected) => expected,
            Err(error) => return Err(error),
        };
        if N != expected {
            return Err(encode::Error::PreparationSize {
                expected,
                actual: N,
            });
        }
        let mut out = [0u8; N];
        let mut at = 0;
        if flags & desc::DYNAMIC_LTREE != 0 {
            let lengths = match statement_lengths(schema::LTREE_PROBE_NAME, LTREE_PROBE_SQL, &[]) {
                Ok(lengths) => lengths,
                Err(error) => return Err(error),
            };
            write_statement(
                &mut out,
                &mut at,
                schema::LTREE_PROBE_NAME,
                LTREE_PROBE_SQL,
                &[],
                lengths,
            );
        }
        let mut group_i = 0;
        while group_i < groups.len() {
            let group = groups[group_i];
            let mut query_i = 0;
            while query_i < group.len() {
                let query = &group[query_i];
                let lengths = match statement_lengths(query.name, query.sql, query.param_oids) {
                    Ok(lengths) => lengths,
                    Err(error) => return Err(error),
                };
                write_query(&mut out, &mut at, query, lengths);
                query_i += 1;
            }
            group_i += 1;
        }
        write_slice(&mut out, &mut at, encode::SYNC);
        Ok(schema::Wire::new(out, stmts, groups, flags))
    }

    pub const fn wire(self) -> &'a [u8] {
        self.wire
    }

    pub const fn statements(self) -> u32 {
        self.statements
    }

    pub const fn wire_bytes(self) -> usize {
        self.wire.len()
    }

    pub(crate) const fn flags(self) -> u8 {
        self.flags
    }
}
