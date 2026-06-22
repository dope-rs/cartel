pub mod dsl;
pub mod sql;

pub use cartel_gen::{SqliteTable, sqlite_query};
pub use dsl::{
    AggBuilder, AggHandle, ConflictTarget, Cte, DeleteBuilder, FilterBuilder, InsertBuilder,
    JoinBuilder, JoinBuilder2, JoinBuilder3, JoinBuilder4, Joined2Filter, Joined3Filter,
    Joined4Filter, RowIter, SelectBuilder, SourceRow, UpdateBuilder, WindowExpr, WindowSpec, avg,
    count, dense_rank, exists, max, min, not_exists, rank, row_number, sum,
};
pub use rusqlite::types::{FromSql, ToSqlOutput, Value, ValueRef};
pub use rusqlite::{
    self, CachedStatement, Connection, Error, Params, Result, Row, Rows, Statement, ToSql,
    Transaction, params, params_from_iter,
};

mod decode;
pub use decode::Decode;

#[doc(hidden)]
pub mod __internal {
    pub const fn concat_len(parts: &[&str]) -> usize {
        let mut total = 0;
        let mut i = 0;
        while i < parts.len() {
            total += parts[i].len();
            i += 1;
        }
        total
    }

    pub const fn concat<const N: usize>(parts: &[&str]) -> [u8; N] {
        let mut buf = [0u8; N];
        let mut bi = 0;
        let mut pi = 0;
        while pi < parts.len() {
            let bytes = parts[pi].as_bytes();
            let mut i = 0;
            while i < bytes.len() {
                buf[bi] = bytes[i];
                bi += 1;
                i += 1;
            }
            pi += 1;
        }
        buf
    }

    pub const fn hash_sql(s: &str) -> u64 {
        let bytes = s.as_bytes();
        let mut h: u64 = 0xcbf29ce484222325;
        let mut i = 0;
        while i < bytes.len() {
            h ^= bytes[i] as u64;
            h = h.wrapping_mul(0x100000001b3);
            i += 1;
        }
        h
    }
}
