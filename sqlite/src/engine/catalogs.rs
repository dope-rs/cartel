use std::marker;

/// One SQL program in a connection's compile-time statement catalog.
#[derive(Clone, Copy)]
pub struct Entry<'sql> {
    sql: &'sql str,
    parameters: usize,
    columns: usize,
}

impl<'sql> Entry<'sql> {
    #[doc(hidden)]
    pub const fn of<Q: TypedQuery>() -> Self {
        Self {
            sql: Q::SQL,
            parameters: Q::PARAMETERS,
            columns: Q::COLUMNS,
        }
    }

    pub const fn sql(self) -> &'sql str {
        self.sql
    }

    pub const fn parameters(self) -> usize {
        self.parameters
    }

    pub const fn columns(self) -> usize {
        self.columns
    }
}

/// Closed SQL program set accepted by a database.
pub trait Catalog {
    const STATEMENTS: &[Entry<'_>];
}

/// Exact static SQL shape of a catalog.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Shape {
    pub statements: usize,
    pub max_sql_bytes: usize,
}

impl Shape {
    pub const fn of<I: Catalog>() -> Self {
        let entries = I::STATEMENTS;
        let mut stmt: usize = 0;
        let mut max_sql: usize = 0;
        while stmt < entries.len() {
            let bytes = entries[stmt].sql().len();
            if max_sql < bytes {
                max_sql = bytes;
            }
            stmt += 1;
        }
        Self {
            statements: entries.len(),
            max_sql_bytes: max_sql,
        }
    }
}

pub(crate) fn validate<I: Catalog>() -> Result<(), crate::Error> {
    for (stmt, entry) in I::STATEMENTS.iter().enumerate() {
        let sql = entry.sql();
        if sql.trim().is_empty() || sql.as_bytes().contains(&0) {
            return Err(crate::Error::CatalogStatementSql { statement: stmt });
        }
    }
    Ok(())
}

/// Empty catalog used by connections that execute only transaction controls.
pub struct Empty;

impl Catalog for Empty {
    const STATEMENTS: &[Entry<'_>] = &[];
}

#[doc(hidden)]
pub trait TypedQuery {
    const SQL: &str;
    const PARAMETERS: usize;
    const COLUMNS: usize;
}

#[doc(hidden)]
pub trait HasQuery<Q: TypedQuery>: Catalog {
    const SLOT: usize;
}

/// Dense, catalog-branded statement selector.
#[doc(hidden)]
pub struct Statement<I: Catalog, Q: TypedQuery> {
    marker: marker::PhantomData<(I, Q)>,
}

enum Mismatch {
    Slot,
    Sql,
    Parameters,
    Columns,
}

impl<I: HasQuery<Q>, Q: TypedQuery> Statement<I, Q> {
    /// Admits the exact catalog entry; invalid mappings fail compilation.
    #[doc(hidden)]
    pub const fn of() -> Self {
        const {
            match Self::try_new() {
                Ok(stmt) => stmt,
                Err(_) => panic!("statement"),
            }
        }
    }

    const fn try_new() -> Result<Self, Mismatch> {
        let slot = <I as HasQuery<Q>>::SLOT;
        if slot >= I::STATEMENTS.len() {
            return Err(Mismatch::Slot);
        }
        let entry = I::STATEMENTS[slot];
        if !crate::sql::raw::Parts::new(&[entry.sql]).matches(Q::SQL) {
            return Err(Mismatch::Sql);
        }
        if entry.parameters != Q::PARAMETERS {
            return Err(Mismatch::Parameters);
        }
        if entry.columns != Q::COLUMNS {
            return Err(Mismatch::Columns);
        }
        Ok(Self {
            marker: marker::PhantomData,
        })
    }

    pub(crate) const fn slot(self) -> usize {
        <I as HasQuery<Q>>::SLOT
    }
}

impl<I: Catalog, Q: TypedQuery> Copy for Statement<I, Q> {}

impl<I: Catalog, Q: TypedQuery> Clone for Statement<I, Q> {
    fn clone(&self) -> Self {
        *self
    }
}
