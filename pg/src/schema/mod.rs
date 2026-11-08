use crate::protocol::{encode::Error, value::encode};
use std::{any::Any, marker::PhantomData, mem};

pub mod array;
#[doc(hidden)]
pub mod desc;
mod project;
mod row;

pub mod result;

#[doc(hidden)]
pub mod groups;
mod prepare;
mod wire;

#[doc(hidden)]
pub use prepare::Prepare;
pub(crate) use prepare::catalog::{Catalog, Cursor};
pub use project::Project;
pub use row::Row;
#[doc(hidden)]
pub use wire::Wire;

const LTREE_PROBE_NAME: &str = "__cartel_type_ltree";

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct QueryMeta<'a> {
    pub(crate) name: &'a str,
    pub(crate) sql: &'a str,
    pub(crate) param_oids: &'a [u32],
    pub(crate) result_columns: u16,
    pub(crate) result: result::Kind,
    pub(crate) dynamic_types: u8,
}

const _: () = assert!(mem::size_of::<QueryMeta<'_>>() == 7 * mem::size_of::<usize>());

impl QueryMeta<'_> {
    /// Constructs static query metadata; invalid declarations fail compilation.
    pub const fn fixed<Q: TypedQuery>() -> Self {
        const {
            match Self::try_new::<Q>() {
                Ok(metadata) => metadata,
                Err(_) => panic!("metadata"),
            }
        }
    }

    #[doc(hidden)]
    pub const fn try_new<Q: TypedQuery>() -> Result<Self, Error> {
        match const { Self::check::<Q>() } {
            Ok(()) => {}
            Err(error) => return Err(error),
        }
        let result = <Q::Result as result::Shape>::KIND;
        Ok(Self {
            name: Q::STATEMENT_NAME,
            sql: Q::SQL,
            param_oids: Q::PARAM_OIDS,
            result_columns: Q::N_RESULT_COLS,
            result,
            dynamic_types: <Q::Row as desc::Validate>::DYNAMIC_TYPES,
        })
    }

    pub(crate) fn validate<Q: TypedQuery>() -> Result<Admission<Q>, Error> {
        const { Self::check::<Q>() }?;
        let mut formats = desc::Formats::new(Q::RESULT_FORMAT_CODES, Q::N_RESULT_COLS);
        <Q::Row as desc::Validate>::formats(&mut formats)?;
        formats.finish()?;
        Ok(Admission(PhantomData))
    }

    const fn check<Q: TypedQuery>() -> Result<(), Error> {
        let result = <Q::Result as result::Shape>::KIND;
        if matches!(result, result::Kind::Rows) != (Q::N_RESULT_COLS != 0)
            || (<Q::Result as result::Shape>::SCALAR && Q::N_RESULT_COLS != 1)
        {
            return Err(Error::ResultShape {
                kind: result,
                columns: Q::N_RESULT_COLS,
            });
        }
        if Q::STATEMENT_NAME.is_empty() {
            return Err(Error::StatementNameEmpty);
        }
        if crate::sql::raw::Parts::new(&[Q::STATEMENT_NAME]).matches(LTREE_PROBE_NAME) {
            return Err(Error::StatementNameReserved);
        }
        if let Some(position) = nul(Q::STATEMENT_NAME) {
            return Err(Error::StatementNameNul { position });
        }
        if let Some(position) = nul(Q::SQL) {
            return Err(Error::StatementSqlNul { position });
        }
        if Q::N_RESULT_COLS > i16::MAX as u16 {
            return Err(Error::ResultCount {
                declared: Q::N_RESULT_COLS,
            });
        }
        if Q::N_PARAMS < 0 || Q::N_PARAMS as usize != Q::PARAM_OIDS.len() {
            return Err(Error::ParameterCount {
                declared: Q::N_PARAMS,
                types: Q::PARAM_OIDS.len(),
            });
        }
        let params = Q::N_PARAMS as usize;
        if let Err(error) = Self::check_codes(Q::PARAM_FORMAT_CODES, params, FormatKind::Parameter)
        {
            return Err(error);
        }
        if let Err(error) = Self::check_codes(
            Q::RESULT_FORMAT_CODES,
            Q::N_RESULT_COLS as usize,
            FormatKind::Result,
        ) {
            return Err(error);
        }
        Ok(())
    }

    const fn check_codes(formats: &[u16], expected: usize, kind: FormatKind) -> Result<(), Error> {
        if !is_format_count(formats.len(), expected) {
            return Err(match kind {
                FormatKind::Parameter => Error::ParameterFormatCount {
                    actual: formats.len(),
                    expected,
                },
                FormatKind::Result => Error::ResultFormatCount {
                    actual: formats.len(),
                    expected,
                },
            });
        }
        let mut index = 0;
        while index < formats.len() {
            let actual = formats[index];
            if actual > 1 {
                return Err(match kind {
                    FormatKind::Parameter => Error::ParameterFormat { index, actual },
                    FormatKind::Result => Error::ResultFormat { index, actual },
                });
            }
            index += 1;
        }
        Ok(())
    }

    pub(crate) fn matches<Q: TypedQuery>(self) -> bool {
        self.name == Q::STATEMENT_NAME
            && self.sql == Q::SQL
            && self.param_oids == Q::PARAM_OIDS
            && self.result_columns == Q::N_RESULT_COLS
            && self.result == <Q::Result as result::Shape>::KIND
            && self.dynamic_types == <Q::Row as desc::Validate>::DYNAMIC_TYPES
    }
}

/// The parameter count and format relation checked for this exact query type.
#[repr(transparent)]
pub(crate) struct Admission<Q>(PhantomData<fn(Q) -> Q>);

impl<Q: TypedQuery> Admission<Q> {
    pub(crate) const fn count(&self) -> i16 {
        Q::N_PARAMS
    }
}

const _: () = assert!(mem::size_of::<Admission<()>>() == 0);

#[derive(Clone, Copy)]
enum FormatKind {
    Parameter,
    Result,
}

const fn is_format_count(actual: usize, values: usize) -> bool {
    actual <= 1 || actual == values
}

const fn nul(value: &str) -> Option<usize> {
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == 0 {
            return Some(index);
        }
        index += 1;
    }
    None
}

pub trait TypedQuery: Any {
    type Params<'p>;
    type Row: Row;
    type Result: result::Shape;
    type Group;

    const STATEMENT_NAME: &str;
    const SQL: &str;
    const PARAM_OIDS: &[u32];
    const N_PARAMS: i16;
    const N_RESULT_COLS: u16;

    const PARAM_FORMAT_CODES: &[u16] = &[1];
    const RESULT_FORMAT_CODES: &[u16] = &[1];

    fn encode(params: Self::Params<'_>, w: &mut encode::BindWriter<'_, '_>) -> Result<(), Error>;
}

pub trait QueryGroup {
    #[doc(hidden)]
    type Items: groups::Items;

    const QUERIES: &[QueryMeta<'_>];
}

pub trait HasGroup<G: QueryGroup> {}

pub trait QuerySet {
    #[doc(hidden)]
    type Groups: groups::List;

    const QUERIES: &[&[QueryMeta<'_>]] = &[];

    const PREPARE: Prepare<'_>;
}

pub(crate) fn contains<I: QuerySet, Q: TypedQuery>() -> bool {
    <I::Groups as groups::List>::contains::<Q>()
}

pub(crate) enum Step<I> {
    Query(Cursor<I>),
    Complete,
}

impl<I> Copy for Step<I> {}

impl<I> Clone for Step<I> {
    fn clone(&self) -> Self {
        *self
    }
}
