#[derive(Clone, Copy)]
pub(crate) enum Cmp {
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
    Like,
    ILike,
    NotLike,
    NotILike,
    Glob,
    Contains,
    Overlaps,
    RegexMatch,
    RegexIMatch,
    NotRegexMatch,
    NotRegexIMatch,
    FtsMatch,
    LtreeDescOf,
    LtreeAncOf,
}

impl Cmp {
    pub(super) fn sql_ref(self, dialect: &backend::Dialect) -> proc_macro2::TokenStream {
        match self {
            Self::Eq => dialect.kw("EQ"),
            Self::Ne => dialect.kw("NE"),
            Self::Lt => dialect.kw("LT"),
            Self::Gt => dialect.kw("GT"),
            Self::Le => dialect.kw("LE"),
            Self::Ge => dialect.kw("GE"),
            Self::Like => dialect.kw("LIKE"),
            Self::ILike => dialect.kw("ILIKE"),
            Self::NotLike => dialect.kw("NOT_LIKE"),
            Self::NotILike => dialect.kw("NOT_ILIKE"),
            Self::Glob => dialect.kw("GLOB"),
            Self::Contains => dialect.kw("CONTAINS"),
            Self::Overlaps => dialect.kw("OVERLAPS"),
            Self::RegexMatch => dialect.kw("REGEX_MATCH"),
            Self::RegexIMatch => dialect.kw("REGEX_IMATCH"),
            Self::NotRegexMatch => dialect.kw("NOT_REGEX_MATCH"),
            Self::NotRegexIMatch => dialect.kw("NOT_REGEX_IMATCH"),
            Self::FtsMatch => dialect.kw("FTS_MATCH"),
            Self::LtreeDescOf => dialect.kw("LTREE_DESC_OF"),
            Self::LtreeAncOf => dialect.kw("LTREE_ANC_OF"),
        }
    }

    pub(super) fn capture(
        self,
        ident: syn::Ident,
        sql: &mut crate::pred::sql::Sql,
        dialect: &backend::Dialect,
    ) {
        sql.capture(ident);
        match self {
            Self::LtreeDescOf | Self::LtreeAncOf => sql.push(dialect.kw("CAST_LTREE_SUFFIX")),
            Self::Eq
            | Self::Ne
            | Self::Lt
            | Self::Gt
            | Self::Le
            | Self::Ge
            | Self::Like
            | Self::ILike
            | Self::NotLike
            | Self::NotILike
            | Self::Glob
            | Self::Contains
            | Self::Overlaps
            | Self::RegexMatch
            | Self::RegexIMatch
            | Self::NotRegexMatch
            | Self::NotRegexIMatch
            | Self::FtsMatch => {}
        }
    }
}
use crate::backend;
