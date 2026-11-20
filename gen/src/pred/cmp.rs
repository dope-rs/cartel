use crate::backend;
use crate::emit::token::{EQ, fragment};

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
    pub(super) fn sql_ref(self) -> proc_macro2::TokenStream {
        fragment(match self {
            Self::Eq => EQ,
            Self::Ne => " <> ",
            Self::Lt => " < ",
            Self::Gt => " > ",
            Self::Le => " <= ",
            Self::Ge => " >= ",
            Self::Like => " LIKE ",
            Self::ILike => " ILIKE ",
            Self::NotLike => " NOT LIKE ",
            Self::NotILike => " NOT ILIKE ",
            Self::Glob => " GLOB ",
            Self::Contains | Self::LtreeAncOf => " @> ",
            Self::Overlaps => " && ",
            Self::RegexMatch => " ~ ",
            Self::RegexIMatch => " ~* ",
            Self::NotRegexMatch => " !~ ",
            Self::NotRegexIMatch => " !~* ",
            Self::FtsMatch => " @@ ",
            Self::LtreeDescOf => " <@ ",
        })
    }

    pub(super) fn capture(self, capture: backend::Capture, sql: &mut crate::pred::sql::Sql) {
        sql.capture(capture);
        if matches!(self, Self::LtreeDescOf | Self::LtreeAncOf) {
            sql.push(fragment("::ltree"));
        }
    }
}
