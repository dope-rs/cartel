use quote::quote;
use syn::{Expr, Ident, Path, Type};

use crate::backend::Dialect;

pub(super) enum QueryShape {
    Select {
        source: QuerySource,
        pred_idents: Vec<Ident>,
        predicate: Expr,
        projection: Option<ProjectionSpec>,
        terminator: Terminator,
        qualifiers: ChainQualifiers,
    },
    Update {
        source: QuerySource,
        pred_idents: Vec<Ident>,
        predicate: Expr,
        set_idents: Vec<Ident>,
        set_body: Expr,
        returning: ReturningKind,
    },
    Delete {
        source: QuerySource,
        pred_idents: Vec<Ident>,
        predicate: Expr,
        returning: ReturningKind,
    },
    UpdateEach {
        source: QuerySource,
        data_sources: Vec<Expr>,
        row_var: Ident,
        col_vars: Vec<Ident>,
        predicate: Expr,
        set_body: Expr,
        returning: ReturningKind,
    },
    Insert {
        table_path: Path,
        closure_arg: Ident,
        body: Expr,
        returning: ReturningKind,
        on_conflict: OnConflictKind,
    },
    InsertEach {
        table_path: Path,
        closure_arg: Ident,
        body: Expr,
        returning: ReturningKind,
        on_conflict: OnConflictKind,
    },
    InsertFrom {
        table_path: Path,
        source: QuerySource,
        source_pred_idents: Vec<Ident>,
        source_predicate: Expr,
        target_arg: Ident,
        source_idents: Vec<Ident>,
        body: Expr,
        returning: ReturningKind,
        on_conflict: OnConflictKind,
    },
    Aggregate(Box<Aggregate>),
    SetOp {
        branches: Vec<SelectBranch>,
        ops: Vec<SetOpKind>,
        terminator: Terminator,
        outer_qualifiers: ChainQualifiers,
    },
}

pub(super) struct Aggregate {
    pub(super) source: QuerySource,
    pub(super) pred_idents: Vec<Ident>,
    pub(super) predicate: Expr,
    pub(super) kind: AggregateKind,
    pub(super) group_by: Option<AggCol>,
    pub(super) having: Option<HavingClause>,
    pub(super) qualifiers: ChainQualifiers,
}

pub(super) struct SelectBranch {
    pub(super) source: QuerySource,
    pub(super) pred_idents: Vec<Ident>,
    pub(super) predicate: Expr,
}

#[derive(Clone, Copy)]
pub(super) enum SetOpKind {
    Union,
    UnionAll,
    Intersect,
    IntersectAll,
    Except,
    ExceptAll,
}

impl SetOpKind {
    pub(super) fn sql_ref(self, dialect: &Dialect) -> proc_macro2::TokenStream {
        match self {
            Self::Union => dialect.kw("UNION_KW"),
            Self::UnionAll => dialect.kw("UNION_ALL_KW"),
            Self::Intersect => dialect.kw("INTERSECT_KW"),
            Self::IntersectAll => dialect.kw("INTERSECT_ALL_KW"),
            Self::Except => dialect.kw("EXCEPT_KW"),
            Self::ExceptAll => dialect.kw("EXCEPT_ALL_KW"),
        }
    }

    pub(super) fn from_method(name: &str) -> Option<Self> {
        match name {
            "union" => Some(Self::Union),
            "union_all" => Some(Self::UnionAll),
            "intersect" => Some(Self::Intersect),
            "intersect_all" => Some(Self::IntersectAll),
            "except" => Some(Self::Except),
            "except_all" => Some(Self::ExceptAll),
            _ => None,
        }
    }
}

pub(super) struct QuerySource {
    pub(super) primary_path: Path,
    pub(super) primary_alias: Option<Ident>,
    pub(super) joins: Vec<JoinSpec>,
}

impl QuerySource {
    pub(super) fn render_primary_table(&self) -> Vec<proc_macro2::TokenStream> {
        match &self.primary_alias {
            Some(alias) => {
                let s = alias.to_string();
                vec![quote! { #s }]
            }
            None => {
                let p = &self.primary_path;
                vec![quote! { <#p>::__CARTEL_TABLE }]
            }
        }
    }
}

pub(super) struct CteBinding {
    pub(super) name: Ident,
    pub(super) inner_chain: Expr,
}

pub(super) struct ProjectionSpec {
    pub(super) idents: Vec<Ident>,
    pub(super) elems: Vec<Expr>,
}

pub(super) struct JoinSpec {
    pub(super) kind: JoinKind,
    pub(super) path: Path,
    pub(super) on_idents: Vec<Ident>,
    pub(super) cond: Expr,
}

#[derive(Clone, Copy)]
pub(super) enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
    LateralInner,
    LateralLeft,
}

impl JoinKind {
    pub(super) fn sql_ref(self, dialect: &Dialect) -> proc_macro2::TokenStream {
        match self {
            Self::Inner => dialect.kw("INNER_JOIN"),
            Self::Left => dialect.kw("LEFT_OUTER_JOIN"),
            Self::Right => dialect.kw("RIGHT_OUTER_JOIN"),
            Self::Full => dialect.kw("FULL_OUTER_JOIN"),
            Self::LateralInner => dialect.kw("INNER_JOIN_LATERAL"),
            Self::LateralLeft => dialect.kw("LEFT_JOIN_LATERAL"),
        }
    }

    pub(super) fn is_lateral(self) -> bool {
        matches!(self, Self::LateralInner | Self::LateralLeft)
    }

    pub(super) fn from_method(method: &Ident) -> syn::Result<Self> {
        match method.to_string().as_str() {
            "join" => Ok(Self::Inner),
            "left_join" => Ok(Self::Left),
            "right_join" => Ok(Self::Right),
            "full_join" => Ok(Self::Full),
            "lateral_join" => Ok(Self::LateralInner),
            "lateral_left_join" => Ok(Self::LateralLeft),
            other => Err(syn::Error::new(
                method.span(),
                format!(
                    "expected `.join` / `.left_join` / `.right_join` / `.full_join` / `.lateral_join` / `.lateral_left_join`, got `.{other}`",
                ),
            )),
        }
    }
}

pub(super) struct HavingClause {
    pub(super) row_args: Vec<Ident>,
    pub(super) agg_arg: Ident,
    pub(super) pred: Expr,
}

pub(super) enum AggregateKind {
    Count,
    Sum(AggCol),
    Avg(AggCol),
    Min(AggCol),
    Max(AggCol),
}

pub(super) struct AggCol {
    pub(super) args: Vec<Ident>,
    pub(super) body: Expr,
}

impl AggregateKind {
    pub(super) fn function(&self) -> Option<(&'static str, &AggCol)> {
        match self {
            Self::Count => None,
            Self::Sum(column) => Some(("SUM", column)),
            Self::Avg(column) => Some(("AVG", column)),
            Self::Min(column) => Some(("MIN", column)),
            Self::Max(column) => Some(("MAX", column)),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ReturningKind {
    None,
    One,
    First,
    All,
}

impl ReturningKind {
    pub(super) fn from_method(name: &str) -> Option<Self> {
        match name {
            "returning_one" => Some(Self::One),
            "returning_first" => Some(Self::First),
            "returning_all" => Some(Self::All),
            _ => None,
        }
    }

    pub(super) fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::One => "one",
            Self::First => "first",
            Self::All => "all",
        }
    }

    pub(super) fn expected_return_type(self) -> &'static str {
        match self {
            Self::One => "T",
            Self::First => "Option<T>",
            Self::All => "Vec<T>",
            Self::None => "()",
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(super) enum OnConflictKind {
    None,
    DoNothing,
    TargetDoNothing(ConflictTarget),
    TargetDoUpdate(ConflictTarget, ConflictUpdate),
}

#[derive(Clone, PartialEq, Eq)]
pub(super) struct ConflictTarget {
    pub(super) cols: Vec<String>,
}

#[derive(Clone, PartialEq, Eq)]
pub(super) struct ConflictUpdate {
    pub(super) set_arg: Ident,
    pub(super) set_body: Expr,
}

pub(super) struct OrderClause {
    pub(super) closure: Expr,
    pub(super) dir: OrderDir,
}

#[derive(Clone, Copy)]
pub(super) enum OrderDir {
    Asc,
    Desc,
}

#[derive(Default)]
pub(super) struct ChainQualifiers {
    pub(super) order_by: Vec<OrderClause>,
    pub(super) limit: Option<Expr>,
    pub(super) offset: Option<Expr>,
    pub(super) distinct: DistinctKind,
    pub(super) lock: LockKind,
}

#[derive(Default)]
pub(super) enum DistinctKind {
    #[default]
    None,
    All,
    On(Ident, Expr),
}

#[derive(Default, Clone, Copy)]
pub(super) enum LockKind {
    #[default]
    None,
    ForUpdate,
    ForShare,
}

impl LockKind {
    pub(super) fn render(self, dialect: &Dialect) -> Vec<proc_macro2::TokenStream> {
        match self {
            Self::None => Vec::new(),
            Self::ForUpdate => vec![dialect.kw("FOR_UPDATE")],
            Self::ForShare => vec![dialect.kw("FOR_SHARE")],
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum Terminator {
    One,
    First,
    All,
    Stream,
}

impl Terminator {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::One => "one",
            Self::First => "first",
            Self::All => "all",
            Self::Stream => "stream",
        }
    }

    pub(super) fn expected_return(self) -> &'static str {
        match self {
            Self::One => "T",
            Self::First => "Option<T>",
            Self::All => "Vec<T>",
            Self::Stream => "Stream<T>",
        }
    }
}

pub(super) enum ReturnShape {
    Plain(Type),
    Optional(Type),
    Many(Type),
    Stream(Type),
}

impl ReturnShape {
    pub(super) fn parse(ty: &Type) -> Self {
        if let Type::Path(syn::TypePath {
            path, qself: None, ..
        }) = ty
            && let Some(seg) = path.segments.last()
            && (seg.ident == "Option" || seg.ident == "Vec" || seg.ident == "Stream")
            && let syn::PathArguments::AngleBracketed(args) = &seg.arguments
            && args.args.len() == 1
            && let syn::GenericArgument::Type(inner) = &args.args[0]
        {
            return match seg.ident.to_string().as_str() {
                "Option" => Self::Optional(inner.clone()),
                "Vec" => Self::Many(inner.clone()),
                "Stream" => Self::Stream(inner.clone()),
                _ => unreachable!(),
            };
        }
        Self::Plain(ty.clone())
    }

    pub(super) fn row_ty(&self) -> &Type {
        match self {
            Self::Plain(t) | Self::Optional(t) | Self::Many(t) | Self::Stream(t) => t,
        }
    }

    pub(super) fn describe_actual(&self) -> &'static str {
        match self {
            Self::Plain(_) => "T",
            Self::Optional(_) => "Option<T>",
            Self::Many(_) => "Vec<T>",
            Self::Stream(_) => "Stream<T>",
        }
    }
}
