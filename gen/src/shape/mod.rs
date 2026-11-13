use crate::{
    backend,
    ext::{ExprExt as _, SqlIdent as _},
};
use std::borrow::Cow;
use syn::spanned::Spanned as _;

pub(super) mod bound;
pub(super) mod join;
pub(super) mod lock;
pub(super) mod mutation;
pub(super) mod qualify;

pub(super) enum QueryShape {
    Select {
        source: QuerySource,
        pred_idents: Vec<syn::Ident>,
        predicate: syn::Expr,
        projection: Option<Projection>,
        terminator: Terminator,
        qualifiers: qualify::State,
    },
    Mutation(mutation::Query),
    Aggregate(Box<Aggregate>),
    SetOp {
        set: SetQuery,
        terminator: Terminator,
        outer_qualifiers: qualify::State,
    },
}

impl QueryShape {
    pub(super) fn returning(self, ret: Returning, span: proc_macro2::Span) -> syn::Result<Self> {
        let Self::Mutation(query) = self else {
            return Err(syn::Error::new(
                span,
                "`.returning_*()` follows mutations only; use `.one() / .first() / .all() / .count()` after `.filter(...)`",
            ));
        };
        Ok(Self::Mutation(query.returning(ret, span)?))
    }

    pub(super) fn conflict(self, conflict: Conflict, span: proc_macro2::Span) -> syn::Result<Self> {
        let Self::Mutation(query) = self else {
            return Err(syn::Error::new(
                span,
                "ON CONFLICT clause follows `Table::insert(...)` / `Table::insert_each(...)` / `Table::insert_from(...)` only",
            ));
        };
        Ok(Self::Mutation(query.conflict(conflict, span)?))
    }
}

pub(super) struct Aggregate {
    pub(super) source: QuerySource,
    pub(super) pred_idents: Vec<syn::Ident>,
    pub(super) predicate: syn::Expr,
    pub(super) kind: Aggregation,
    pub(super) group_by: Option<AggCol>,
    pub(super) having: Option<HavingClause>,
    pub(super) qualifiers: qualify::State,
}

pub(super) struct SelectBranch {
    pub(super) source: QuerySource,
    pub(super) pred_idents: Vec<syn::Ident>,
    pub(super) predicate: syn::Expr,
}

pub(super) struct SetQuery {
    pub(super) first: SelectBranch,
    pub(super) links: Vec<(SetOpKind, SelectBranch)>,
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
    pub(super) fn sql_ref(self, dialect: &backend::Dialect) -> proc_macro2::TokenStream {
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
        Some(match name {
            "union" => Self::Union,
            "union_all" => Self::UnionAll,
            "intersect" => Self::Intersect,
            "intersect_all" => Self::IntersectAll,
            "except" => Self::Except,
            "except_all" => Self::ExceptAll,
            _ => return None,
        })
    }
}

pub(super) struct QuerySource {
    pub(super) primary_path: syn::Path,
    pub(super) primary_alias: Option<syn::Ident>,
    pub(super) joins: Vec<join::Join>,
    pub(super) alias_depth: usize,
}

impl QuerySource {
    pub(super) fn new(path: syn::Path) -> Self {
        Self {
            primary_path: path,
            primary_alias: None,
            joins: Vec::new(),
            alias_depth: 0,
        }
    }

    pub(super) fn qualifier(&self, index: usize) -> proc_macro2::TokenStream {
        let alias = format!("q{}_{index}", self.alias_depth);
        quote::quote! { #alias }
    }

    pub(super) fn declaration(
        &self,
        relation: proc_macro2::TokenStream,
        index: usize,
    ) -> [proc_macro2::TokenStream; 3] {
        [relation, quote::quote! { " AS " }, self.qualifier(index)]
    }

    pub(super) fn render_base(&self) -> [proc_macro2::TokenStream; 3] {
        let relation = match &self.primary_alias {
            Some(alias) => {
                let name = crate::ext::unraw(&alias.to_string()).quoted();
                quote::quote! { #name }
            }
            None => {
                let path = &self.primary_path;
                quote::quote! { <#path>::__CARTEL_TABLE }
            }
        };
        self.declaration(relation, 0)
    }

    pub(super) fn render_join(
        &self,
        index: usize,
        join: &join::Join,
    ) -> [proc_macro2::TokenStream; 3] {
        let path = &join.path;
        self.declaration(quote::quote! { <#path>::__CARTEL_TABLE }, index + 1)
    }

    pub(super) fn nested(mut self, parent: Option<usize>) -> Self {
        self.alias_depth = parent.map_or(0, |depth| depth + 1);
        self
    }
}

pub(super) struct CteBinding<'a> {
    pub(super) name: &'a syn::Ident,
    pub(super) inner_chain: &'a syn::Expr,
}

pub(super) struct Projection {
    pub(super) idents: Vec<syn::Ident>,
    pub(super) elems: Vec<syn::Expr>,
}

pub(super) struct HavingClause {
    pub(super) row_args: Vec<syn::Ident>,
    pub(super) agg_arg: syn::Ident,
    pub(super) pred: syn::Expr,
}

pub(super) enum Aggregation {
    Count,
    Column(AggregateFn, AggCol),
}

pub(super) struct AggCol {
    pub(super) args: Vec<syn::Ident>,
    pub(super) body: syn::Expr,
}

impl Aggregation {
    pub(super) fn parse(top: &syn::ExprMethodCall) -> syn::Result<Self> {
        if top.method == "count" {
            if !top.args.is_empty() {
                return Err(syn::Error::new(
                    top.args.span(),
                    "`.count()` takes no arguments",
                ));
            }
            return Ok(Self::Count);
        }
        let function = AggregateFn::new(&top.method).ok_or_else(|| {
            syn::Error::new(
                top.method.span(),
                format!("unknown aggregate method `.{}(...)`", top.method),
            )
        })?;
        if top.args.len() != 1 {
            return Err(syn::Error::new(
                top.args.span(),
                format!("`.{}(...)` takes one closure argument", top.method),
            ));
        }
        let (args, body) = top.args[0].closure_args(&top.method.to_string())?;
        Ok(Self::Column(function, AggCol { args, body }))
    }

    pub(super) fn function(&self) -> Option<(AggregateFn, &AggCol)> {
        match self {
            Self::Count => None,
            Self::Column(function, column) => Some((*function, column)),
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum AggregateFn {
    Sum,
    Avg,
    Min,
    Max,
}

impl AggregateFn {
    pub(super) fn new(ident: &syn::Ident) -> Option<Self> {
        if ident == "sum" {
            Some(Self::Sum)
        } else if ident == "avg" {
            Some(Self::Avg)
        } else if ident == "min" {
            Some(Self::Min)
        } else if ident == "max" {
            Some(Self::Max)
        } else {
            None
        }
    }

    pub(super) fn lower(&self) -> &str {
        match self {
            Self::Sum => "sum",
            Self::Avg => "avg",
            Self::Min => "min",
            Self::Max => "max",
        }
    }

    pub(super) fn upper(&self) -> &str {
        match self {
            Self::Sum => "SUM",
            Self::Avg => "AVG",
            Self::Min => "MIN",
            Self::Max => "MAX",
        }
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Returning {
    None,
    Required,
    One,
    First,
    All,
}

impl Returning {
    pub(super) fn from_method(name: &str) -> Option<Self> {
        Some(match name {
            "required" => Self::Required,
            "returning_one" => Self::One,
            "returning_first" => Self::First,
            "returning_all" => Self::All,
            _ => return None,
        })
    }

    pub(super) fn expected(self) -> Option<Terminator> {
        match self {
            Self::One => Some(Terminator::One),
            Self::First => Some(Terminator::First),
            Self::All => Some(Terminator::All),
            Self::None | Self::Required => None,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(super) enum Conflict {
    None,
    DoNothing,
    TargetDoNothing(Target),
    TargetDoUpdate(Target, Update),
}

#[derive(Clone, PartialEq, Eq)]
pub(super) struct Target {
    pub(super) cols: Vec<String>,
}

#[derive(Clone, PartialEq, Eq)]
pub(super) struct Update {
    pub(super) set_arg: syn::Ident,
    pub(super) set_body: syn::Expr,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Terminator {
    One,
    First,
    All,
    Stream,
}

impl Terminator {
    pub(super) fn method(&self) -> &str {
        match self {
            Self::One => "one",
            Self::First => "first",
            Self::All => "all",
            Self::Stream => "stream",
        }
    }

    pub(super) fn syntax(&self) -> &str {
        match self {
            Self::One => "T",
            Self::First => "Option<T>",
            Self::All => "Vec<T>",
            Self::Stream => "Stream<T>",
        }
    }
}
pub(super) struct Explicit<'a> {
    output: &'a syn::ReturnType,
    ty: &'a syn::Type,
    row: &'a syn::Type,
    kind: Terminator,
}

impl<'a> Explicit<'a> {
    pub(super) fn kind(&self) -> Terminator {
        self.kind
    }

    pub(super) fn row(&self) -> &syn::Type {
        self.row
    }

    pub(super) fn is_unit(&self) -> bool {
        if let syn::Type::Tuple(tuple) = self.ty {
            tuple.elems.is_empty()
        } else {
            false
        }
    }

    pub(super) fn scalar(&mut self) {
        self.row = self.ty;
    }

    pub(super) fn into_parts(self) -> (Cow<'a, syn::ReturnType>, &'a syn::Type, Terminator) {
        (Cow::Borrowed(self.output), self.row, self.kind)
    }
}

pub(super) enum ReturnShape<'a> {
    Default,
    Explicit(Explicit<'a>),
}

impl<'a> ReturnShape<'a> {
    pub(super) fn parse(output: &'a syn::ReturnType) -> Self {
        let syn::ReturnType::Type(_, ty) = output else {
            return Self::Default;
        };
        let (row, kind) = if let syn::Type::Path(path) = ty.as_ref()
            && path.qself.is_none()
            && let Some(seg) = path.path.segments.last()
            && (seg.ident == "Option" || seg.ident == "Vec" || seg.ident == "Stream")
            && let syn::PathArguments::AngleBracketed(args) = &seg.arguments
            && args.args.len() == 1
            && let syn::GenericArgument::Type(inner) = &args.args[0]
        {
            if seg.ident == "Option" {
                (inner, Terminator::First)
            } else if seg.ident == "Vec" {
                (inner, Terminator::All)
            } else {
                (inner, Terminator::Stream)
            }
        } else {
            (ty.as_ref(), Terminator::One)
        };
        Self::Explicit(Explicit {
            output,
            ty,
            row,
            kind,
        })
    }

    pub(super) fn is_unit(&self) -> bool {
        match self {
            Self::Default => true,
            Self::Explicit(explicit) => explicit.is_unit(),
        }
    }
}
