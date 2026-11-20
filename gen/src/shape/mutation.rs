use crate::ext::ExprExt as _;
use crate::ext::PathExt as _;
use crate::shape;

use syn::spanned::Spanned as _;

pub(crate) struct Query<'a> {
    pub(crate) kind: Mutation<'a>,
    pub(crate) returning: shape::Returning,
}

pub(crate) enum Mutation<'a> {
    Insert(Insert<'a>),
    Update(Update<'a>),
    Delete(shape::Selection<'a>),
    Each(Each<'a>),
}

pub(crate) struct Insert<'a> {
    pub(crate) input: Input<'a>,
    pub(crate) conflict: shape::Conflict<'a>,
}

pub(crate) enum Input<'a> {
    One(Values<'a>),
    Each(Values<'a>),
    From(Box<From<'a>>),
}

pub(crate) struct Values<'a> {
    pub(crate) table: syn::Path,
    pub(crate) arg: &'a syn::Ident,
    pub(crate) body: &'a syn::Expr,
}

pub(crate) struct From<'a> {
    pub(crate) table: syn::Path,
    pub(crate) selection: shape::Selection<'a>,
    pub(crate) target: &'a syn::Ident,
    pub(crate) source: &'a syn::Ident,
    pub(crate) body: &'a syn::Expr,
}

pub(crate) struct Update<'a> {
    pub(crate) selection: shape::Selection<'a>,
    pub(crate) set_arg: &'a syn::Ident,
    pub(crate) body: &'a syn::Expr,
}

pub(crate) struct Each<'a> {
    pub(crate) source: shape::QuerySource<'a>,
    pub(crate) data: Vec<&'a syn::Expr>,
    pub(crate) row: syn::Ident,
    pub(crate) cols: Vec<syn::Ident>,
    pub(crate) predicate: &'a syn::Expr,
    pub(crate) body: &'a syn::Expr,
}

impl<'a> Query<'a> {
    pub(crate) fn new(kind: Mutation<'a>) -> Self {
        Self {
            kind,
            returning: shape::Returning::None,
        }
    }

    pub(crate) fn returning(
        mut self,
        ret: shape::Returning,
        span: proc_macro2::Span,
    ) -> syn::Result<Self> {
        if self.returning != shape::Returning::None {
            return Err(syn::Error::new(
                span,
                "only one mutation outcome is allowed",
            ));
        }
        self.returning = ret;
        Ok(self)
    }

    pub(crate) fn conflict(
        mut self,
        conflict: shape::Conflict<'a>,
        span: proc_macro2::Span,
    ) -> syn::Result<Self> {
        let Mutation::Insert(insert) = &mut self.kind else {
            return Err(syn::Error::new(
                span,
                "ON CONFLICT clause follows `Table::insert(...)` / `Table::insert_each(...)` / `Table::insert_from(...)` only",
            ));
        };
        if self.returning != shape::Returning::None {
            return Err(syn::Error::new(
                span,
                "ON CONFLICT precedes the mutation outcome",
            ));
        }
        if !matches!(insert.conflict, shape::Conflict::None) {
            return Err(syn::Error::new(span, "duplicate ON CONFLICT clause"));
        }
        insert.conflict = conflict;
        Ok(self)
    }

    pub(crate) fn try_each(
        receiver: &'a syn::Expr,
        update: &'a syn::Expr,
    ) -> syn::Result<Option<Self>> {
        let syn::Expr::Call(call) = receiver else {
            return Ok(None);
        };
        let syn::Expr::Path(function) = call.func.as_ref() else {
            return Ok(None);
        };
        let Some(last) = function.path.segments.last() else {
            return Ok(None);
        };
        if last.ident != "filter_each" {
            return Ok(None);
        }
        if let Some(attr) = call.attrs.first() {
            return Err(syn::Error::new(
                attr.span(),
                "attributes are not supported on a filter_each call",
            ));
        }
        if let Some(attr) = function.attrs.first() {
            return Err(syn::Error::new(
                attr.span(),
                "attributes are not supported on a filter_each path",
            ));
        }
        if let Some(qself) = &function.qself {
            return Err(syn::Error::new(
                qself.span(),
                "filter_each must be called through an unqualified Table path",
            ));
        }
        let args = &call.args;
        if args.len() != 2 {
            return Err(syn::Error::new(
                args.span(),
                "`Table::filter_each((cols...), |row, c0...| pred)` takes exactly two arguments",
            ));
        }
        let table = function.path.table()?;
        let count = if let syn::Expr::Tuple(tuple) = &args[0] {
            tuple.elems.len()
        } else {
            1
        };
        if count == 0 {
            return Err(syn::Error::new(
                args[0].span(),
                "`filter_each` first argument must list at least one `&[T]` column source",
            ));
        }
        let arity = 1 + count;
        let pred = args[1].binding(Some(arity), "filter_each")?;
        let body = update.closure_same(&pred.idents, "update")?;
        let source = shape::QuerySource::new(table);
        let data = match &args[0] {
            syn::Expr::Tuple(tuple) => tuple.elems.iter().collect(),
            single => vec![single],
        };
        let row = pred.idents[0].clone();
        let cols = pred.idents[1..].to_vec();
        Ok(Some(Self::new(Mutation::Each(Each {
            source,
            data,
            row,
            cols,
            predicate: pred.body,
            body,
        }))))
    }
}
