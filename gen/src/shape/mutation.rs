use crate::ext::ExprExt as _;
use crate::ext::PathExt as _;
use crate::shape;

use syn::spanned::Spanned as _;

pub(crate) struct Query {
    pub(crate) kind: Mutation,
    pub(crate) returning: shape::Returning,
}

pub(crate) enum Mutation {
    Insert(Insert),
    Update(Update),
    Delete(Delete),
    Each(Each),
}

pub(crate) struct Insert {
    pub(crate) input: Input,
    pub(crate) conflict: shape::Conflict,
}

pub(crate) enum Input {
    One(Values),
    Each(Values),
    From(Box<From>),
}

pub(crate) struct Values {
    pub(crate) table: syn::Path,
    pub(crate) arg: syn::Ident,
    pub(crate) body: Box<syn::Expr>,
}

pub(crate) struct From {
    pub(crate) table: syn::Path,
    pub(crate) source: shape::QuerySource,
    pub(crate) pred_idents: Vec<syn::Ident>,
    pub(crate) predicate: Box<syn::Expr>,
    pub(crate) target: syn::Ident,
    pub(crate) source_idents: Vec<syn::Ident>,
    pub(crate) body: Box<syn::Expr>,
}

pub(crate) struct Update {
    pub(crate) source: shape::QuerySource,
    pub(crate) pred_idents: Vec<syn::Ident>,
    pub(crate) predicate: Box<syn::Expr>,
    pub(crate) set_arg: syn::Ident,
    pub(crate) body: Box<syn::Expr>,
}

pub(crate) struct Delete {
    pub(crate) source: shape::QuerySource,
    pub(crate) pred_idents: Vec<syn::Ident>,
    pub(crate) predicate: Box<syn::Expr>,
}

pub(crate) struct Each {
    pub(crate) source: shape::QuerySource,
    pub(crate) data: Vec<syn::Expr>,
    pub(crate) row: syn::Ident,
    pub(crate) cols: Vec<syn::Ident>,
    pub(crate) predicate: Box<syn::Expr>,
    pub(crate) body: Box<syn::Expr>,
}

impl Query {
    pub(crate) fn new(kind: Mutation) -> Self {
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
        conflict: shape::Conflict,
        span: proc_macro2::Span,
    ) -> syn::Result<Self> {
        let Mutation::Insert(insert) = &mut self.kind else {
            return Err(syn::Error::new(
                span,
                "ON CONFLICT clause follows `Table::insert(...)` / `Table::insert_each(...)` / `Table::insert_from(...)` only",
            ));
        };
        if insert.conflict != shape::Conflict::None {
            return Err(syn::Error::new(span, "duplicate ON CONFLICT clause"));
        }
        insert.conflict = conflict;
        Ok(self)
    }

    pub(crate) fn try_each(receiver: &syn::Expr, update: &syn::Expr) -> syn::Result<Option<Self>> {
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
        let table = function.path.without_last()?;
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
        let (pred_ids, pred) = args[1].closure_n(arity, "filter_each")?;
        let body = update.closure_same(&pred_ids, "update")?;
        let source = shape::QuerySource::new(table);
        let data = match &args[0] {
            syn::Expr::Tuple(tuple) => tuple.elems.iter().cloned().collect(),
            single => vec![single.clone()],
        };
        let row = pred_ids[0].clone();
        let cols = pred_ids[1..].to_vec();
        Ok(Some(Self::new(Mutation::Each(Each {
            source,
            data,
            row,
            cols,
            predicate: Box::new(pred),
            body: Box::new(body),
        }))))
    }
}
