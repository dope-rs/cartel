use crate::pred::{self, cmp, expr, sql};
use expr::Expr as _;
use syn::spanned::Spanned as _;

pub(super) enum Op<'a> {
    And,
    Or,
    Compare(cmp::Cmp),
    Unsupported(&'a syn::BinOp),
}

impl<'a> Op<'a> {
    fn new(operator: &'a syn::BinOp) -> Self {
        if operator == &syn::BinOp::And(Default::default()) {
            Self::And
        } else if operator == &syn::BinOp::Or(Default::default()) {
            Self::Or
        } else if operator == &syn::BinOp::Eq(Default::default()) {
            Self::Compare(cmp::Cmp::Eq)
        } else if operator == &syn::BinOp::Ne(Default::default()) {
            Self::Compare(cmp::Cmp::Ne)
        } else if operator == &syn::BinOp::Lt(Default::default()) {
            Self::Compare(cmp::Cmp::Lt)
        } else if operator == &syn::BinOp::Gt(Default::default()) {
            Self::Compare(cmp::Cmp::Gt)
        } else if operator == &syn::BinOp::Le(Default::default()) {
            Self::Compare(cmp::Cmp::Le)
        } else if operator == &syn::BinOp::Ge(Default::default()) {
            Self::Compare(cmp::Cmp::Ge)
        } else {
            Self::Unsupported(operator)
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum Method {
    In,
    NotIn,
    Fts,
    Compare(cmp::Cmp),
}

impl Method {
    fn new(method: &syn::Ident) -> Option<Self> {
        if method == "in_" {
            Some(Self::In)
        } else if method == "not_in" {
            Some(Self::NotIn)
        } else if method == "fts_match" {
            Some(Self::Fts)
        } else if method == "like" {
            Some(Self::Compare(cmp::Cmp::Like))
        } else if method == "ilike" {
            Some(Self::Compare(cmp::Cmp::ILike))
        } else if method == "not_like" {
            Some(Self::Compare(cmp::Cmp::NotLike))
        } else if method == "not_ilike" {
            Some(Self::Compare(cmp::Cmp::NotILike))
        } else if method == "glob" {
            Some(Self::Compare(cmp::Cmp::Glob))
        } else if method == "pg_contains" {
            Some(Self::Compare(cmp::Cmp::Contains))
        } else if method == "pg_overlaps" {
            Some(Self::Compare(cmp::Cmp::Overlaps))
        } else if method == "regex_match" {
            Some(Self::Compare(cmp::Cmp::RegexMatch))
        } else if method == "regex_imatch" {
            Some(Self::Compare(cmp::Cmp::RegexIMatch))
        } else if method == "not_regex_match" {
            Some(Self::Compare(cmp::Cmp::NotRegexMatch))
        } else if method == "not_regex_imatch" {
            Some(Self::Compare(cmp::Cmp::NotRegexIMatch))
        } else if method == "descendant_of" {
            Some(Self::Compare(cmp::Cmp::LtreeDescOf))
        } else if method == "ancestor_of" {
            Some(Self::Compare(cmp::Cmp::LtreeAncOf))
        } else {
            None
        }
    }
}

pub(super) enum Null<'a> {
    Present(&'a syn::ExprMethodCall),
    Absent(&'a syn::ExprMethodCall),
}

impl Null<'_> {
    pub(super) fn parse(
        self,
        scope: &pred::RowScope,
        context: &pred::WhereContext<'_>,
    ) -> syn::Result<sql::Sql> {
        let (syntax, keyword) = match self {
            Self::Present(syntax) => (syntax, "IS_NOT_NULL"),
            Self::Absent(syntax) => (syntax, "IS_NULL"),
        };
        if let Some(attr) = syntax.attrs.first() {
            return Err(syn::Error::new(
                attr.span(),
                "attributes are not supported on a null predicate",
            ));
        }
        if let Some(generic) = &syntax.turbofish {
            return Err(syn::Error::new(
                generic.span(),
                "null predicates do not take generic arguments",
            ));
        }
        if let Some(extra) = syntax.args.first() {
            return Err(syn::Error::new(
                extra.span(),
                "null predicates do not take arguments",
            ));
        }
        let column = scope
            .parse(
                &syntax.receiver,
                None,
                context.param_tys,
                context.dialect,
                context.cte_map,
            )?
            .required(syn::Error::new(
                syntax.receiver.span(),
                "null predicate receiver must be a column or column-derived expression",
            ))?;
        Ok(column.push(context.dialect.kw(keyword)))
    }
}

pub(super) enum Input<'a> {
    Exists {
        syntax: &'a syn::ExprCall,
        name: &'a syn::Ident,
        negate: bool,
    },
    Binary {
        syntax: &'a syn::ExprBinary,
        op: Op<'a>,
    },
    Boolean {
        syntax: &'a syn::ExprLit,
        value: &'a syn::LitBool,
    },
    Field {
        expr: &'a syn::Expr,
        syntax: &'a syn::ExprField,
    },
    Not(&'a syn::ExprUnary),
    Paren(&'a syn::ExprParen),
    Block(&'a syn::ExprBlock),
    Null(Null<'a>),
    Method {
        syntax: &'a syn::ExprMethodCall,
        kind: Method,
    },
    UnsupportedMethod(&'a syn::ExprMethodCall),
    Unsupported(&'a syn::Expr),
}

impl<'a> Input<'a> {
    pub(super) fn new(expr: &'a syn::Expr) -> Self {
        if let syn::Expr::Call(syntax) = expr
            && let Some(path) = expr::Path::new(&syntax.func)
            && let Some(name) = path.function()
        {
            if name == "exists" {
                return Self::Exists {
                    syntax,
                    name,
                    negate: false,
                };
            }
            if name == "not_exists" {
                return Self::Exists {
                    syntax,
                    name,
                    negate: true,
                };
            }
        }
        if let syn::Expr::Binary(syntax) = expr {
            return Self::Binary {
                syntax,
                op: Op::new(&syntax.op),
            };
        }
        if let syn::Expr::Lit(syntax) = expr
            && let syn::Lit::Bool(value) = &syntax.lit
        {
            return Self::Boolean { syntax, value };
        }
        if let syn::Expr::Field(syntax) = expr {
            return Self::Field { expr, syntax };
        }
        if let syn::Expr::Unary(syntax) = expr
            && syntax.op == syn::UnOp::Not(Default::default())
        {
            return Self::Not(syntax);
        }
        if let syn::Expr::Paren(syntax) = expr {
            return Self::Paren(syntax);
        }
        if let syn::Expr::Block(syntax) = expr {
            return Self::Block(syntax);
        }
        if let syn::Expr::MethodCall(syntax) = expr {
            if syntax.method == "is_some" {
                return Self::Null(Null::Present(syntax));
            }
            if syntax.method == "is_none" {
                return Self::Null(Null::Absent(syntax));
            }
            return match Method::new(&syntax.method) {
                Some(kind) => Self::Method { syntax, kind },
                None => Self::UnsupportedMethod(syntax),
            };
        }
        Self::Unsupported(expr)
    }
}
