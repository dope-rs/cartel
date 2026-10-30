use std::mem;
use syn::visit_mut::{self, VisitMut};

pub(super) struct Owner<'a> {
    model: &'a syn::Type,
}

impl<'a> Owner<'a> {
    pub(super) fn new(model: &'a syn::Type) -> Self {
        Self { model }
    }
}

impl VisitMut for Owner<'_> {
    fn visit_type_mut(&mut self, ty: &mut syn::Type) {
        visit_mut::visit_type_mut(self, ty);
        let syn::Type::Path(path) = ty else {
            return;
        };
        if path.qself.is_some()
            || path.path.leading_colon.is_some()
            || !path
                .path
                .segments
                .first()
                .is_some_and(|first| first.ident == "Self" && first.arguments.is_none())
        {
            return;
        }
        if path.path.segments.len() == 1 {
            *ty = self.model.clone();
            return;
        }
        path.qself = Some(syn::QSelf {
            lt_token: Default::default(),
            ty: Box::new(self.model.clone()),
            position: 0,
            as_token: None,
            gt_token: Default::default(),
        });
        path.path.leading_colon = Some(Default::default());
        path.path.segments = mem::take(&mut path.path.segments)
            .into_pairs()
            .skip(1)
            .collect();
    }
}
