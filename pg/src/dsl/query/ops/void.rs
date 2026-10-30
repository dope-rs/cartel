use crate::dsl::query;

pub trait Void: Sized {
    fn infer<R, A>(self, args: A) -> R;
}

impl<T> Void for query::Filter<T> {
    fn infer<R, A>(self, args: A) -> R {
        self.0.infer(args)
    }
}

impl<T> Void for query::Cte<T> {
    fn infer<R, A>(self, args: A) -> R {
        self.0.infer(args)
    }
}

impl<T> Void for query::Select<T> {
    fn infer<R, A>(self, args: A) -> R {
        self.0.infer(args)
    }
}
