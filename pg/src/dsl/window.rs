use crate::dsl::scope::never::Never;

pub struct Expr<T>(Never<T>);
pub struct Spec(Never<()>);

impl<T> Expr<T> {
    pub fn over<F>(self, plan: F) -> T
    where
        F: FnOnce(Spec) -> Spec,
    {
        self.0.infer(plan)
    }
}

impl Spec {
    pub fn partition_by<T>(self, key: T) -> Self {
        self.0.infer(key)
    }

    pub fn order_by<T>(self, key: T) -> Self {
        self.0.infer(key)
    }
}
