use crate::common::schema::User;
use cartel_sqlite::dsl::query;

#[cartel_sqlite::query]
pub fn users() -> query::Stream<(i64, String)> {
    User::filter(|_u| true)
        .order_by(|u| u.id)
        .select(|u| (u.id, u.name))
        .stream()
}
