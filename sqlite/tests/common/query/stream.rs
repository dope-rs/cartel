use crate::common::schema::User;
use cartel_sqlite::dsl::query;

#[cartel_sqlite::query]
pub fn users() -> query::Stream<User> {
    User::filter(|_u| true).rows().order_by(|u| u.id).stream()
}
