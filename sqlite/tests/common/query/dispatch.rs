use crate::common::schema::{Keyword, User};

#[cartel_sqlite::query]
pub fn user_by_id(id: i64) -> User {
    User::filter(|u| u.id == id).one()
}

#[cartel_sqlite::query]
pub fn r#maybe_user_by_id(id: i64) -> Option<User> {
    User::filter(|u| u.id == id).first()
}

#[cartel_sqlite::query]
pub fn all_users() -> Vec<User> {
    User::filter(|_u| true).all()
}

#[cartel_sqlite::query]
pub fn user_names_min_id(min: i64) -> Vec<String> {
    User::filter(|u| u.id >= min)
        .order_by(|u| u.id)
        .select(|u| u.name)
        .all()
}

#[cartel_sqlite::query]
pub fn user_id_name_pairs() -> Vec<(i64, String)> {
    User::filter(|_u| true)
        .order_by(|u| u.id)
        .select(|u| (u.id, u.name))
        .all()
}

#[cartel_sqlite::query]
pub fn user_id_plus_one(id: i64) -> i64 {
    User::filter(|u| u.id == id).select(|u| u.id + 1).one()
}

#[cartel_sqlite::query]
pub fn keyword_value(value: i64) -> Option<i64> {
    Keyword::<
        i64,
        {
            let mut count = filter();
            count = count + self::filter();
            count
        },
    >::filter(|row| row.r#type == value)
    .select(|row| row.r#type)
    .first()
}

const fn filter() -> usize {
    0
}

#[cartel_sqlite::query]
pub fn user_blob(id: i64) -> Vec<Option<Vec<u8>>> {
    User::filter(|u| u.id == id).select(|u| u.avatar).all()
}

#[cartel_sqlite::query]
pub fn unicode_user_name() -> String {
    User::filter(|u| u.id == 5).select(|u| u.name).one()
}
