use crate::common::schema::{Correlated, Keyword, User};

type UserAlias = User;

#[cartel_sqlite::query]
pub fn rename_user(id: i64, name: &str) {
    User::filter(|scope| scope.id == id).update(|scope| scope.name = name)
}

#[cartel_sqlite::query]
pub fn set_fields(id: i64, name: String, age: i32, score: f64) {
    User::filter(|u| u.id == id).update(|u| {
        u.name = name;
        u.age = age;
        u.score = score;
    })
}

#[cartel_sqlite::query]
pub fn delete_user(id: i64) {
    User::filter(|u| u.id == id).delete()
}

#[cartel_sqlite::query]
pub fn insert_keyword(value: i64) {
    Keyword::insert(|row| row.r#type = value)
}

#[cartel_sqlite::query]
pub fn rewrite_keyword(current: i64, next: i64) {
    Keyword::filter(|row| row.r#type == current).update(|row| row.r#type = next)
}

#[cartel_sqlite::query]
pub fn upsert_keyword(value: i64, next: i64) {
    Keyword::insert(|row| row.r#type = value)
        .on_conflict(|row| row.r#type)
        .do_update(|row| row.r#type = next)
}

#[cartel_sqlite::query]
pub fn upsert_correlated(id: i64) -> Correlated {
    Correlated::insert(|row| {
        row.id = id;
        row.value = 0;
    })
    .on_conflict(|row| row.id)
    .do_update(|row| row.value = Correlated::filter(|inner| inner.id < row.id).count())
    .returning_one()
}

#[cartel_sqlite::query]
pub fn insert_user(id: i64, name: String, age: i32, score: f64, active: bool) {
    User::insert(|u| {
        u.id = id;
        u.name = name;
        u.age = age;
        u.score = score;
        u.active = active;
    })
}

#[cartel_sqlite::query]
pub fn insert_all(id: i64, name: String, age: i32, score: f64, active: bool) -> Vec<User> {
    User::insert(|u| {
        u.id = id;
        u.name = name;
        u.age = age;
        u.score = score;
        u.active = active;
    })
    .returning_all()
}

#[cartel_sqlite::query]
pub fn insert_one(
    id: i64,
    name: String,
    age: i32,
    score: f64,
    active: bool,
) -> crate::common::schema::User {
    User::insert(|u| {
        u.id = id;
        u.name = name;
        u.age = age;
        u.score = score;
        u.active = active;
    })
    .returning_one()
}

#[cartel_sqlite::query]
pub fn insert_first(id: i64, name: String, age: i32, score: f64, active: bool) -> Option<User> {
    User::insert(|u| {
        u.id = id;
        u.name = name;
        u.age = age;
        u.score = score;
        u.active = active;
    })
    .returning_first()
}

#[cartel_sqlite::query]
pub fn copy_older(min_age: i32, offset: i64) {
    let selected = User::filter(|u| u.age >= min_age).cte();
    User::insert_from(selected.filter(|_u| true), |t, src| {
        t.id = src.id + offset;
        t.name = src.name;
        t.age = src.age;
        t.score = src.score;
        t.active = src.active;
    })
}

#[cartel_sqlite::query]
pub fn insert_or_ignore(id: i64, name: String, age: i32, score: f64, active: bool) {
    User::insert(|u| {
        u.id = id;
        u.name = name;
        u.age = age;
        u.score = score;
        u.active = active;
    })
    .on_conflict_do_nothing()
}

#[cartel_sqlite::query]
pub fn upsert_name(id: i64, name: String, upd_name: String, age: i32, score: f64, active: bool) {
    User::insert(|u| {
        u.id = id;
        u.name = name;
        u.age = age;
        u.score = score;
        u.active = active;
    })
    .on_conflict(|u| u.id)
    .do_update(|u| u.name = upd_name)
}

#[cartel_sqlite::query]
pub fn insert_or_keep(id: i64, name: String, age: i32, score: f64, active: bool) {
    User::insert(|u| {
        u.id = id;
        u.name = name;
        u.age = age;
        u.score = score;
        u.active = active;
    })
    .on_conflict(|u| u.id)
    .do_nothing()
}

#[cartel_sqlite::query]
pub fn update_one(id: i64, name: String) -> UserAlias {
    User::filter(|u| u.id == id)
        .update(|u| u.name = name)
        .returning_one()
}

#[cartel_sqlite::query]
pub fn delete_one(id: i64) -> Option<User> {
    User::filter(|u| u.id == id).delete().returning_first()
}

#[cartel_sqlite::query]
pub fn delete_inactive() -> Vec<User> {
    User::filter(|u| !u.active).delete().returning_all()
}
