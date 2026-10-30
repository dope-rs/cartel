use cartel_gen::query_group;
use cartel_pg::{
    PgRow, PgTable, dsl::expr::In as _, dsl::query, dsl::query::ops::Arrange as _,
    dsl::query::ops::Change as _, dsl::query::ops::Fetch as _, dsl::query::ops::Project as _,
};

#[derive(PgTable, PgRow, PartialEq)]
#[table_name("cartel_pg_users")]
pub struct User {
    pub id: i64,
    pub name: String,
    pub age: i32,
    pub score: f64,
    pub active: bool,
    pub nickname: Option<String>,
    pub avatar: Option<Vec<u8>>,
}

type UserAlias = User;

pub enum Lookup {}

#[query_group]
impl Lookup {
    pub fn by_id(id: i64) -> User {
        User::filter(|u| u.id == id).one()
    }

    pub fn maybe_by_id(id: i64) -> Option<User> {
        User::filter(|u| u.id == id).first()
    }

    pub fn all_rows() -> Vec<User> {
        User::filter(|_u| true).all()
    }

    pub fn all_stream() -> query::Stream<User> {
        User::filter(|_u| true).order_by(|u| u.id).stream()
    }

    pub fn names_min_id(min: i64) -> Vec<String> {
        User::filter(|u| u.id >= min)
            .order_by(|u| u.id)
            .select(|u| u.name)
            .all()
    }

    pub fn id_names() -> Vec<(i64, String)> {
        User::filter(|_u| true)
            .order_by(|u| u.id)
            .select(|u| (u.id, u.name))
            .all()
    }

    pub fn id_plus_one(id: i64) -> i64 {
        User::filter(|u| u.id == id).select(|u| u.id + 1).one()
    }

    pub fn id_literal(id: i64) -> i64 {
        User::filter(|u| u.id == id).select(|u| u.id).one()
    }

    pub fn blob(id: i64) -> Vec<Option<Vec<u8>>> {
        User::filter(|u| u.id == id).select(|u| u.avatar).all()
    }

    pub fn unicode_name() -> String {
        User::filter(|u| u.id == 5).select(|u| u.name).one()
    }
}

pub enum Write {}

impl User {
    pub fn conflicting() -> [(i64, String, i32, f64, bool); 3] {
        [(101, "group-first"), (1, "duplicate"), (102, "skipped")]
            .map(|(id, name)| (id, name.to_owned(), 30, 1.0, true))
    }
}

#[query_group]
impl Write {
    pub fn rename(id: i64, name: String) {
        User::filter(|u| u.id == id).update(|u| u.name = name)
    }

    pub fn set_fields(id: i64, name: String, age: i32, score: f64) {
        User::filter(|u| u.id == id).update(|u| {
            u.name = name;
            u.age = age;
            u.score = score;
        })
    }

    pub fn remove(id: i64) {
        User::filter(|u| u.id == id).delete()
    }

    pub fn clear_age(id: i64) {
        User::filter(|u| u.id == id).update(|u| u.age = 0)
    }

    pub fn add(id: i64, name: String, age: i32, score: f64, active: bool) {
        User::insert(|u| {
            u.id = id;
            u.name = name;
            u.age = age;
            u.score = score;
            u.active = active;
        })
    }

    pub fn add_all(id: i64, name: String, age: i32, score: f64, active: bool) -> Vec<User> {
        User::insert(|u| {
            u.id = id;
            u.name = name;
            u.age = age;
            u.score = score;
            u.active = active;
        })
        .returning_all()
    }

    pub fn add_one(id: i64, name: String, age: i32, score: f64, active: bool) -> UserAlias {
        User::insert(|u| {
            u.id = id;
            u.name = name;
            u.age = age;
            u.score = score;
            u.active = active;
        })
        .returning_one()
    }

    pub fn add_first(
        id: i64,
        name: String,
        age: i32,
        score: f64,
        active: bool,
    ) -> Option<self::User> {
        User::insert(|u| {
            u.id = id;
            u.name = name;
            u.age = age;
            u.score = score;
            u.active = active;
        })
        .returning_first()
    }

    pub fn copy_min_age(min_age: i32, offset: i64) {
        User::insert_from(User::filter(|u| u.age >= min_age), |t, src| {
            t.id = src.id + offset;
            t.name = src.name;
            t.age = src.age;
            t.score = src.score;
            t.active = src.active;
        })
    }

    pub fn add_literal(id: i64, name: String) {
        User::insert(|u| {
            u.id = id;
            u.name = name;
            u.age = 7;
            u.score = 1.25;
            u.active = true;
        })
    }

    pub fn add_or_skip(id: i64, name: String, age: i32, score: f64, active: bool) {
        User::insert(|u| {
            u.id = id;
            u.name = name;
            u.age = age;
            u.score = score;
            u.active = active;
        })
        .on_conflict_do_nothing()
    }

    pub fn upsert_name(
        id: i64,
        name: String,
        upd_name: String,
        age: i32,
        score: f64,
        active: bool,
    ) {
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

    pub fn add_id_skip(id: i64, name: String, age: i32, score: f64, active: bool) {
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

    pub fn rename_row(id: i64, name: String) -> User {
        User::filter(|u| u.id == id)
            .update(|u| u.name = name)
            .returning_one()
    }

    pub fn take(id: i64) -> Option<User> {
        User::filter(|u| u.id == id).delete().returning_first()
    }

    pub fn take_idle() -> Vec<User> {
        User::filter(|u| !u.active).delete().returning_all()
    }
}

pub enum Compare {}

#[query_group]
impl Compare {
    pub fn eq_age(age: i32) -> Vec<User> {
        User::filter(|u| u.age == age).order_by(|u| u.id).all()
    }

    pub fn ne_age(age: i32) -> Vec<User> {
        User::filter(|u| u.age != age).order_by(|u| u.id).all()
    }

    pub fn gt_age(age: i32) -> Vec<User> {
        User::filter(|u| u.age > age).order_by(|u| u.id).all()
    }

    pub fn lt_age(age: i32) -> Vec<User> {
        User::filter(|u| u.age < age).order_by(|u| u.id).all()
    }

    pub fn ge_age(age: i32) -> Vec<User> {
        User::filter(|u| u.age >= age).order_by(|u| u.id).all()
    }

    pub fn le_age(age: i32) -> Vec<User> {
        User::filter(|u| u.age <= age).order_by(|u| u.id).all()
    }

    pub fn with_nick() -> Vec<User> {
        User::filter(|u| u.nickname.is_some())
            .order_by(|u| u.id)
            .all()
    }

    pub fn no_nick() -> Vec<User> {
        User::filter(|u| u.nickname.is_none())
            .order_by(|u| u.id)
            .all()
    }

    pub fn active_from(min_age: i32, active: bool) -> Vec<User> {
        User::filter(|u| u.age >= min_age && u.active == active)
            .order_by(|u| u.id)
            .all()
    }

    pub fn age_or(low: i32, high: i32) -> Vec<User> {
        User::filter(|u| u.age <= low || u.age >= high)
            .order_by(|u| u.id)
            .all()
    }

    pub fn nested(min_age: i32, max_age: i32, active: bool) -> Vec<User> {
        User::filter(|u| (u.age >= min_age && u.age <= max_age) || u.active == active)
            .order_by(|u| u.id)
            .all()
    }

    pub fn not_via_ne(active: bool) -> Vec<User> {
        User::filter(|u| u.active != active)
            .order_by(|u| u.id)
            .all()
    }

    pub fn in_ids(ids: &[i64]) -> Vec<User> {
        User::filter(|u| u.id.in_(ids)).order_by(|u| u.id).all()
    }

    pub fn not_in_ids(ids: &[i64]) -> Vec<User> {
        User::filter(|u| u.id.not_in(ids)).order_by(|u| u.id).all()
    }
}
