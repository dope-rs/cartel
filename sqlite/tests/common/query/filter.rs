use crate::{common::schema::User, common::schema::Widget};
use cartel_sqlite::dsl::expr::Text as _;

#[cartel_sqlite::query]
pub fn users_eq_age(age: i32) -> Vec<User> {
    User::filter(|u| u.age == age)
        .rows()
        .order_by(|u| u.id)
        .all()
}

#[cartel_sqlite::query]
pub fn users_ne_age(age: i32) -> Vec<User> {
    User::filter(|u| u.age != age)
        .rows()
        .order_by(|u| u.id)
        .all()
}

#[cartel_sqlite::query]
pub fn users_gt_age(age: i32) -> Vec<User> {
    User::filter(|u| u.age > age)
        .rows()
        .order_by(|u| u.id)
        .all()
}

#[cartel_sqlite::query]
pub fn users_lt_age(age: i32) -> Vec<User> {
    User::filter(|u| u.age < age)
        .rows()
        .order_by(|u| u.id)
        .all()
}

#[cartel_sqlite::query]
pub fn users_ge_age(age: i32) -> Vec<User> {
    User::filter(|u| u.age >= age)
        .rows()
        .order_by(|u| u.id)
        .all()
}

#[cartel_sqlite::query]
pub fn users_le_age(age: i32) -> Vec<User> {
    User::filter(|u| u.age <= age)
        .rows()
        .order_by(|u| u.id)
        .all()
}

#[cartel_sqlite::query]
pub fn users_like(pat: String) -> Vec<User> {
    User::filter(|u| u.name.like(pat))
        .rows()
        .order_by(|u| u.id)
        .all()
}

#[cartel_sqlite::query]
pub fn users_not_like(pat: String) -> Vec<User> {
    User::filter(|u| u.name.not_like(pat))
        .rows()
        .order_by(|u| u.id)
        .all()
}

#[cartel_sqlite::query]
pub fn users_glob(pat: String) -> Vec<User> {
    User::filter(|u| u.name.glob(pat))
        .rows()
        .order_by(|u| u.id)
        .all()
}

#[cartel_sqlite::query]
pub fn users_nickname_some() -> Vec<User> {
    User::filter(|u| u.nickname.is_some())
        .rows()
        .order_by(|u| u.id)
        .all()
}

#[cartel_sqlite::query]
pub fn users_nickname_none() -> Vec<User> {
    User::filter(|u| u.nickname.is_none())
        .rows()
        .order_by(|u| u.id)
        .all()
}

#[cartel_sqlite::query]
pub fn users_and(min_age: i32, active: bool) -> Vec<User> {
    User::filter(|u| u.age >= min_age && u.active == active)
        .rows()
        .order_by(|u| u.id)
        .all()
}

#[cartel_sqlite::query]
pub fn users_or(low: i32, high: i32) -> Vec<User> {
    User::filter(|u| u.age <= low || u.age >= high)
        .rows()
        .order_by(|u| u.id)
        .all()
}

#[cartel_sqlite::query]
pub fn users_nested(min_age: i32, max_age: i32, active: bool) -> Vec<User> {
    User::filter(|u| (u.age >= min_age && u.age <= max_age) || u.active == active)
        .rows()
        .order_by(|u| u.id)
        .all()
}

#[cartel_sqlite::query]
pub fn users_not_via_ne(active: bool) -> Vec<User> {
    User::filter(|u| u.active != active)
        .rows()
        .order_by(|u| u.id)
        .all()
}

#[cartel_sqlite::query]
pub fn users_ordered_desc() -> Vec<i64> {
    User::filter(|_u| true)
        .rows()
        .order_by_desc(|u| u.age)
        .select(|u| u.id)
        .all()
}

#[cartel_sqlite::query]
pub fn users_limited() -> Vec<i64> {
    User::filter(|_u| true)
        .rows()
        .order_by(|u| u.id)
        .limit(2)
        .select(|u| u.id)
        .all()
}

#[cartel_sqlite::query]
pub fn users_offset() -> Vec<i64> {
    User::filter(|_u| true)
        .rows()
        .order_by(|u| u.id)
        .limit(2)
        .offset(2)
        .select(|u| u.id)
        .all()
}

#[cartel_sqlite::query]
pub fn distinct_ages() -> Vec<i32> {
    User::filter(|_u| true)
        .rows()
        .distinct()
        .order_by(|u| u.age)
        .select(|u| u.age)
        .all()
}

#[cartel_sqlite::query]
pub fn users_limit_param(r#n: i64) -> Vec<User> {
    User::filter(|_u| true)
        .rows()
        .order_by(|u| u.id)
        .limit(n)
        .all()
}

#[cartel_sqlite::query]
pub fn users_offset_param(n: i64) -> Vec<User> {
    User::filter(|_u| true)
        .rows()
        .order_by(|u| u.id)
        .offset(n)
        .all()
}

#[cartel_sqlite::query]
pub fn count_all() -> i64 {
    User::filter(|_u| true).count()
}

#[cartel_sqlite::query]
pub fn count_active(active: bool) -> i64 {
    User::filter(|u| u.active == active).count()
}

#[cartel_sqlite::query]
pub fn sum_ages() -> Option<i64> {
    User::filter(true).sum(User::age)
}

#[cartel_sqlite::query]
pub fn avg_age() -> Option<f64> {
    User::filter(true).avg(User::age)
}

#[cartel_sqlite::query]
pub fn min_age() -> Option<i32> {
    User::filter(true).min(User::age)
}

#[cartel_sqlite::query]
pub fn max_score() -> Option<f64> {
    User::filter(true).max(User::score)
}

#[cartel_sqlite::query]
pub fn widgets_count_per_bucket() -> Vec<(i32, i64)> {
    Widget::filter(true)
        .group_by(Widget::bucket)
        .order_by(Widget::bucket)
        .count()
}

#[cartel_sqlite::query]
pub fn widgets_sum_per_bucket() -> Vec<(i32, Option<i64>)> {
    Widget::filter(true)
        .group_by(Widget::bucket)
        .order_by(Widget::bucket)
        .sum(Widget::value)
}

#[cartel_sqlite::query]
pub fn widgets_having(min: i64) -> Vec<(i32, i64)> {
    Widget::filter(|_w| true)
        .group_by(|w| w.bucket)
        .having(|_w, agg| agg.count() >= min)
        .order_by(|w| w.bucket)
        .count()
}
