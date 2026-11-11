use crate::common::external::user::User;
use cartel_gen::query_group;
use cartel_pg::dsl::expr::Text as _;

pub enum Pattern {}

#[query_group]
impl Pattern {
    pub fn like(pat: String) -> Vec<User> {
        User::filter(|u| u.name.like(pat)).order_by(|u| u.id).all()
    }

    pub fn not_like(pat: String) -> Vec<User> {
        User::filter(|u| u.name.not_like(pat))
            .order_by(|u| u.id)
            .all()
    }

    pub fn ilike(pat: String) -> Vec<User> {
        User::filter(|u| u.name.ilike(pat)).order_by(|u| u.id).all()
    }

    pub fn not_ilike(pat: String) -> Vec<User> {
        User::filter(|u| u.name.not_ilike(pat))
            .order_by(|u| u.id)
            .all()
    }

    pub fn regex(pat: String) -> Vec<User> {
        User::filter(|u| u.name.regex_match(pat))
            .order_by(|u| u.id)
            .all()
    }

    pub fn regex_i(pat: String) -> Vec<User> {
        User::filter(|u| u.name.regex_imatch(pat))
            .order_by(|u| u.id)
            .all()
    }

    pub fn not_regex(pat: String) -> Vec<User> {
        User::filter(|u| u.name.not_regex_match(pat))
            .order_by(|u| u.id)
            .all()
    }

    pub fn not_regex_i(pat: String) -> Vec<User> {
        User::filter(|u| u.name.not_regex_imatch(pat))
            .order_by(|u| u.id)
            .all()
    }
}

pub enum Order {}

#[query_group]
impl Order {
    pub fn ordered_desc() -> Vec<i64> {
        User::filter(|_u| true)
            .order_by_desc(|u| u.age)
            .select(|u| u.id)
            .all()
    }

    pub fn limited() -> Vec<i64> {
        User::filter(|_u| true)
            .order_by(|u| u.id)
            .limit(2)
            .select(|u| u.id)
            .all()
    }

    pub fn offset() -> Vec<i64> {
        User::filter(|_u| true)
            .order_by(|u| u.id)
            .limit(2)
            .offset(2)
            .select(|u| u.id)
            .all()
    }

    pub fn unique_ages() -> Vec<i32> {
        User::filter(|_u| true)
            .distinct()
            .order_by(|u| u.age)
            .select(|u| u.age)
            .all()
    }

    pub fn limit_param(n: i64) -> Vec<User> {
        User::filter(|_u| true).order_by(|u| u.id).limit(n).all()
    }

    pub fn active_heads() -> Vec<User> {
        User::filter(|_u| true)
            .distinct_on(|u| u.active)
            .order_by(|u| u.active)
            .order_by(|u| u.id)
            .all()
    }

    pub fn expr_order() -> Vec<i64> {
        User::filter(|_u| true)
            .order_by_desc(|u| u.id + u.id)
            .select(|u| u.id)
            .all()
    }

    pub fn locked() -> Vec<i64> {
        User::filter(|u| u.id <= 2)
            .order_by(|u| u.id)
            .for_update()
            .select(|u| u.id)
            .all()
    }

    pub fn share_lock() -> Vec<i64> {
        User::filter(|u| u.id <= 2)
            .order_by(|u| u.id)
            .for_share()
            .select(|u| u.id)
            .all()
    }
}
