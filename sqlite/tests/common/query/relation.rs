use crate::{
    common::schema::Comment, common::schema::Post, common::schema::Tag, common::schema::User,
};

#[cartel_sqlite::query]
pub fn user_with_posts() -> Vec<(User, Post)> {
    User::join::<Post>(|u, p| u.id == p.author_id)
        .filter(|_u, _p| true)
        .order_by(|_u, p| p.id)
        .all()
}

#[cartel_sqlite::query]
pub fn user_left_join_posts() -> i64 {
    User::left_join::<Post>(|u, p| u.id == p.author_id)
        .filter(|_u, _p| true)
        .count()
}

#[cartel_sqlite::query]
pub fn user_post_comment() -> Vec<(User, Post, Comment)> {
    User::join::<Post>(|u, p| u.id == p.author_id)
        .join::<Comment>(|_u, p, c| p.id == c.post_id)
        .filter(|_u, _p, _c| true)
        .order_by(|_u, _p, c| c.id)
        .all()
}

#[cartel_sqlite::query]
pub fn user_post_comment_tag() -> Vec<(User, Post, Comment, Tag)> {
    User::join::<Post>(|u, p| u.id == p.author_id)
        .join::<Comment>(|_u, p, c| p.id == c.post_id)
        .join::<Tag>(|_u, p, _c, t| p.id == t.post_id)
        .filter(|_u, _p, _c, _t| true)
        .order_by(|_u, _p, c, _t| c.id)
        .all()
}

#[cartel_sqlite::query]
pub fn count_user_posts() -> i64 {
    User::join::<Post>(|u, p| u.id == p.author_id)
        .filter(|_u, _p| true)
        .count()
}

#[cartel_sqlite::query]
pub fn post_titles_by_user(uid: i64) -> Vec<String> {
    User::join::<Post>(|u, p| u.id == p.author_id)
        .filter(|u, _p| u.id == uid)
        .select(|_u, p| p.title)
        .all()
}

#[cartel_sqlite::query]
pub fn users_union() -> Vec<User> {
    User::filter(|u| u.id <= 1)
        .union(User::filter(|u| u.id >= 4))
        .rows()
        .order_by(|u| u.id)
        .all()
}

#[cartel_sqlite::query]
pub fn users_union_all() -> Vec<User> {
    User::filter(|u| u.id <= 1)
        .union_all(User::filter(|u| u.id == 1))
        .rows()
        .order_by(|u| u.id)
        .all()
}

#[cartel_sqlite::query]
pub fn users_intersect() -> Vec<User> {
    User::filter(|u| u.age >= 25)
        .intersect(User::filter(|u| u.active))
        .rows()
        .order_by(|u| u.id)
        .all()
}

#[cartel_sqlite::query]
pub fn users_except() -> Vec<User> {
    User::filter(|_u| true)
        .except(User::filter(|u| !u.active))
        .rows()
        .order_by(|u| u.id)
        .all()
}

#[cartel_sqlite::query]
pub fn cte_active_users() -> Vec<User> {
    let active = User::filter(User::active).cte();
    active
        .filter(|u| u.age >= 25)
        .rows()
        .order_by(|u| u.id)
        .all()
}

#[cartel_sqlite::query]
pub fn cte_union(min_age: i32) -> Vec<User> {
    let young = User::filter(User::age < 30).cte();
    let actives = User::filter(User::active).cte();
    young
        .filter(|u| u.age >= min_age)
        .union(actives.filter(|u| u.age >= min_age))
        .rows()
        .order_by(|u| u.id)
        .all()
}

#[cartel_sqlite::query]
pub fn users_with_any_post() -> Vec<User> {
    User::filter(|u| Dialect::exists(Post::filter(|p| p.author_id == u.id)))
        .rows()
        .order_by(|u| u.id)
        .all()
}

#[cartel_sqlite::query]
pub fn users_without_post() -> Vec<User> {
    User::filter(|u| Dialect::not_exists(Post::filter(|p| p.author_id == u.id)))
        .rows()
        .order_by(|u| u.id)
        .all()
}

#[cartel_sqlite::query]
pub fn user_post_counts() -> Vec<(i64, i64)> {
    User::filter(|_u| true)
        .rows()
        .order_by(|u| u.id)
        .select(|u| (u.id, Post::filter(|p| p.author_id == u.id).count()))
        .all()
}
