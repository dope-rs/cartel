#![allow(clippy::bool_comparison, clippy::doc_overindented_list_items)]

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

use cartel_gen::{pg_instance, query_group};
use cartel_pg::dsl::{
    Array as _, Fts as _, In as _, Ltree as _, Text as _, array_length, cardinality, exists, lag,
    lead, not_exists, plainto_tsquery, row_number, to_tsquery, to_tsvector, ts_rank,
};
use cartel_pg::{
    Date, IsolationLevel, Ltree, PgHolding, PgOps, PgPool, PgRawExt, PgTable, Stream, Timestamp,
    Uuid,
};
use dope::manifold::connector::Connector;
use dope::manifold::connector::source::Static;
use dope::manifold::env::Bundle;
use dope::runtime::profile::Throughput;
use dope::transport::Tcp;
use dope::wire::Identity;
use dope::{DriverCfg, DriverConfig, Executor};

type PgEnv = Bundle<Tcp, Identity, Throughput>;
type PgConn<I> = Connector<0, cartel_pg::Session<I>, Static<Tcp>, PgEnv>;
type CartelClient<'d, I> = PgHolding<'d, I, Static<Tcp>, PgEnv>;

#[derive(PgTable, Debug, PartialEq)]
#[table_name("cartel_pg_users")]
struct User {
    #[pk]
    id: i64,
    name: String,
    age: i32,
    score: f64,
    active: bool,
    nickname: Option<String>,
    avatar: Option<Vec<u8>>,
}

#[derive(PgTable, Debug, PartialEq)]
#[table_name("cartel_pg_posts")]
struct Post {
    #[pk]
    id: i64,
    author_id: i64,
    title: String,
    body: String,
    likes: i64,
}

#[derive(PgTable, Debug, PartialEq)]
#[table_name("cartel_pg_comments")]
struct Comment {
    #[pk]
    id: i64,
    post_id: i64,
    author_id: i64,
    text: String,
}

#[derive(PgTable, Debug, PartialEq)]
#[table_name("cartel_pg_tags")]
struct Tag {
    #[pk]
    id: i64,
    post_id: i64,
    label: String,
}

#[derive(PgTable, Debug, PartialEq)]
#[table_name("cartel_pg_widgets")]
struct Widget {
    #[pk]
    id: i64,
    bucket: i32,
    value: i64,
}

#[derive(PgTable, Debug, PartialEq)]
#[table_name("cartel_pg_items")]
struct Item {
    #[pk]
    a: i32,
    #[pk]
    b: i32,
    payload: String,
}

#[derive(PgTable, Debug, PartialEq)]
#[table_name("cartel_pg_types")]
struct AllTypes {
    #[pk]
    id: i64,
    v_i16: i16,
    v_i32: i32,
    v_i64: i64,
    v_f32: f32,
    v_f64: f64,
    v_bool: bool,
    v_string: String,
    v_bytes: Vec<u8>,
    v_uuid: Uuid,
    v_ts: Timestamp,
    v_date: Date,
    v_ltree: Ltree,
    o_i32: Option<i32>,
    o_i64: Option<i64>,
    o_bool: Option<bool>,
    o_string: Option<String>,
    o_bytes: Option<Vec<u8>>,
    o_uuid: Option<Uuid>,
    arr_i32: Vec<i32>,
    arr_i64: Vec<i64>,
    arr_str: Vec<String>,
}

#[derive(PgTable, Debug, PartialEq)]
#[table_name("cartel_pg_docs")]
struct Doc {
    #[pk]
    id: i64,
    body: String,
    payload: Vec<u8>,
}

#[derive(PgTable, Debug, PartialEq)]
#[table_name("cartel_pg_nodes")]
struct LtreeNode {
    #[pk]
    id: i64,
    path: Ltree,
}

#[derive(PgTable, Debug, PartialEq)]
#[table_name("cartel_pg_widgets")]
struct WidgetSlim {
    #[pk]
    id: i64,
    bucket: i32,
    value: i64,
}

#[derive(PgTable, Debug, PartialEq)]
#[table_name("cartel_pg_scores")]
struct Score {
    #[pk]
    id: i32,
    randomnumber: i32,
}

#[derive(PgTable, Debug, PartialEq)]
#[table_name("cartel_pg_copy")]
struct CopyRow {
    #[pk]
    id: i64,
    label: String,
}

#[query_group]
impl User {
    fn by_id(id: i64) -> User {
        User::filter(|u| u.id == id).one()
    }

    fn maybe_by_id(id: i64) -> Option<User> {
        User::filter(|u| u.id == id).first()
    }

    fn all_rows() -> Vec<User> {
        User::filter(|_u| true).all()
    }

    fn all_stream() -> Stream<User> {
        User::filter(|_u| true).order_by(|u| u.id).stream()
    }

    fn names_min_id(min: i64) -> Vec<String> {
        User::filter(|u| u.id >= min)
            .order_by(|u| u.id)
            .select(|u| u.name)
            .all()
    }

    fn id_name_pairs() -> Vec<(i64, String)> {
        User::filter(|_u| true)
            .order_by(|u| u.id)
            .select(|u| (u.id, u.name))
            .all()
    }

    fn id_plus_one(id: i64) -> i64 {
        User::filter(|u| u.id == id).select(|u| u.id + 1).one()
    }

    fn id_literal(id: i64) -> i64 {
        User::filter(|u| u.id == id).select(|u| u.id).one()
    }

    fn blob(id: i64) -> Vec<Option<Vec<u8>>> {
        User::filter(|u| u.id == id).select(|u| u.avatar).all()
    }

    fn unicode_name() -> String {
        User::filter(|u| u.id == 5).select(|u| u.name).one()
    }

    fn rename(id: i64, name: String) {
        User::filter(|u| u.id == id).update(|u| u.name = name)
    }

    fn set_fields(id: i64, name: String, age: i32, score: f64) {
        User::filter(|u| u.id == id).update(|u| {
            u.name = name;
            u.age = age;
            u.score = score;
        })
    }

    fn remove(id: i64) {
        User::filter(|u| u.id == id).delete()
    }

    fn reset_age_to_zero(id: i64) {
        User::filter(|u| u.id == id).update(|u| u.age = 0)
    }

    fn add(id: i64, name: String, age: i32, score: f64, active: bool) {
        User::insert(|u| {
            u.id = id;
            u.name = name;
            u.age = age;
            u.score = score;
            u.active = active;
        })
    }

    fn insert_returning_all(
        id: i64,
        name: String,
        age: i32,
        score: f64,
        active: bool,
    ) -> Vec<User> {
        User::insert(|u| {
            u.id = id;
            u.name = name;
            u.age = age;
            u.score = score;
            u.active = active;
        })
        .returning_all()
    }

    fn insert_returning_one(id: i64, name: String, age: i32, score: f64, active: bool) -> User {
        User::insert(|u| {
            u.id = id;
            u.name = name;
            u.age = age;
            u.score = score;
            u.active = active;
        })
        .returning_one()
    }

    fn insert_returning_first(
        id: i64,
        name: String,
        age: i32,
        score: f64,
        active: bool,
    ) -> Option<User> {
        User::insert(|u| {
            u.id = id;
            u.name = name;
            u.age = age;
            u.score = score;
            u.active = active;
        })
        .returning_first()
    }

    fn copy_min_age(min_age: i32, offset: i64) {
        User::insert_from(User::filter(|u| u.age >= min_age), |t, src| {
            t.id = src.id + offset;
            t.name = src.name;
            t.age = src.age;
            t.score = src.score;
            t.active = src.active;
        })
    }

    fn insert_literal_age(id: i64, name: String) {
        User::insert(|u| {
            u.id = id;
            u.name = name;
            u.age = 7;
            u.score = 1.25;
            u.active = true;
        })
    }

    fn insert_on_conflict_do_nothing(id: i64, name: String, age: i32, score: f64, active: bool) {
        User::insert(|u| {
            u.id = id;
            u.name = name;
            u.age = age;
            u.score = score;
            u.active = active;
        })
        .on_conflict_do_nothing()
    }

    fn upsert_name(id: i64, name: String, upd_name: String, age: i32, score: f64, active: bool) {
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

    fn insert_target_do_nothing(id: i64, name: String, age: i32, score: f64, active: bool) {
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

    fn update_returning(id: i64, name: String) -> User {
        User::filter(|u| u.id == id)
            .update(|u| u.name = name)
            .returning_one()
    }

    fn delete_returning(id: i64) -> Option<User> {
        User::filter(|u| u.id == id).delete().returning_first()
    }

    fn delete_all_inactive_returning() -> Vec<User> {
        User::filter(|u| u.active == false).delete().returning_all()
    }

    fn eq_age(age: i32) -> Vec<User> {
        User::filter(|u| u.age == age).order_by(|u| u.id).all()
    }
    fn ne_age(age: i32) -> Vec<User> {
        User::filter(|u| u.age != age).order_by(|u| u.id).all()
    }
    fn gt_age(age: i32) -> Vec<User> {
        User::filter(|u| u.age > age).order_by(|u| u.id).all()
    }
    fn lt_age(age: i32) -> Vec<User> {
        User::filter(|u| u.age < age).order_by(|u| u.id).all()
    }
    fn ge_age(age: i32) -> Vec<User> {
        User::filter(|u| u.age >= age).order_by(|u| u.id).all()
    }
    fn le_age(age: i32) -> Vec<User> {
        User::filter(|u| u.age <= age).order_by(|u| u.id).all()
    }
    fn like(pat: String) -> Vec<User> {
        User::filter(|u| u.name.like(pat)).order_by(|u| u.id).all()
    }
    fn not_like(pat: String) -> Vec<User> {
        User::filter(|u| u.name.not_like(pat))
            .order_by(|u| u.id)
            .all()
    }
    fn ilike(pat: String) -> Vec<User> {
        User::filter(|u| u.name.ilike(pat)).order_by(|u| u.id).all()
    }
    fn not_ilike(pat: String) -> Vec<User> {
        User::filter(|u| u.name.not_ilike(pat))
            .order_by(|u| u.id)
            .all()
    }
    fn regex(pat: String) -> Vec<User> {
        User::filter(|u| u.name.regex_match(pat))
            .order_by(|u| u.id)
            .all()
    }
    fn regex_i(pat: String) -> Vec<User> {
        User::filter(|u| u.name.regex_imatch(pat))
            .order_by(|u| u.id)
            .all()
    }
    fn not_regex(pat: String) -> Vec<User> {
        User::filter(|u| u.name.not_regex_match(pat))
            .order_by(|u| u.id)
            .all()
    }
    fn not_regex_i(pat: String) -> Vec<User> {
        User::filter(|u| u.name.not_regex_imatch(pat))
            .order_by(|u| u.id)
            .all()
    }
    fn nickname_some() -> Vec<User> {
        User::filter(|u| u.nickname.is_some())
            .order_by(|u| u.id)
            .all()
    }
    fn nickname_none() -> Vec<User> {
        User::filter(|u| u.nickname.is_none())
            .order_by(|u| u.id)
            .all()
    }
    fn min_age_active(min_age: i32, active: bool) -> Vec<User> {
        User::filter(|u| u.age >= min_age && u.active == active)
            .order_by(|u| u.id)
            .all()
    }
    fn age_or(low: i32, high: i32) -> Vec<User> {
        User::filter(|u| u.age <= low || u.age >= high)
            .order_by(|u| u.id)
            .all()
    }
    fn nested(min_age: i32, max_age: i32, active: bool) -> Vec<User> {
        User::filter(|u| (u.age >= min_age && u.age <= max_age) || u.active == active)
            .order_by(|u| u.id)
            .all()
    }
    fn not_via_ne(active: bool) -> Vec<User> {
        User::filter(|u| u.active != active)
            .order_by(|u| u.id)
            .all()
    }
    fn in_ids(ids: &[i64]) -> Vec<User> {
        User::filter(|u| u.id.in_(ids)).order_by(|u| u.id).all()
    }
    fn not_in_ids(ids: &[i64]) -> Vec<User> {
        User::filter(|u| u.id.not_in(ids)).order_by(|u| u.id).all()
    }

    fn ordered_desc() -> Vec<i64> {
        User::filter(|_u| true)
            .order_by_desc(|u| u.age)
            .select(|u| u.id)
            .all()
    }
    fn limited() -> Vec<i64> {
        User::filter(|_u| true)
            .order_by(|u| u.id)
            .limit(2)
            .select(|u| u.id)
            .all()
    }
    fn offset() -> Vec<i64> {
        User::filter(|_u| true)
            .order_by(|u| u.id)
            .limit(2)
            .offset(2)
            .select(|u| u.id)
            .all()
    }
    fn distinct_ages() -> Vec<i32> {
        User::filter(|_u| true)
            .distinct()
            .order_by(|u| u.age)
            .select(|u| u.age)
            .all()
    }
    fn limit_param(n: i64) -> Vec<User> {
        User::filter(|_u| true).order_by(|u| u.id).limit(n).all()
    }
    fn distinct_on_active() -> Vec<User> {
        User::filter(|_u| true)
            .distinct_on(|u| u.active)
            .order_by(|u| u.active)
            .order_by(|u| u.id)
            .all()
    }
    fn ordered_by_expr() -> Vec<i64> {
        User::filter(|_u| true)
            .order_by_desc(|u| u.id + u.id)
            .select(|u| u.id)
            .all()
    }

    fn count_all() -> i64 {
        User::filter(|_u| true).count()
    }
    fn count_active(active: bool) -> i64 {
        User::filter(|u| u.active == active).count()
    }
    fn sum_ages() -> Option<i64> {
        User::filter(|_u| true).sum(|u| u.age)
    }
    fn avg_score() -> Option<f64> {
        User::filter(|_u| true).avg(|u| u.score)
    }
    fn min_age() -> Option<i32> {
        User::filter(|_u| true).min(|u| u.age)
    }
    fn max_score() -> Option<f64> {
        User::filter(|_u| true).max(|u| u.score)
    }
    fn sum_ages_arith(min: i32) -> Option<i64> {
        User::filter(|u| u.age >= min).sum(|u| u.age * 2)
    }

    fn union() -> Vec<User> {
        User::filter(|u| u.id <= 1)
            .union(User::filter(|u| u.id >= 4))
            .order_by(|u| u.id)
            .all()
    }
    fn union_all() -> Vec<User> {
        User::filter(|u| u.id <= 1)
            .union_all(User::filter(|u| u.id == 1))
            .order_by(|u| u.id)
            .all()
    }
    fn intersect() -> Vec<User> {
        User::filter(|u| u.age >= 25)
            .intersect(User::filter(|u| u.active == true))
            .order_by(|u| u.id)
            .all()
    }
    fn intersect_all() -> Vec<User> {
        User::filter(|u| u.age >= 25)
            .intersect_all(User::filter(|u| u.active == true))
            .order_by(|u| u.id)
            .all()
    }
    fn except() -> Vec<User> {
        User::filter(|_u| true)
            .except(User::filter(|u| u.active == false))
            .order_by(|u| u.id)
            .all()
    }
    fn except_all() -> Vec<User> {
        User::filter(|_u| true)
            .except_all(User::filter(|u| u.active == false))
            .order_by(|u| u.id)
            .all()
    }

    fn cte_active() -> Vec<User> {
        let active = User::filter(|u| u.active == true).cte();
        active.filter(|u| u.age >= 25).order_by(|u| u.id).all()
    }
    fn cte_union(min_age: i32) -> Vec<User> {
        let young = User::filter(|u| u.age < 30).cte();
        let actives = User::filter(|u| u.active == true).cte();
        young
            .filter(|u| u.age >= min_age)
            .union(actives.filter(|u| u.age >= min_age))
            .order_by(|u| u.id)
            .all()
    }
    fn cte_nested_chain() -> Vec<User> {
        let active = User::filter(|u| u.active == true).cte();
        let active_old = active.filter(|u| u.age >= 25).cte();
        active_old.filter(|_u| true).order_by(|u| u.id).all()
    }

    fn with_any_post() -> Vec<User> {
        User::filter(|u| exists(Post::filter(|p| p.author_id == u.id)))
            .order_by(|u| u.id)
            .all()
    }
    fn without_post() -> Vec<User> {
        User::filter(|u| not_exists(Post::filter(|p| p.author_id == u.id)))
            .order_by(|u| u.id)
            .all()
    }
    fn post_counts() -> Vec<(i64, i64)> {
        User::filter(|_u| true)
            .order_by(|u| u.id)
            .select(|u| (u.id, Post::filter(|p| p.author_id == u.id).count()))
            .all()
    }

    fn row_numbers() -> Vec<(i64, i64)> {
        User::filter(|_u| true)
            .order_by(|u| u.id)
            .select(|u| (u.id, row_number().over(|w| w.order_by(u.id))))
            .all()
    }
    fn lag_lead_ids() -> Vec<(i64, Option<i64>, Option<i64>)> {
        User::filter(|_u| true)
            .order_by(|u| u.id)
            .select(|u| {
                (
                    u.id,
                    lag::<_, i64>(u.id, 1).over(|w| w.order_by(u.id)),
                    lead::<_, i64>(u.id, 1).over(|w| w.order_by(u.id)),
                )
            })
            .all()
    }

    fn array_int4(ids: &[i32]) -> i64 {
        User::filter(|_u| array_length(ids, 1) > 0).count()
    }
    fn array_cardinality(ids: &[i32]) -> i64 {
        User::filter(|_u| cardinality(ids) > 0).count()
    }

    fn locked() -> Vec<i64> {
        User::filter(|u| u.id <= 2)
            .order_by(|u| u.id)
            .for_update()
            .select(|u| u.id)
            .all()
    }
    fn share_lock() -> Vec<i64> {
        User::filter(|u| u.id <= 2)
            .order_by(|u| u.id)
            .for_share()
            .select(|u| u.id)
            .all()
    }
}

#[query_group]
impl Post {
    fn titles_by_user(uid: i64) -> Vec<String> {
        User::join::<Post>(|u, p| u.id == p.author_id)
            .filter(|u, _p| u.id == uid)
            .select(|_u, p| p.title)
            .all()
    }
}

#[query_group]
impl Widget {
    fn count_per_bucket() -> Vec<(i32, i64)> {
        Widget::filter(|_w| true)
            .group_by(|w| w.bucket)
            .order_by(|w| w.bucket)
            .count()
    }
    fn max_per_bucket() -> Vec<(i32, Option<i64>)> {
        Widget::filter(|_w| true)
            .group_by(|w| w.bucket)
            .order_by(|w| w.bucket)
            .max(|w| w.value)
    }
    fn having(min: i64) -> Vec<(i32, i64)> {
        Widget::filter(|_w| true)
            .group_by(|w| w.bucket)
            .having(|_w, agg| agg.count() >= min)
            .order_by(|w| w.bucket)
            .count()
    }
    fn running_count() -> Vec<(i64, i64)> {
        Widget::filter(|_w| true)
            .order_by(|w| w.id)
            .select(|w| {
                (
                    w.id,
                    cartel_pg::dsl::count(w.id).over(|win| win.order_by(w.id)),
                )
            })
            .all()
    }
}

#[query_group]
impl WidgetSlim {
    fn insert_bulk(ids: &[i64], buckets: &[i32], values: &[i64]) {
        WidgetSlim::insert_each(|w| {
            w.id = ids;
            w.bucket = buckets;
            w.value = values;
        })
    }
    fn count_min_id_500() -> i64 {
        WidgetSlim::filter(|w| w.id >= 500).count()
    }
}

#[query_group]
impl Score {
    fn by_id(id: i32) -> Score {
        Score::filter(|s| s.id == id).one()
    }
    fn bulk_update(ids: &[i32], numbers: &[i32]) {
        Score::filter_each((&ids, &numbers), |s, id, n| s.id == id)
            .update(|s, id, n| s.randomnumber = n)
    }
}

#[query_group]
impl CopyRow {
    fn count_all() -> i64 {
        CopyRow::filter(|_c| true).count()
    }
}

#[query_group]
impl Item {
    fn by_key(a: i32, b: i32) -> Option<Item> {
        Item::filter(|i| i.a == a && i.b == b).first()
    }
    fn add(a: i32, b: i32, payload: String) {
        Item::insert(|i| {
            i.a = a;
            i.b = b;
            i.payload = payload;
        })
    }
}

struct Reports;

#[query_group]
impl Reports {
    fn user_with_posts() -> Vec<(User, Post)> {
        User::join::<Post>(|u, p| u.id == p.author_id)
            .filter(|_u, _p| true)
            .order_by(|_u, p| p.id)
            .all()
    }
    fn user_left_join_posts() -> i64 {
        User::left_join::<Post>(|u, p| u.id == p.author_id)
            .filter(|_u, _p| true)
            .count()
    }
    fn user_post_comment() -> Vec<(User, Post, Comment)> {
        User::join::<Post>(|u, p| u.id == p.author_id)
            .join::<Comment>(|_u, p, c| p.id == c.post_id)
            .filter(|_u, _p, _c| true)
            .order_by(|_u, _p, c| c.id)
            .all()
    }
    fn user_post_comment_tag() -> Vec<(User, Post, Comment, Tag)> {
        User::join::<Post>(|u, p| u.id == p.author_id)
            .join::<Comment>(|_u, p, c| p.id == c.post_id)
            .join::<Tag>(|_u, p, _c, t| p.id == t.post_id)
            .filter(|_u, _p, _c, _t| true)
            .order_by(|_u, _p, c, _t| c.id)
            .all()
    }
    fn count_user_posts() -> i64 {
        User::join::<Post>(|u, p| u.id == p.author_id)
            .filter(|_u, _p| true)
            .count()
    }
    fn lateral_top_post() -> Vec<(User, Post)> {
        User::lateral_join::<Post>(|u| {
            Post::filter(|p| p.author_id == u.id)
                .order_by_desc(|p| p.likes)
                .limit(1)
        })
        .filter(|_u, _p| true)
        .order_by(|u, _p| u.id)
        .all()
    }
    fn lateral_left_top_post() -> i64 {
        User::lateral_left_join::<Post>(|u| {
            Post::filter(|p| p.author_id == u.id)
                .order_by_desc(|p| p.likes)
                .limit(1)
        })
        .filter(|_u, _p| true)
        .count()
    }
}

#[query_group]
impl Doc {
    fn fts_match(needle: String) -> Vec<i64> {
        Doc::filter(|d| to_tsvector(d.body).fts_match(plainto_tsquery(needle)))
            .order_by(|d| d.id)
            .select(|d| d.id)
            .all()
    }
    fn fts_to_tsquery(query: String) -> Vec<i64> {
        Doc::filter(|d| to_tsvector(d.body).fts_match(to_tsquery(query)))
            .order_by(|d| d.id)
            .select(|d| d.id)
            .all()
    }
    fn ts_rank_q(needle: String, needle2: String) -> Vec<(i64, f32)> {
        Doc::filter(|d| to_tsvector(d.body).fts_match(plainto_tsquery(needle)))
            .order_by(|d| d.id)
            .select(|d| (d.id, ts_rank(to_tsvector(d.body), plainto_tsquery(needle2))))
            .all()
    }
}

#[query_group]
impl AllTypes {
    fn i32_arr_contains(rhs: &[i32]) -> Vec<i64> {
        AllTypes::filter(|r| r.arr_i32.pg_contains(rhs))
            .order_by(|r| r.id)
            .select(|r| r.id)
            .all()
    }
    fn i32_arr_overlaps(rhs: &[i32]) -> Vec<i64> {
        AllTypes::filter(|r| r.arr_i32.pg_overlaps(rhs))
            .order_by(|r| r.id)
            .select(|r| r.id)
            .all()
    }
    fn str_arr_contains(rhs: &[&str]) -> Vec<i64> {
        AllTypes::filter(|r| r.arr_str.pg_contains(rhs))
            .order_by(|r| r.id)
            .select(|r| r.id)
            .all()
    }
    fn str_arr_overlaps(rhs: &[&str]) -> Vec<i64> {
        AllTypes::filter(|r| r.arr_str.pg_overlaps(rhs))
            .order_by(|r| r.id)
            .select(|r| r.id)
            .all()
    }
    fn i32_arr_length() -> Vec<(i64, i32)> {
        AllTypes::filter(|_r| true)
            .order_by(|r| r.id)
            .select(|r| (r.id, array_length(r.arr_i32, 1)))
            .all()
    }
    fn str_arr_cardinality() -> Vec<(i64, i32)> {
        AllTypes::filter(|_r| true)
            .order_by(|r| r.id)
            .select(|r| (r.id, cardinality(r.arr_str)))
            .all()
    }
    fn by_id(id: i64) -> AllTypes {
        AllTypes::filter(|r| r.id == id).one()
    }
    fn arrays_by_id(id: i64) -> (Vec<i32>, Vec<i64>, Vec<String>) {
        AllTypes::filter(|r| r.id == id)
            .select(|r| (r.arr_i32, r.arr_i64, r.arr_str))
            .one()
    }
    fn update_ints_by_id(id: i64, v_i16: i16, v_i32: i32, v_i64: i64, v_bool: bool) {
        AllTypes::filter(|r| r.id == id).update(|r| {
            r.v_i16 = v_i16;
            r.v_i32 = v_i32;
            r.v_i64 = v_i64;
            r.v_bool = v_bool;
        })
    }
    fn update_strs_by_id(id: i64, v_f32: f32, v_f64: f64, v_string: String, v_bytes: Vec<u8>) {
        AllTypes::filter(|r| r.id == id).update(|r| {
            r.v_f32 = v_f32;
            r.v_f64 = v_f64;
            r.v_string = v_string;
            r.v_bytes = v_bytes;
        })
    }
    fn update_temporal_by_id(id: i64, v_uuid: Uuid, v_ts: Timestamp, v_date: Date, v_ltree: Ltree) {
        AllTypes::filter(|r| r.id == id).update(|r| {
            r.v_uuid = v_uuid;
            r.v_ts = v_ts;
            r.v_date = v_date;
            r.v_ltree = v_ltree;
        })
    }
}

#[query_group]
impl LtreeNode {
    fn descendants(of: Ltree) -> Vec<i64> {
        LtreeNode::filter(|n| n.path.is_descendant_of(of))
            .order_by(|n| n.id)
            .select(|n| n.id)
            .all()
    }
    fn ancestors(of: Ltree) -> Vec<i64> {
        LtreeNode::filter(|n| n.path.is_ancestor_of(of))
            .order_by(|n| n.id)
            .select(|n| n.id)
            .all()
    }
}

pg_instance! {
    Db: User, Post, Widget, WidgetSlim, Score, Item, CopyRow, Reports, Doc, AllTypes, LtreeNode
}

pg_instance! { Boot: }

struct Rt<I: cartel_pg::QuerySet + 'static> {
    // drops before exec: socket close needs the live io_uring driver
    dispatcher: Pin<Box<PgDispatcher<I>>>,
    exec: Box<Executor>,
    schema: String,
    drop_schema: bool,
}

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct PgDispatcher<I: cartel_pg::QuerySet + 'static> {
    #[pin]
    #[manifold]
    pg: PgConn<I>,
}

struct EnvCfg {
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
}

impl EnvCfg {
    fn load() -> Self {
        let get = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
        Self {
            host: get("PG_HOST", "127.0.0.1"),
            port: get("PG_PORT", "5432").parse().expect("PG_PORT"),
            user: get("PG_USER", "bench"),
            password: get("PG_PASSWORD", "bench"),
            database: get("PG_DATABASE", "bench"),
        }
    }

    fn tcp_addr(&self) -> SocketAddr {
        format!("{}:{}", self.host, self.port)
            .parse()
            .expect("PG addr")
    }

    fn cartel_config(&self) -> cartel_pg::Config {
        cartel_pg::Config::new(
            self.user.clone(),
            self.password.clone(),
            self.database.clone(),
        )
    }
}

impl<I: cartel_pg::QuerySet + 'static> Rt<I> {
    fn new(
        addr: SocketAddr,
        cfg: cartel_pg::Config,
        max_conn: usize,
        schema: String,
        drop_schema: bool,
    ) -> std::io::Result<Self> {
        let mut exec = Box::new(Executor::new(DriverCfg::for_tcp_profile::<Throughput>(8))?);
        let driver = exec.driver_mut();
        let session = cartel_pg::Session::<I>::new(cfg.with_search_path(&schema));
        let upstreams = Static::<Tcp>::new(
            vec![addr; max_conn.max(1)],
            std::time::Duration::from_millis(500),
        );
        let dispatcher = Box::pin(PgDispatcher {
            pg: PgConn::new(session, upstreams, max_conn, driver),
        });
        let mut this = Self {
            exec,
            dispatcher,
            schema,
            drop_schema,
        };
        this.block_on(std::future::ready(()));
        Ok(this)
    }

    fn client<'a>(&mut self) -> CartelClient<'a, I> {
        // SAFETY: the dispatcher box is never moved; projecting `pg` keeps it pinned in place.
        let dispatcher = unsafe { Pin::get_unchecked_mut(self.dispatcher.as_mut()) };
        let pg = &mut dispatcher.pg as *mut PgConn<I>;
        // SAFETY: the connector is pinned inside the dispatcher box and never moved while the minted holding is alive; thread-per-core.
        dope::fiber::Holding::of(unsafe { Pin::new_unchecked(&mut *pg) })
    }

    fn block_on<F: Future>(&mut self, fut: F) -> F::Output {
        dope_extra::block_on(
            &mut self.exec,
            self.dispatcher.as_mut(),
            dope::fiber::Fiber::new(fut),
        )
    }
}

impl<I: cartel_pg::QuerySet + 'static> Drop for Rt<I> {
    fn drop(&mut self) {
        if std::thread::panicking() || !self.drop_schema {
            return;
        }
        let client = self.client();
        if client.is_failed() {
            return;
        }
        let drop_sql = format!("DROP SCHEMA IF EXISTS {} CASCADE", self.schema);
        self.block_on(async move {
            let _ = client.execute_raw(&drop_sql).await;
        });
    }
}

async fn wait_ready<I: cartel_pg::QuerySet + 'static>(c: CartelClient<'_, I>, n: usize) {
    std::future::poll_fn(move |_cx| {
        if c.is_failed() {
            panic!("cartel-pg: connection failed");
        }
        if c.live_count() >= n {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await
}

fn unique_suffix() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static C: AtomicU64 = AtomicU64::new(0);
    let n = C.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{pid}_{nanos}_{n}")
}

const SCHEMA_STATEMENTS: &[&str] = &[
    "CREATE TABLE cartel_pg_users (
        id BIGINT PRIMARY KEY,
        name TEXT NOT NULL,
        age INTEGER NOT NULL,
        score DOUBLE PRECISION NOT NULL,
        active BOOLEAN NOT NULL,
        nickname TEXT,
        avatar BYTEA
     )",
    "CREATE TABLE cartel_pg_posts (
        id BIGINT PRIMARY KEY,
        author_id BIGINT NOT NULL,
        title TEXT NOT NULL,
        body TEXT NOT NULL,
        likes BIGINT NOT NULL DEFAULT 0
     )",
    "CREATE TABLE cartel_pg_comments (
        id BIGINT PRIMARY KEY,
        post_id BIGINT NOT NULL,
        author_id BIGINT NOT NULL,
        text TEXT NOT NULL
     )",
    "CREATE TABLE cartel_pg_tags (
        id BIGINT PRIMARY KEY,
        post_id BIGINT NOT NULL,
        label TEXT NOT NULL
     )",
    "CREATE TABLE cartel_pg_widgets (
        id BIGINT PRIMARY KEY,
        bucket INTEGER NOT NULL,
        value BIGINT NOT NULL
     )",
    "CREATE TABLE cartel_pg_items (
        a INTEGER NOT NULL,
        b INTEGER NOT NULL,
        payload TEXT NOT NULL,
        PRIMARY KEY (a, b)
     )",
    "CREATE TABLE cartel_pg_types (
        id BIGINT PRIMARY KEY,
        v_i16 SMALLINT NOT NULL,
        v_i32 INTEGER NOT NULL,
        v_i64 BIGINT NOT NULL,
        v_f32 REAL NOT NULL,
        v_f64 DOUBLE PRECISION NOT NULL,
        v_bool BOOLEAN NOT NULL,
        v_string TEXT NOT NULL,
        v_bytes BYTEA NOT NULL,
        v_uuid UUID NOT NULL,
        v_ts TIMESTAMP NOT NULL,
        v_date DATE NOT NULL,
        v_ltree LTREE NOT NULL,
        o_i32 INTEGER,
        o_i64 BIGINT,
        o_bool BOOLEAN,
        o_string TEXT,
        o_bytes BYTEA,
        o_uuid UUID,
        arr_i32 INTEGER[] NOT NULL,
        arr_i64 BIGINT[] NOT NULL,
        arr_str TEXT[] NOT NULL
     )",
    "CREATE TABLE cartel_pg_docs (
        id BIGINT PRIMARY KEY,
        body TEXT NOT NULL,
        payload BYTEA NOT NULL
     )",
    "CREATE TABLE cartel_pg_nodes (
        id BIGINT PRIMARY KEY,
        path LTREE NOT NULL
     )",
    "CREATE TABLE cartel_pg_copy (
        id BIGINT PRIMARY KEY,
        label TEXT NOT NULL
     )",
    "CREATE TABLE cartel_pg_scores (
        id INTEGER PRIMARY KEY,
        randomnumber INTEGER NOT NULL
     )",
];

const SEED_STATEMENTS: &[&str] = &[
    "INSERT INTO cartel_pg_users (id, name, age, score, active, nickname, avatar) VALUES
        (1, 'alice', 30, 9.5, TRUE, 'al',  NULL),
        (2, 'bob',   25, 7.0, TRUE, NULL,  NULL),
        (3, 'carol', 40, 8.2, FALSE,'caz', E'\\\\xcafe'),
        (4, 'dave',  22, 6.1, TRUE, NULL,  E'\\\\xdeadbeef'),
        (5, '한국어 √',29,5.0,TRUE,'한',   NULL)",
    "INSERT INTO cartel_pg_posts (id, author_id, title, body, likes) VALUES
        (10, 1, 'hello',  'world', 3),
        (11, 1, 'second', 'post', 1),
        (12, 2, 'bob-1',  'b-body', 7),
        (13, 3, 'carol',  'c-body', 0)",
    "INSERT INTO cartel_pg_comments (id, post_id, author_id, text) VALUES
        (100, 10, 2, 'nice'),
        (101, 10, 3, 'cool'),
        (102, 12, 1, 'lol')",
    "INSERT INTO cartel_pg_tags (id, post_id, label) VALUES
        (1000, 10, 'rust'),
        (1001, 10, 'sql'),
        (1002, 12, 'rust')",
    "INSERT INTO cartel_pg_widgets (id, bucket, value) VALUES
        (1, 1, 10), (2, 1, 20), (3, 2, 30), (4, 2, 40), (5, 3, 50)",
    "INSERT INTO cartel_pg_docs (id, body, payload) VALUES
        (1, 'the quick brown fox jumps over the lazy dog',
            E'\\\\x7b226b6579223a2276616c75652c226e223a317d'::bytea),
        (2, 'quick rust queries with cartel-pg are excellent',
            E'\\\\x7b226b6579223a2274776f2c226e223a327d'::bytea),
        (3, 'PostgreSQL full text search shines',
            E'\\\\x7b226b6579223a2274686972642c226e223a337d'::bytea)",
    "INSERT INTO cartel_pg_nodes (id, path) VALUES
        (1, 'top'),
        (2, 'top.a'),
        (3, 'top.a.b'),
        (4, 'top.c')",
    "INSERT INTO cartel_pg_scores (id, randomnumber) VALUES
        (1, 100), (2, 200), (3, 300), (4, 400), (5, 500)",
];

fn provision_schema(addr: SocketAddr, cfg: cartel_pg::Config, schema: &str) {
    let mut boot =
        Rt::<Boot>::new(addr, cfg, 1, schema.to_string(), false).expect("create bootstrap runtime");
    let client = boot.client();
    boot.block_on(wait_ready(client, 1));
    let create = format!("CREATE SCHEMA {schema}");
    boot.block_on(async move {
        client.execute_raw(&create).await.expect("create schema");
        client
            .migrate(SCHEMA_STATEMENTS)
            .await
            .expect("migrate schema");
        client.migrate(SEED_STATEMENTS).await.expect("seed");
    });
}

fn boot() -> Rt<Db> {
    boot_with(1)
}

fn boot_with(max_conn: usize) -> Rt<Db> {
    let env = EnvCfg::load();
    let addr = env.tcp_addr();
    let schema = format!("cartel_pg_test_{}", unique_suffix());
    provision_schema(addr, env.cartel_config(), &schema);
    let mut rt =
        Rt::<Db>::new(addr, env.cartel_config(), max_conn, schema, true).expect("create runtime");
    let client = rt.client();
    rt.block_on(wait_ready(client, max_conn));
    rt
}

#[test]
fn dispatch_kinds() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        let u = User::by_id(&client, 1).await.unwrap();
        assert_eq!(u.id, 1);
        assert_eq!(u.name, "alice");
        assert_eq!(u.age, 30);
        assert!((u.score - 9.5).abs() < 1e-9);
        assert!(u.active);
        assert_eq!(u.nickname.as_deref(), Some("al"));
        assert!(u.avatar.is_none());

        let err = User::by_id(&client, 999).await.unwrap_err();
        assert!(matches!(err, cartel_pg::Error::NotFound));

        assert!(User::maybe_by_id(&client, 999).await.unwrap().is_none());
        assert!(User::maybe_by_id(&client, 2).await.unwrap().is_some());

        let all = User::all_rows(&client).await.unwrap();
        assert_eq!(all.len(), 5);

        let names = User::names_min_id(&client, 3).await.unwrap();
        assert_eq!(names, vec!["carol", "dave", "한국어 √"]);

        let pairs = User::id_name_pairs(&client).await.unwrap();
        assert_eq!(pairs.len(), 5);
        assert_eq!(pairs[0], (1, "alice".to_string()));

        assert_eq!(User::id_plus_one(&client, 3).await.unwrap(), 4);

        let mut stream = User::all_stream(&client);
        let mut streamed = Vec::new();
        while let Some(u) = stream.next_row().await.unwrap() {
            streamed.push(u);
        }
        assert_eq!(streamed.len(), 5);
        assert_eq!(streamed[0].id, 1);
    });
}

#[test]
fn type_roundtrips_basic() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        let row = User::blob(&client, 3).await.unwrap();
        assert_eq!(row, vec![Some(vec![0xcau8, 0xfe])]);
        let row = User::blob(&client, 4).await.unwrap();
        assert_eq!(row, vec![Some(vec![0xdeu8, 0xad, 0xbe, 0xef])]);
        let row = User::blob(&client, 1).await.unwrap();
        assert_eq!(row, vec![None]);
        assert_eq!(User::unicode_name(&client).await.unwrap(), "한국어 √");
    });
}

#[test]
fn update_and_delete() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        User::rename(&client, 2, "robert".into()).await.unwrap();
        assert_eq!(User::by_id(&client, 2).await.unwrap().name, "robert");

        User::set_fields(&client, 2, "bobby".into(), 26, 7.5)
            .await
            .unwrap();
        let u = User::by_id(&client, 2).await.unwrap();
        assert_eq!(u.name, "bobby");
        assert_eq!(u.age, 26);
        assert!((u.score - 7.5).abs() < 1e-9);

        User::remove(&client, 4).await.unwrap();
        assert!(User::maybe_by_id(&client, 4).await.unwrap().is_none());

        User::reset_age_to_zero(&client, 1).await.unwrap();
        assert_eq!(User::by_id(&client, 1).await.unwrap().age, 0);
    });
}

#[test]
fn insert_variants() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        User::add(&client, 100, "ned".into(), 50, 9.0, true)
            .await
            .unwrap();
        assert_eq!(User::by_id(&client, 100).await.unwrap().name, "ned");

        let rows = User::insert_returning_all(&client, 101, "olive".into(), 0, 0.0, true)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, 101);
        assert_eq!(rows[0].name, "olive");

        let row = User::insert_returning_one(&client, 102, "pete".into(), 0, 0.0, true)
            .await
            .unwrap();
        assert_eq!(row.id, 102);

        let row = User::insert_returning_first(&client, 103, "quinn".into(), 0, 0.0, true)
            .await
            .unwrap();
        assert_eq!(row.unwrap().id, 103);

        User::copy_min_age(&client, 30, 1000).await.unwrap();
        let names: Vec<String> = {
            let mut acc = Vec::new();
            for id in 1001..=1005 {
                if let Some(u) = User::maybe_by_id(&client, id).await.unwrap() {
                    acc.push(u.name);
                }
            }
            acc
        };
        assert!(names.contains(&"alice".to_string()));
        assert!(names.contains(&"carol".to_string()));

        User::insert_literal_age(&client, 200, "ulysses".into())
            .await
            .unwrap();
        let u = User::by_id(&client, 200).await.unwrap();
        assert_eq!(u.age, 7);
        assert!((u.score - 1.25).abs() < 1e-9);
    });
}

#[test]
fn insert_on_conflict() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        User::insert_on_conflict_do_nothing(&client, 1, "ignored".into(), 0, 0.0, true)
            .await
            .unwrap();
        assert_eq!(User::by_id(&client, 1).await.unwrap().name, "alice");

        User::upsert_name(&client, 1, "ignored".into(), "ALICE".into(), 0, 0.0, true)
            .await
            .unwrap();
        assert_eq!(User::by_id(&client, 1).await.unwrap().name, "ALICE");

        User::upsert_name(&client, 222, "new".into(), "new-upd".into(), 0, 0.0, true)
            .await
            .unwrap();
        assert_eq!(User::by_id(&client, 222).await.unwrap().name, "new");

        User::insert_target_do_nothing(&client, 1, "still-ignored".into(), 0, 0.0, true)
            .await
            .unwrap();
        assert_eq!(User::by_id(&client, 1).await.unwrap().name, "ALICE");
    });
}

#[test]
fn returning_clauses() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        let u = User::update_returning(&client, 1, "AAA".into())
            .await
            .unwrap();
        assert_eq!(u.name, "AAA");

        let u = User::delete_returning(&client, 2).await.unwrap().unwrap();
        assert_eq!(u.name, "bob");

        let removed = User::delete_all_inactive_returning(&client).await.unwrap();
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].id, 3);
    });
}

#[test]
fn predicates() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        assert_eq!(User::eq_age(&client, 25).await.unwrap().len(), 1);
        assert_eq!(User::ne_age(&client, 25).await.unwrap().len(), 4);
        assert_eq!(User::gt_age(&client, 30).await.unwrap().len(), 1);
        assert_eq!(User::lt_age(&client, 30).await.unwrap().len(), 3);
        assert_eq!(User::ge_age(&client, 30).await.unwrap().len(), 2);
        assert_eq!(User::le_age(&client, 30).await.unwrap().len(), 4);
        assert_eq!(User::like(&client, "a%".into()).await.unwrap().len(), 1);
        assert_eq!(User::not_like(&client, "a%".into()).await.unwrap().len(), 4);
        assert_eq!(User::ilike(&client, "A%".into()).await.unwrap().len(), 1);
        assert_eq!(
            User::not_ilike(&client, "A%".into()).await.unwrap().len(),
            4
        );
        assert_eq!(User::regex(&client, "^[ab]".into()).await.unwrap().len(), 2);
        assert_eq!(
            User::regex_i(&client, "^[AB]".into()).await.unwrap().len(),
            2
        );
        assert_eq!(
            User::not_regex(&client, "^[ab]".into())
                .await
                .unwrap()
                .len(),
            3
        );
        assert_eq!(
            User::not_regex_i(&client, "^[AB]".into())
                .await
                .unwrap()
                .len(),
            3
        );
        assert_eq!(User::nickname_some(&client).await.unwrap().len(), 3);
        assert_eq!(User::nickname_none(&client).await.unwrap().len(), 2);
        assert_eq!(
            User::min_age_active(&client, 25, true).await.unwrap().len(),
            3
        );
        assert_eq!(User::age_or(&client, 22, 40).await.unwrap().len(), 2);
        assert_eq!(User::nested(&client, 25, 35, true).await.unwrap().len(), 4);
        assert_eq!(User::not_via_ne(&client, true).await.unwrap().len(), 1);

        let ids = vec![1i64, 3, 5];
        let r = User::in_ids(&client, &ids).await.unwrap();
        assert_eq!(r.iter().map(|u| u.id).collect::<Vec<_>>(), vec![1, 3, 5]);
        let r = User::not_in_ids(&client, &ids).await.unwrap();
        assert_eq!(r.iter().map(|u| u.id).collect::<Vec<_>>(), vec![2, 4]);
    });
}

#[test]
fn ordering_and_limits() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        let ids = User::ordered_desc(&client).await.unwrap();
        assert_eq!(ids, vec![3, 1, 5, 2, 4]);
        assert_eq!(User::limited(&client).await.unwrap(), vec![1, 2]);
        assert_eq!(User::offset(&client).await.unwrap(), vec![3, 4]);
        let mut ages = User::distinct_ages(&client).await.unwrap();
        ages.sort();
        assert_eq!(ages, vec![22, 25, 29, 30, 40]);
        assert_eq!(User::limit_param(&client, 3).await.unwrap().len(), 3);
        let dist_on = User::distinct_on_active(&client).await.unwrap();
        assert_eq!(dist_on.len(), 2);
        let ord_expr = User::ordered_by_expr(&client).await.unwrap();
        assert_eq!(ord_expr.len(), 5);
        assert_eq!(ord_expr[0], 5);
    });
}

#[test]
fn aggregates() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        assert_eq!(User::count_all(&client).await.unwrap(), 5);
        assert_eq!(User::count_active(&client, true).await.unwrap(), 4);
        assert_eq!(
            User::sum_ages(&client).await.unwrap(),
            Some(30 + 25 + 40 + 22 + 29)
        );
        let avg = User::avg_score(&client).await.unwrap().unwrap();
        assert!((avg - (9.5 + 7.0 + 8.2 + 6.1 + 5.0) / 5.0).abs() < 1e-6);
        assert_eq!(User::min_age(&client).await.unwrap(), Some(22));
        let m = User::max_score(&client).await.unwrap().unwrap();
        assert!((m - 9.5).abs() < 1e-9);
        assert_eq!(
            Widget::count_per_bucket(&client).await.unwrap(),
            vec![(1, 2), (2, 2), (3, 1)]
        );
        let maxes = Widget::max_per_bucket(&client).await.unwrap();
        assert_eq!(maxes, vec![(1, Some(20)), (2, Some(40)), (3, Some(50))]);
        assert_eq!(
            Widget::having(&client, 2).await.unwrap(),
            vec![(1, 2), (2, 2)]
        );
    });
}

#[test]
fn joins() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        let rows = Reports::user_with_posts(&client).await.unwrap();
        assert_eq!(rows.len(), 4);

        let n = Reports::user_left_join_posts(&client).await.unwrap();
        assert!(n >= 5);

        let rows3 = Reports::user_post_comment(&client).await.unwrap();
        assert_eq!(rows3.len(), 3);

        let rows4 = Reports::user_post_comment_tag(&client).await.unwrap();
        assert!(!rows4.is_empty());

        assert_eq!(Reports::count_user_posts(&client).await.unwrap(), 4);

        let titles = Post::titles_by_user(&client, 1).await.unwrap();
        assert_eq!(titles, vec!["hello", "second"]);
    });
}

#[test]
fn lateral_joins() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        let rows = Reports::lateral_top_post(&client).await.unwrap();
        assert_eq!(rows.len(), 3);
        for (u, p) in &rows {
            assert_eq!(p.author_id, u.id);
        }
        let n = Reports::lateral_left_top_post(&client).await.unwrap();
        assert!(n >= 5);
    });
}

#[test]
fn set_ops() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        let r = User::union(&client).await.unwrap();
        assert_eq!(r.len(), 3);
        let ids: Vec<i64> = r.iter().map(|u| u.id).collect();
        assert_eq!(ids, vec![1, 4, 5]);

        let r = User::union_all(&client).await.unwrap();
        assert_eq!(r.len(), 2);

        let r = User::intersect(&client).await.unwrap();
        assert_eq!(r.len(), 3);
        let r = User::intersect_all(&client).await.unwrap();
        assert_eq!(r.len(), 3);

        let r = User::except(&client).await.unwrap();
        assert_eq!(r.len(), 4);
        let r = User::except_all(&client).await.unwrap();
        assert_eq!(r.len(), 4);
    });
}

#[test]
fn ctes() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        let r = User::cte_active(&client).await.unwrap();
        let ids: Vec<i64> = r.iter().map(|u| u.id).collect();
        assert_eq!(ids, vec![1, 2, 5]);

        let r = User::cte_union(&client, 22).await.unwrap();
        let ids: Vec<i64> = r.iter().map(|u| u.id).collect();
        assert!(ids.contains(&2));
        assert!(ids.contains(&4));
        assert!(ids.contains(&5));

        let r = User::cte_nested_chain(&client).await.unwrap();
        let ids: Vec<i64> = r.iter().map(|u| u.id).collect();
        assert_eq!(ids, vec![1, 2, 5]);
    });
}

#[test]
fn subqueries() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        let r = User::with_any_post(&client).await.unwrap();
        let ids: Vec<i64> = r.iter().map(|u| u.id).collect();
        assert_eq!(ids, vec![1, 2, 3]);

        let r = User::without_post(&client).await.unwrap();
        let ids: Vec<i64> = r.iter().map(|u| u.id).collect();
        assert_eq!(ids, vec![4, 5]);

        let counts = User::post_counts(&client).await.unwrap();
        assert_eq!(counts, vec![(1, 2), (2, 1), (3, 1), (4, 0), (5, 0)]);
    });
}

#[test]
fn composite_pk_and_default_table_name() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        Item::add(&client, 1, 2, "hi".into()).await.unwrap();
        Item::add(&client, 1, 3, "ho".into()).await.unwrap();
        let it = Item::by_key(&client, 1, 2).await.unwrap().unwrap();
        assert_eq!(it.payload, "hi");
        assert!(Item::by_key(&client, 2, 2).await.unwrap().is_none());

        assert_eq!(Item::__CARTEL_PK_COL, "a,b");
        assert_eq!(Item::__CARTEL_TABLE, "cartel_pg_items");
        assert_eq!(Widget::__CARTEL_TABLE, "cartel_pg_widgets");
    });
}

#[test]
fn arithmetic_in_aggregate() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        let s = User::sum_ages_arith(&client, 0).await.unwrap();
        assert_eq!(s, Some((30 + 25 + 40 + 22 + 29) * 2));
    });
}

#[test]
fn window_functions() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        let rows = User::row_numbers(&client).await.unwrap();
        assert_eq!(rows.len(), 5);
        let rns: Vec<i64> = rows.iter().map(|(_, n)| *n).collect();
        assert_eq!(rns, vec![1, 2, 3, 4, 5]);

        let ll = User::lag_lead_ids(&client).await.unwrap();
        assert_eq!(ll.len(), 5);
        assert_eq!(ll[0].1, None);
        assert_eq!(ll[1].1, Some(1));
        assert_eq!(ll[4].2, None);

        let rs = Widget::running_count(&client).await.unwrap();
        assert_eq!(rs.len(), 5);
        let totals: Vec<i64> = rs.iter().map(|(_, t)| *t).collect();
        assert_eq!(totals, vec![1, 2, 3, 4, 5]);
    });
}

#[test]
fn full_text_search() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        let r = Doc::fts_match(&client, "quick".into()).await.unwrap();
        assert_eq!(r, vec![1, 2]);
        let r = Doc::fts_to_tsquery(&client, "rust & cartel".into())
            .await
            .unwrap();
        assert_eq!(r, vec![2]);
        let r = Doc::ts_rank_q(&client, "quick".into(), "quick".into())
            .await
            .unwrap();
        assert_eq!(r.len(), 2);
        for (_, rank) in &r {
            assert!(*rank >= 0.0);
        }
    });
}

#[test]
fn pg_array_op_native_semantics() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        client
            .execute_raw("DELETE FROM cartel_pg_types")
            .await
            .unwrap();
        client
            .execute_raw(
                "INSERT INTO cartel_pg_types
                (id, v_i16, v_i32, v_i64, v_f32, v_f64, v_bool, v_string, v_bytes,
                 v_uuid, v_ts, v_date, v_ltree,
                 arr_i32, arr_i64, arr_str)
             VALUES
                (10, 0, 0, 0, 0.0, 0.0, TRUE, 'a', E'\\\\x',
                 '11111111-1111-1111-1111-111111111111'::uuid,
                 NOW(), CURRENT_DATE, 'r'::ltree,
                 ARRAY[1,2,3]::int4[], ARRAY[]::int8[], ARRAY['rust','sql']::text[]),
                (11, 0, 0, 0, 0.0, 0.0, TRUE, 'b', E'\\\\x',
                 '22222222-2222-2222-2222-222222222222'::uuid,
                 NOW(), CURRENT_DATE, 'r'::ltree,
                 ARRAY[2,3,4]::int4[], ARRAY[]::int8[], ARRAY['sql','tree']::text[]),
                (12, 0, 0, 0, 0.0, 0.0, TRUE, 'c', E'\\\\x',
                 '33333333-3333-3333-3333-333333333333'::uuid,
                 NOW(), CURRENT_DATE, 'r'::ltree,
                 ARRAY[7,8,9]::int4[], ARRAY[]::int8[], ARRAY['cake']::text[])",
            )
            .await
            .unwrap();

        let rhs: &[i32] = &[2, 3];
        let r = AllTypes::i32_arr_contains(&client, rhs).await.unwrap();
        assert_eq!(r, vec![10, 11]);

        let rhs: &[i32] = &[2, 99];
        let r = AllTypes::i32_arr_contains(&client, rhs).await.unwrap();
        assert!(r.is_empty());

        let rhs: &[i32] = &[3, 4];
        let r = AllTypes::i32_arr_overlaps(&client, rhs).await.unwrap();
        assert_eq!(r, vec![10, 11]);

        let rhs: &[i32] = &[99, 100];
        let r = AllTypes::i32_arr_overlaps(&client, rhs).await.unwrap();
        assert!(r.is_empty());

        let rhs: &[&str] = &["sql"];
        let r = AllTypes::str_arr_contains(&client, rhs).await.unwrap();
        assert_eq!(r, vec![10, 11]);

        let rhs: &[&str] = &["rust", "missing"];
        let r = AllTypes::str_arr_contains(&client, rhs).await.unwrap();
        assert!(r.is_empty());

        let rhs: &[&str] = &["cake", "missing"];
        let r = AllTypes::str_arr_overlaps(&client, rhs).await.unwrap();
        assert_eq!(r, vec![12]);

        let r = AllTypes::i32_arr_length(&client).await.unwrap();
        assert_eq!(r, vec![(10, 3), (11, 3), (12, 3)]);

        let r = AllTypes::str_arr_cardinality(&client).await.unwrap();
        assert_eq!(r, vec![(10, 2), (11, 2), (12, 1)]);
    });
}

#[test]
fn ltree_ops() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        let r = LtreeNode::descendants(&client, Ltree::new("top.a"))
            .await
            .unwrap();
        assert_eq!(r, vec![2, 3]);
        let r = LtreeNode::ancestors(&client, Ltree::new("top.a.b"))
            .await
            .unwrap();
        assert_eq!(r, vec![1, 2, 3]);
    });
}

#[test]
fn array_length_and_cardinality() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        let ids = [1i32, 2, 3];
        let c = User::array_int4(&client, &ids).await.unwrap();
        assert_eq!(c, 5);
        let c = User::array_cardinality(&client, &ids).await.unwrap();
        assert_eq!(c, 5);
    });
}

#[test]
fn row_lock_clauses() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        let tx = client.begin().await.unwrap();
        let r = User::locked(&tx).await.unwrap();
        assert_eq!(r, vec![1, 2]);
        let r = User::share_lock(&tx).await.unwrap();
        assert_eq!(r, vec![1, 2]);
        tx.commit().await.unwrap();
    });
}

#[test]
fn insert_each_bulk() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        let ids = [500i64, 501, 502];
        let buckets = [10i32, 11, 12];
        let values = [100i64, 200, 300];
        WidgetSlim::insert_bulk(&client, &ids, &buckets, &values)
            .await
            .unwrap();
        let n = WidgetSlim::count_min_id_500(&client).await.unwrap();
        assert_eq!(n, 3);
    });
}

#[test]
fn bulk_update_filter_each() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        let tx = client.begin().await.unwrap();
        let ids = [1i32, 3, 5];
        let numbers = [111i32, 333, 555];
        Score::bulk_update(&tx, &ids, &numbers).await.unwrap();
        assert_eq!(Score::by_id(&tx, 1).await.unwrap().randomnumber, 111);
        assert_eq!(Score::by_id(&tx, 2).await.unwrap().randomnumber, 200);
        assert_eq!(Score::by_id(&tx, 3).await.unwrap().randomnumber, 333);
        assert_eq!(Score::by_id(&tx, 4).await.unwrap().randomnumber, 400);
        assert_eq!(Score::by_id(&tx, 5).await.unwrap().randomnumber, 555);
        tx.commit().await.unwrap();
        assert_eq!(Score::by_id(&client, 3).await.unwrap().randomnumber, 333);
    });
}

#[test]
fn raw_typed_query() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        let v = User::id_literal(&client, 1).await.unwrap();
        assert_eq!(v, 1);
    });
}

#[test]
fn tx_closure_commit_and_rollback() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        let committed = client
            .tx(async |tx| {
                User::rename(tx, 1, "closure-committed".into()).await?;
                User::by_id(tx, 1).await
            })
            .await
            .unwrap();
        assert_eq!(committed.name, "closure-committed");
        assert_eq!(
            User::by_id(&client, 1).await.unwrap().name,
            "closure-committed"
        );

        let rolled = client
            .tx(async |tx| -> Result<(), cartel_pg::Error> {
                User::rename(tx, 1, "closure-failed".into()).await?;
                Err(cartel_pg::Error::Other("simulate failure".into()))
            })
            .await;
        assert!(matches!(rolled, Err(cartel_pg::Error::Other(_))));
        assert_eq!(
            User::by_id(&client, 1).await.unwrap().name,
            "closure-committed"
        );
    });
}

#[test]
fn tx_commit_and_rollback() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        {
            let tx = client.begin().await.unwrap();
            User::rename(&tx, 1, "tx-renamed".into()).await.unwrap();
            assert_eq!(User::by_id(&tx, 1).await.unwrap().name, "tx-renamed");
            tx.rollback().await.unwrap();
        }
        assert_eq!(User::by_id(&client, 1).await.unwrap().name, "alice");

        {
            let tx = client.begin().await.unwrap();
            User::rename(&tx, 1, "tx-committed".into()).await.unwrap();
            tx.commit().await.unwrap();
        }
        assert_eq!(User::by_id(&client, 1).await.unwrap().name, "tx-committed");
    });
}

#[test]
fn tx_drop_rollback() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        {
            let tx = client.begin().await.unwrap();
            User::rename(&tx, 1, "drop-rollback".into()).await.unwrap();
        }
        let u = User::by_id(&client, 1).await.unwrap();
        assert_eq!(u.name, "alice");
    });
}

#[test]
fn tx_isolation_levels() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        for level in [
            IsolationLevel::ReadCommitted,
            IsolationLevel::RepeatableRead,
            IsolationLevel::Serializable,
        ] {
            let tx = client.tx_with().isolation(level).begin().await.unwrap();
            assert_eq!(User::by_id(&tx, 1).await.unwrap().id, 1);
            tx.commit().await.unwrap();
        }

        let tx = client
            .tx_with()
            .isolation(IsolationLevel::Serializable)
            .read_only()
            .deferrable()
            .begin()
            .await
            .unwrap();
        assert_eq!(User::count_all(&tx).await.unwrap(), 5);
        tx.commit().await.unwrap();
    });
}

#[test]
fn tx_statement_timeout_long() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        let tx = client
            .tx_with()
            .statement_timeout(Duration::from_secs(10))
            .begin()
            .await
            .unwrap();
        assert_eq!(User::count_all(&tx).await.unwrap(), 5);
        tx.commit().await.unwrap();
    });
}

#[test]
fn tx_savepoint_release_and_rollback() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        let tx = client.begin().await.unwrap();
        User::rename(&tx, 1, "lvl-0".into()).await.unwrap();

        let sp = tx.savepoint("a").await.unwrap();
        User::rename(&tx, 1, "lvl-1".into()).await.unwrap();
        sp.release().await.unwrap();
        assert_eq!(User::by_id(&tx, 1).await.unwrap().name, "lvl-1");

        let sp2 = tx.savepoint("b").await.unwrap();
        User::rename(&tx, 1, "lvl-2".into()).await.unwrap();
        assert_eq!(User::by_id(&tx, 1).await.unwrap().name, "lvl-2");
        sp2.rollback().await.unwrap();
        assert_eq!(User::by_id(&tx, 1).await.unwrap().name, "lvl-1");

        let sp_a = tx.savepoint("aa").await.unwrap();
        User::rename(&tx, 1, "lvl-3a".into()).await.unwrap();
        let sp_b = sp_a.savepoint("bb").await.unwrap();
        User::rename(&tx, 1, "lvl-3b".into()).await.unwrap();
        sp_b.rollback().await.unwrap();
        assert_eq!(User::by_id(&tx, 1).await.unwrap().name, "lvl-3a");
        sp_a.release().await.unwrap();

        tx.commit().await.unwrap();
        assert_eq!(User::by_id(&client, 1).await.unwrap().name, "lvl-3a");
    });
}

#[test]
fn tx_savepoint_drop_rollback() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        let tx = client.begin().await.unwrap();
        User::rename(&tx, 1, "outer".into()).await.unwrap();
        {
            let _sp = tx.savepoint("drop_me").await.unwrap();
            User::rename(&tx, 1, "inner".into()).await.unwrap();
        }
        assert_eq!(User::by_id(&tx, 1).await.unwrap().name, "outer");
        tx.commit().await.unwrap();
    });
}

#[test]
fn tx_cancel_token_api() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        let tx = client.begin().await.unwrap();
        let token = tx.cancel_token();
        assert!(token.is_some());
        if let Some(tok) = token {
            assert!(tok.pid() != 0);
        }
        tx.commit().await.unwrap();
    });
}

#[test]
fn copy_in_binary() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        client
            .execute_raw("DELETE FROM cartel_pg_copy")
            .await
            .unwrap();

        let mut buf = Vec::new();
        buf.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
        buf.extend_from_slice(&0i32.to_be_bytes());
        buf.extend_from_slice(&0i32.to_be_bytes());
        for (id, label) in [(1i64, "alpha"), (2, "beta"), (3, "gamma")] {
            buf.extend_from_slice(&2i16.to_be_bytes());
            buf.extend_from_slice(&8i32.to_be_bytes());
            buf.extend_from_slice(&id.to_be_bytes());
            buf.extend_from_slice(&(label.len() as i32).to_be_bytes());
            buf.extend_from_slice(label.as_bytes());
        }
        buf.extend_from_slice(&(-1i16).to_be_bytes());

        client
            .copy_in("COPY cartel_pg_copy (id, label) FROM STDIN BINARY", &buf)
            .await
            .unwrap();

        let n = CopyRow::count_all(&client).await.unwrap();
        assert_eq!(n, 3);
    });
}

#[test]
fn copy_out_binary() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        client
            .execute_raw("DELETE FROM cartel_pg_copy")
            .await
            .unwrap();
        client
            .execute_raw(
                "INSERT INTO cartel_pg_copy (id, label) VALUES (10, 'x'), (11, 'y'), (12, 'z')",
            )
            .await
            .unwrap();

        let mut stream = client.copy_out("COPY cartel_pg_copy TO STDOUT BINARY");
        let mut total: Vec<u8> = Vec::new();
        while let Some(chunk) = stream.next_chunk().await.unwrap() {
            total.extend_from_slice(&chunk);
        }
        assert!(total.starts_with(b"PGCOPY\n\xff\r\n\0"));
        assert!(total.len() > 19);
    });
}

#[test]
fn copy_in_stream_chunks() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        client
            .execute_raw("DELETE FROM cartel_pg_copy")
            .await
            .unwrap();
        let mut guard = client
            .copy_in_stream("COPY cartel_pg_copy (id, label) FROM STDIN BINARY")
            .unwrap();
        let mut header = Vec::new();
        header.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
        header.extend_from_slice(&0i32.to_be_bytes());
        header.extend_from_slice(&0i32.to_be_bytes());
        guard.write(&header).unwrap();
        for (id, label) in [(50i64, "p"), (51, "q")] {
            let mut row = Vec::new();
            row.extend_from_slice(&2i16.to_be_bytes());
            row.extend_from_slice(&8i32.to_be_bytes());
            row.extend_from_slice(&id.to_be_bytes());
            row.extend_from_slice(&(label.len() as i32).to_be_bytes());
            row.extend_from_slice(label.as_bytes());
            guard.write(&row).unwrap();
        }
        let mut trailer = Vec::new();
        trailer.extend_from_slice(&(-1i16).to_be_bytes());
        guard.write(&trailer).unwrap();
        guard.finish().await.unwrap();
    });
}

#[test]
fn listen_unlisten_api() {
    let mut rt = boot_with(1);
    let client = rt.client();

    let suffix = unique_suffix();
    let channel = format!("cartel_test_{suffix}");
    rt.block_on(async move {
        let guard = client.listen(channel.clone()).await.unwrap();
        assert_eq!(guard.channel(), channel);
        guard.unlisten().await.unwrap();
    });
}

#[test]
fn listen_notify_delivery() {
    let mut rt = boot_with(2);
    let client = rt.client();

    let suffix = unique_suffix();
    let channel = format!("cartel_notify_{suffix}");
    let payload = "hello world";
    rt.block_on(async move {
        let guard = client.listen(channel.clone()).await.unwrap();
        let notifier = client;
        let escaped = payload.replace('\'', "''");
        let sql = format!("NOTIFY \"{}\", '{}'", channel.replace('"', "\"\""), escaped);
        notifier.execute_raw(&sql).await.unwrap();
        let n = guard.next_notification().await.unwrap();
        assert_eq!(n.channel, channel);
        assert_eq!(n.payload, payload);
        assert!(n.pid > 0);
        guard.unlisten().await.unwrap();
    });
}

#[test]
fn type_roundtrips_full() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        client
            .execute_raw("DELETE FROM cartel_pg_types")
            .await
            .unwrap();
        client
            .execute_raw(
                "INSERT INTO cartel_pg_types
                (id, v_i16, v_i32, v_i64, v_f32, v_f64, v_bool, v_string, v_bytes,
                 v_uuid, v_ts, v_date, v_ltree,
                 o_i32, o_i64, o_bool, o_string, o_bytes, o_uuid,
                 arr_i32, arr_i64, arr_str)
             VALUES
                (1, 12345, 123456789, 9876543210, 1.5::real, 2.5::double precision, TRUE,
                 'rust √', E'\\\\xcafebabe',
                 '10101010-1010-1010-1010-101010101010'::uuid,
                 '2001-09-09 01:46:39'::timestamp,
                 DATE '2024-10-04',
                 'a.b.c'::ltree,
                 42, 43, TRUE, 'opt', E'\\\\xdead',
                 '20202020-2020-2020-2020-202020202020'::uuid,
                 ARRAY[1,2,3]::int4[], ARRAY[10,20,30]::int8[], ARRAY['x','y']::text[])",
            )
            .await
            .unwrap();
        client
            .execute_raw(
                "INSERT INTO cartel_pg_types
                (id, v_i16, v_i32, v_i64, v_f32, v_f64, v_bool, v_string, v_bytes,
                 v_uuid, v_ts, v_date, v_ltree,
                 o_i32, o_i64, o_bool, o_string, o_bytes, o_uuid,
                 arr_i32, arr_i64, arr_str)
             VALUES
                (2, 0, 0, 0, 0.0::real, 0.0::double precision, FALSE,
                 'n', E'\\\\x',
                 '00000000-0000-0000-0000-000000000000'::uuid,
                 '2001-09-09 01:46:39'::timestamp,
                 DATE '2024-10-04',
                 'z'::ltree,
                 NULL, NULL, NULL, NULL, NULL, NULL,
                 ARRAY[]::int4[], ARRAY[]::int8[], ARRAY[]::text[])",
            )
            .await
            .unwrap();

        let row = AllTypes::by_id(&client, 1).await.unwrap();
        assert_eq!(row.id, 1);
        assert_eq!(row.v_i16, 12345);
        assert_eq!(row.v_i32, 123456789);
        assert_eq!(row.v_i64, 9876543210);
        assert!((row.v_f32 - 1.5).abs() < 1e-6);
        assert!((row.v_f64 - 2.5).abs() < 1e-9);
        assert!(row.v_bool);
        assert_eq!(row.v_string, "rust √");
        assert_eq!(row.v_bytes, vec![0xca, 0xfe, 0xba, 0xbe]);
        assert_eq!(row.v_uuid, Uuid::from_bytes([0x10; 16]));
        assert_eq!(row.v_ltree.as_str(), "a.b.c");
        assert_eq!(row.o_i32, Some(42));
        assert_eq!(row.o_i64, Some(43));
        assert_eq!(row.o_bool, Some(true));
        assert_eq!(row.o_string.as_deref(), Some("opt"));
        assert_eq!(row.o_bytes, Some(vec![0xde, 0xad]));
        assert_eq!(row.o_uuid, Some(Uuid::from_bytes([0x20; 16])));

        let row2 = AllTypes::by_id(&client, 2).await.unwrap();
        assert!(row2.o_i32.is_none());
        assert!(row2.o_i64.is_none());
        assert!(row2.o_bool.is_none());
        assert!(row2.o_string.is_none());
        assert!(row2.o_bytes.is_none());
        assert!(row2.o_uuid.is_none());

        let arrs = AllTypes::arrays_by_id(&client, 1).await.unwrap();
        assert_eq!(arrs.0, vec![1, 2, 3]);
        assert_eq!(arrs.1, vec![10, 20, 30]);
        assert_eq!(arrs.2, vec!["x".to_string(), "y".to_string()]);
    });
}

#[test]
fn type_param_binding_each() {
    let mut rt = boot();
    let client = rt.client();
    rt.block_on(async move {
        client
            .execute_raw("DELETE FROM cartel_pg_types")
            .await
            .unwrap();
        seed_alltypes(&client).await.unwrap();
        let uuid = Uuid::from_bytes([0x31; 16]);
        let ts = Timestamp(100i64);
        let date = Date(1000i32);
        let ltree = Ltree::new("a.b.c");
        AllTypes::update_ints_by_id(&client, 42, 321i16, 654i32, 987i64, false)
            .await
            .unwrap();
        AllTypes::update_strs_by_id(
            &client,
            42,
            1.25f32,
            6.5f64,
            "updated".into(),
            vec![0xaa, 0xbb],
        )
        .await
        .unwrap();
        AllTypes::update_temporal_by_id(&client, 42, uuid, ts, date, ltree.clone())
            .await
            .unwrap();
        let row = AllTypes::by_id(&client, 42).await.unwrap();
        assert_eq!(row.v_i16, 321);
        assert_eq!(row.v_i32, 654);
        assert_eq!(row.v_i64, 987);
        assert!((row.v_f32 - 1.25).abs() < 1e-6);
        assert!((row.v_f64 - 6.5).abs() < 1e-9);
        assert!(!row.v_bool);
        assert_eq!(row.v_string, "updated");
        assert_eq!(row.v_bytes, vec![0xaa, 0xbb]);
        assert_eq!(row.v_uuid, uuid);
        assert_eq!(row.v_ts.0, 100);
        assert_eq!(row.v_date.0, 1000);
        assert_eq!(row.v_ltree, ltree);
    });
}

async fn seed_alltypes(client: &CartelClient<'_, Db>) -> Result<(), cartel_pg::Error> {
    client
        .execute_raw(
            "INSERT INTO cartel_pg_types
            (id, v_i16, v_i32, v_i64, v_f32, v_f64, v_bool, v_string, v_bytes,
             v_uuid, v_ts, v_date, v_ltree,
             arr_i32, arr_i64, arr_str)
         VALUES
            (42, 0, 0, 0, 0.0, 0.0, TRUE, 'init', E'\\\\x00',
             '11111111-1111-1111-1111-111111111111'::uuid,
             NOW(), CURRENT_DATE, 'root'::ltree,
             ARRAY[]::int4[], ARRAY[]::int8[], ARRAY[]::text[])",
        )
        .await
}

#[test]
fn permanent_failure_no_infinite_retry() {
    let env = EnvCfg::load();
    let addr = env.tcp_addr();
    let schema = format!("cartel_pg_missing_{}", unique_suffix());
    let mut rt =
        Rt::<Db>::new(addr, env.cartel_config(), 2, schema, false).expect("create runtime");
    let client = rt.client();
    let failed = rt.block_on(async move {
        for _ in 0..200_000 {
            if client.is_failed() {
                return true;
            }
            let mut yielded = false;
            std::future::poll_fn(|cx| {
                if yielded {
                    Poll::Ready(())
                } else {
                    yielded = true;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            })
            .await;
        }
        false
    });
    assert!(failed, "pool with missing schema must reach is_failed()");
}
