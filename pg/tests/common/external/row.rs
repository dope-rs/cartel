use crate::common::external::user::User;
use cartel_gen::query_group;
use cartel_pg::{
    Date, PgRow, PgTable, Timestamp, Uuid, dsl::expr::Array as _, dsl::expr::Ltree as _,
    dsl::query::ops::Arrange as _, dsl::query::ops::Change as _, dsl::query::ops::Fetch as _,
    dsl::query::ops::Project as _, dsl::query::ops::Reduce as _,
};

#[derive(PgTable, PgRow, PartialEq)]
#[table_name("cartel_pg_posts")]
pub struct Post {
    pub id: i64,
    pub author_id: i64,
    pub title: String,
    pub body: String,
    pub likes: i64,
}

#[derive(PgTable, PgRow, PartialEq)]
#[table_name("cartel_pg_comments")]
pub struct Comment {
    pub id: i64,
    pub post_id: i64,
    pub author_id: i64,
    pub text: String,
}

#[derive(PgTable, PgRow, PartialEq)]
#[table_name("cartel_pg_tags")]
pub struct Tag {
    pub id: i64,
    pub post_id: i64,
    pub label: String,
}

#[derive(PgTable, PgRow, PartialEq)]
#[row(view)]
#[table_name("cartel_pg_widgets")]
pub struct Widget {
    pub id: i64,
    pub bucket: i32,
    pub value: i64,
}

#[derive(PgTable, PgRow)]
#[table_name("q1_0")]
pub struct Correlated {
    pub id: i64,
    pub value: i64,
}

#[derive(PgTable, PgRow, PartialEq)]
#[table_name("cartel_pg_items")]
pub struct Item {
    pub a: i32,
    pub b: i32,
    pub payload: String,
}

pub enum AllTypes {}

#[derive(PgTable, PgRow, PartialEq)]
#[table_name("cartel_pg_types")]
pub struct Numbers {
    pub id: i64,
    pub v_i16: i16,
    pub v_i32: i32,
    pub v_i64: i64,
    pub v_f32: f32,
    pub v_f64: f64,
    pub v_bool: bool,
    pub v_string: String,
}

#[derive(PgTable, PgRow, PartialEq)]
#[table_name("cartel_pg_types")]
pub struct Payload {
    pub id: i64,
    pub v_bytes: Vec<u8>,
    pub v_uuid: Uuid,
    pub v_ts: Timestamp,
    pub v_date: Date,
    pub v_ltree: cartel_pg::Ltree,
}

#[derive(PgTable, PgRow, PartialEq)]
#[table_name("cartel_pg_types")]
pub struct Optional {
    pub id: i64,
    pub o_i32: Option<i32>,
    pub o_i64: Option<i64>,
    pub o_bool: Option<bool>,
    pub o_string: Option<String>,
    pub o_bytes: Option<Vec<u8>>,
    pub o_uuid: Option<Uuid>,
}

#[derive(PgTable, PgRow, PartialEq)]
#[table_name("cartel_pg_types")]
pub struct Arrays {
    pub id: i64,
    pub arr_i32: Vec<i32>,
    pub arr_i64: Vec<i64>,
    pub arr_str: Vec<String>,
}

#[derive(PgTable)]
#[table_name("cartel_pg_docs")]
pub struct Doc {
    pub id: i64,
    pub body: String,
}

#[derive(PgTable)]
#[table_name("cartel_pg_nodes")]
pub struct LtreeNode {
    pub id: i64,
    pub path: cartel_pg::Ltree,
}

#[derive(PgTable, PgRow, PartialEq)]
#[row(view)]
#[table(batch)]
#[table_name("cartel_pg_widgets")]
pub struct WidgetSlim {
    pub id: i64,
    pub bucket: i32,
    pub value: i64,
}

#[derive(PgTable, PgRow, PartialEq)]
#[row(view)]
#[table_name("cartel_pg_scores")]
pub struct Score {
    pub id: i32,
    pub randomnumber: i32,
}

#[derive(PgTable)]
#[table_name("cartel_pg_copy")]
pub struct Copy {
    pub id: i64,
}

#[query_group]
impl Post {
    pub fn titles(uid: i64) -> Vec<String> {
        User::join::<Post>(|u, p| u.id == p.author_id)
            .filter(|u, _p| u.id == uid)
            .select(|_u, p| p.title)
            .all()
    }
}

#[query_group]
impl Widget {
    pub fn all_rows() -> Vec<Widget> {
        Widget::filter(|_w| true).order_by(|w| w.id).all()
    }
    pub fn bucket_n() -> Vec<(i32, i64)> {
        Widget::filter(|_w| true)
            .group_by(|w| w.bucket)
            .order_by(|w| w.bucket)
            .count()
    }
    pub fn bucket_max() -> Vec<(i32, Option<i64>)> {
        Widget::filter(|_w| true)
            .group_by(|w| w.bucket)
            .order_by(|w| w.bucket)
            .max(|w| w.value)
    }
    pub fn having(min: i64) -> Vec<(i32, i64)> {
        Widget::filter(|_w| true)
            .group_by(|w| w.bucket)
            .having(|_w, agg| agg.count() >= min)
            .order_by(|w| w.bucket)
            .count()
    }
    pub fn running_n() -> Vec<(i64, i64)> {
        Widget::filter(|_w| true)
            .order_by(|w| w.id)
            .select(|w| (w.id, Dialect::count(w.id).over(|win| win.order_by(w.id))))
            .all()
    }
}

#[query_group]
impl WidgetSlim {
    pub fn insert_bulk(ids: &[i64], buckets: &[i32], values: &[i64]) {
        WidgetSlim::insert_each(|w| {
            w.id = ids;
            w.bucket = buckets;
            w.value = values;
        })
    }
    pub fn count_500() -> i64 {
        WidgetSlim::filter(|w| w.id >= 500).count()
    }
    pub fn rows_500() -> Vec<WidgetSlim> {
        WidgetSlim::filter(|w| w.id >= 500).order_by(|w| w.id).all()
    }
}

#[query_group]
impl Score {
    pub fn by_id(id: i32) -> Score {
        Score::filter(|s| s.id == id).one()
    }
    pub fn bulk_update(ids: &[i32], numbers: &[i32]) {
        Score::filter_each((&ids, &numbers), |s, id, n| s.id == id)
            .update(|s, id, n| s.randomnumber = n)
    }
}

#[query_group]
impl Copy {
    pub fn count_all() -> i64 {
        Copy::filter(|_c| true).count()
    }
}

#[query_group]
impl Item {
    pub fn by_key(a: i32, b: i32) -> Option<Item> {
        Item::filter(|i| i.a == a && i.b == b).first()
    }
    pub fn add(a: i32, b: i32, payload: String) {
        Item::insert(|i| {
            i.a = a;
            i.b = b;
            i.payload = payload;
        })
    }
}

pub enum Reports {}

#[query_group]
impl Reports {
    pub fn self_later() -> Vec<i64> {
        Widget::filter(|outer| Dialect::exists(Widget::filter(|inner| inner.id > outer.id)))
            .order_by(|outer| outer.id)
            .select(|outer| outer.id)
            .all()
    }

    pub fn self_counts() -> Vec<(i64, i64)> {
        Widget::filter(|_outer| true)
            .order_by(|outer| outer.id)
            .select(|outer| {
                (
                    outer.id,
                    Widget::filter(|inner| inner.id < outer.id).count(),
                )
            })
            .all()
    }

    pub fn correlated(id: i64) -> Correlated {
        Correlated::insert(|row| {
            row.id = id;
            row.value = 0;
        })
        .on_conflict(|row| row.id)
        .do_update(|row| row.value = Correlated::filter(|inner| inner.id < row.id).count())
        .returning_one()
    }

    pub fn inner_posts() -> Vec<(User, Post)> {
        User::join::<Post>(|u, p| u.id == p.author_id)
            .filter(|_u, _p| true)
            .order_by(|_u, p| p.id)
            .all()
    }
    pub fn left_n() -> i64 {
        User::left_join::<Post>(|u, p| u.id == p.author_id)
            .filter(|_u, _p| true)
            .count()
    }
    pub fn comments() -> Vec<(User, Post, Comment)> {
        User::join::<Post>(|u, p| u.id == p.author_id)
            .join::<Comment>(|_u, p, c| p.id == c.post_id)
            .filter(|_u, _p, _c| true)
            .order_by(|_u, _p, c| c.id)
            .all()
    }
    pub fn comment_tags() -> Vec<(User, Post, Comment, Tag)> {
        User::join::<Post>(|u, p| u.id == p.author_id)
            .join::<Comment>(|_u, p, c| p.id == c.post_id)
            .join::<Tag>(|_u, p, _c, t| p.id == t.post_id)
            .filter(|_u, _p, _c, _t| true)
            .order_by(|_u, _p, c, _t| c.id)
            .all()
    }
    pub fn post_n() -> i64 {
        User::join::<Post>(|u, p| u.id == p.author_id)
            .filter(|_u, _p| true)
            .count()
    }
    pub fn with_posts() -> Vec<User> {
        User::filter(|u| Dialect::exists(Post::filter(|p| p.author_id == u.id)))
            .order_by(|u| u.id)
            .all()
    }
    pub fn without_post() -> Vec<User> {
        User::filter(|u| Dialect::not_exists(Post::filter(|p| p.author_id == u.id)))
            .order_by(|u| u.id)
            .all()
    }
    pub fn post_counts() -> Vec<(i64, i64)> {
        User::filter(|_u| true)
            .order_by(|u| u.id)
            .select(|u| (u.id, Post::filter(|p| p.author_id == u.id).count()))
            .all()
    }
    pub fn top_posts() -> Vec<(User, Post)> {
        User::lateral_join::<Post>(|u| {
            Post::filter(|p| p.author_id == u.id)
                .order_by_desc(|p| p.likes)
                .limit(1)
        })
        .filter(|_u, _p| true)
        .order_by(|u, _p| u.id)
        .all()
    }
    pub fn left_top_n() -> i64 {
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
    pub fn fts_match(needle: String) -> Vec<i64> {
        Doc::filter(|d| Dialect::to_tsvector(d.body).fts_match(Dialect::plainto_tsquery(needle)))
            .order_by(|d| d.id)
            .select(|d| d.id)
            .all()
    }
    pub fn fts_raw(query: String) -> Vec<i64> {
        Doc::filter(|d| Dialect::to_tsvector(d.body).fts_match(Dialect::to_tsquery(query)))
            .order_by(|d| d.id)
            .select(|d| d.id)
            .all()
    }
    pub fn ts_rank_q(needle: String, needle2: String) -> Vec<(i64, f32)> {
        Doc::filter(|d| Dialect::to_tsvector(d.body).fts_match(Dialect::plainto_tsquery(needle)))
            .order_by(|d| d.id)
            .select(|d| {
                (
                    d.id,
                    Dialect::ts_rank(
                        Dialect::to_tsvector(d.body),
                        Dialect::plainto_tsquery(needle2),
                    ),
                )
            })
            .all()
    }
}

#[query_group]
impl AllTypes {
    pub fn ints_contain(rhs: &[i32]) -> Vec<i64> {
        Arrays::filter(|r| r.arr_i32.pg_contains(rhs))
            .order_by(|r| r.id)
            .select(|r| r.id)
            .all()
    }
    pub fn ints_overlap(rhs: &[i32]) -> Vec<i64> {
        Arrays::filter(|r| r.arr_i32.pg_overlaps(rhs))
            .order_by(|r| r.id)
            .select(|r| r.id)
            .all()
    }
    pub fn strs_contain(rhs: &[&str]) -> Vec<i64> {
        Arrays::filter(|r| r.arr_str.pg_contains(rhs))
            .order_by(|r| r.id)
            .select(|r| r.id)
            .all()
    }
    pub fn strs_overlap(rhs: &[&str]) -> Vec<i64> {
        Arrays::filter(|r| r.arr_str.pg_overlaps(rhs))
            .order_by(|r| r.id)
            .select(|r| r.id)
            .all()
    }
    pub fn ints_len() -> Vec<(i64, i32)> {
        Arrays::filter(|_r| true)
            .order_by(|r| r.id)
            .select(|r| (r.id, Dialect::array_length(r.arr_i32, 1)))
            .all()
    }
    pub fn strs_len() -> Vec<(i64, i32)> {
        Arrays::filter(|_r| true)
            .order_by(|r| r.id)
            .select(|r| (r.id, Dialect::cardinality(r.arr_str)))
            .all()
    }
    pub fn by_id(id: i64) -> (Numbers, Payload, Optional, Arrays) {
        Numbers::join::<Payload>(|n, p| n.id == p.id)
            .join::<Optional>(|n, _p, o| n.id == o.id)
            .join::<Arrays>(|n, _p, _o, a| n.id == a.id)
            .filter(|n, _p, _o, _a| n.id == id)
            .one()
    }
    pub fn arrays_by_id(id: i64) -> (Vec<i32>, Vec<i64>, Vec<String>) {
        Arrays::filter(|r| r.id == id)
            .select(|r| (r.arr_i32, r.arr_i64, r.arr_str))
            .one()
    }
    pub fn set_ints(id: i64, v_i16: i16, v_i32: i32, v_i64: i64, v_bool: bool) {
        Numbers::filter(|r| r.id == id).update(|r| {
            r.v_i16 = v_i16;
            r.v_i32 = v_i32;
            r.v_i64 = v_i64;
            r.v_bool = v_bool;
        })
    }
    pub fn set_scalars(id: i64, v_f32: f32, v_f64: f64, v_string: &str) {
        Numbers::filter(|r| r.id == id).update(|r| {
            r.v_f32 = v_f32;
            r.v_f64 = v_f64;
            r.v_string = v_string;
        })
    }

    pub fn set_bytes(id: i64, v_bytes: Vec<u8>) {
        Payload::filter(|r| r.id == id).update(|r| r.v_bytes = v_bytes)
    }
    pub fn set_temporal(
        id: i64,
        v_uuid: Uuid,
        v_ts: Timestamp,
        v_date: Date,
        v_ltree: cartel_pg::Ltree,
    ) {
        Payload::filter(|r| r.id == id).update(|r| {
            r.v_uuid = v_uuid;
            r.v_ts = v_ts;
            r.v_date = v_date;
            r.v_ltree = v_ltree;
        })
    }
}

#[query_group]
impl LtreeNode {
    pub fn descendants(of: cartel_pg::Ltree) -> Vec<i64> {
        LtreeNode::filter(|n| n.path.descendant_of(of))
            .order_by(|n| n.id)
            .select(|n| n.id)
            .all()
    }
    pub fn ancestors(of: cartel_pg::Ltree) -> Vec<i64> {
        LtreeNode::filter(|n| n.path.ancestor_of(of))
            .order_by(|n| n.id)
            .select(|n| n.id)
            .all()
    }
}
