#![allow(clippy::bool_comparison, clippy::doc_overindented_list_items)]

use cartel_sqlite::dsl::Text;
use cartel_sqlite::{Connection, Error, SqliteTable, exists, not_exists, row_number, sqlite_query};

#[derive(SqliteTable, Debug, PartialEq)]
#[table_name("users")]
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

#[derive(SqliteTable, Debug, PartialEq)]
#[table_name("posts")]
struct Post {
    #[pk]
    id: i64,
    author_id: i64,
    title: String,
    body: String,
    likes: i64,
}

#[derive(SqliteTable, Debug, PartialEq)]
#[table_name("comments")]
struct Comment {
    #[pk]
    id: i64,
    post_id: i64,
    author_id: i64,
    text: String,
}

#[derive(SqliteTable, Debug, PartialEq)]
#[table_name("tags")]
struct Tag {
    #[pk]
    id: i64,
    post_id: i64,
    label: String,
}

#[derive(SqliteTable, Debug, PartialEq)]
struct Widget {
    #[pk]
    id: i64,
    bucket: i32,
    value: i64,
}

#[derive(SqliteTable, Debug, PartialEq)]
struct Item {
    #[pk]
    a: i32,
    #[pk]
    b: i32,
    payload: String,
}

struct Fixture {
    conn: Connection,
}

impl Fixture {
    fn new() -> Self {
        let conn = Connection::open_in_memory().expect("open in-memory sqlite");
        conn.execute_batch(
            "CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                age INTEGER NOT NULL,
                score REAL NOT NULL,
                active INTEGER NOT NULL,
                nickname TEXT,
                avatar BLOB
             );
             CREATE TABLE posts (
                id INTEGER PRIMARY KEY,
                author_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                body TEXT NOT NULL,
                likes INTEGER NOT NULL DEFAULT 0
             );
             CREATE TABLE comments (
                id INTEGER PRIMARY KEY,
                post_id INTEGER NOT NULL,
                author_id INTEGER NOT NULL,
                text TEXT NOT NULL
             );
             CREATE TABLE tags (
                id INTEGER PRIMARY KEY,
                post_id INTEGER NOT NULL,
                label TEXT NOT NULL
             );
             CREATE TABLE widgets (
                id INTEGER PRIMARY KEY,
                bucket INTEGER NOT NULL,
                value INTEGER NOT NULL
             );
             CREATE TABLE items (
                a INTEGER NOT NULL,
                b INTEGER NOT NULL,
                payload TEXT NOT NULL,
                PRIMARY KEY (a, b)
             );",
        )
        .expect("create schema");
        conn.execute_batch(
            "INSERT INTO users (id, name, age, score, active, nickname, avatar) VALUES
                (1, 'alice',   30, 9.5, 1, 'al',    NULL),
                (2, 'bob',     25, 7.0, 1, NULL,    NULL),
                (3, 'carol',   40, 8.2, 0, 'caz',   x'cafe'),
                (4, 'dave',    22, 6.1, 1, NULL,    x'deadbeef'),
                (5, '한국어 √',  29, 5.0, 1, '한',    NULL);
             INSERT INTO posts (id, author_id, title, body, likes) VALUES
                (10, 1, 'hello',  'world',   3),
                (11, 1, 'second', 'post',    1),
                (12, 2, 'bob-1',  'b-body',  7),
                (13, 3, 'carol',  'c-body',  0);
             INSERT INTO comments (id, post_id, author_id, text) VALUES
                (100, 10, 2, 'nice'),
                (101, 10, 3, 'cool'),
                (102, 12, 1, 'lol');
             INSERT INTO tags (id, post_id, label) VALUES
                (1000, 10, 'rust'),
                (1001, 10, 'sql'),
                (1002, 12, 'rust');
             INSERT INTO widgets (id, bucket, value) VALUES
                (1, 1, 10), (2, 1, 20), (3, 2, 30), (4, 2, 40), (5, 3, 50);",
        )
        .expect("seed data");
        Self { conn }
    }
}

#[sqlite_query]
fn user_by_id(id: i64) -> User {
    User::filter(|u| u.id == id).one()
}

#[sqlite_query]
fn maybe_user_by_id(id: i64) -> Option<User> {
    User::filter(|u| u.id == id).first()
}

#[sqlite_query]
fn all_users() -> Vec<User> {
    User::filter(|_u| true).all()
}

#[sqlite_query]
fn user_names_min_id(min: i64) -> Vec<String> {
    User::filter(|u| u.id >= min)
        .order_by(|u| u.id)
        .select(|u| u.name)
        .all()
}

#[sqlite_query]
fn user_id_name_pairs() -> Vec<(i64, String)> {
    User::filter(|_u| true)
        .order_by(|u| u.id)
        .select(|u| (u.id, u.name))
        .all()
}

#[sqlite_query]
fn user_id_plus_one(id: i64) -> i64 {
    User::filter(|u| u.id == id).select(|u| u.id + 1).one()
}

#[test]
fn dispatch_kinds() {
    let f = Fixture::new();
    let u = user_by_id(&f.conn, 1).unwrap();
    assert_eq!(u.id, 1);
    assert_eq!(u.name, "alice");
    assert_eq!(u.age, 30);
    assert_eq!(u.score, 9.5);
    assert!(u.active);
    assert_eq!(u.nickname.as_deref(), Some("al"));
    assert!(u.avatar.is_none());

    let err = user_by_id(&f.conn, 999).unwrap_err();
    assert!(matches!(err, Error::QueryReturnedNoRows));

    assert!(maybe_user_by_id(&f.conn, 999).unwrap().is_none());
    assert!(maybe_user_by_id(&f.conn, 2).unwrap().is_some());

    let all = all_users(&f.conn).unwrap();
    assert_eq!(all.len(), 5);

    let names = user_names_min_id(&f.conn, 3).unwrap();
    assert_eq!(names, vec!["carol", "dave", "한국어 √"]);

    let pairs = user_id_name_pairs(&f.conn).unwrap();
    assert_eq!(pairs.len(), 5);
    assert_eq!(pairs[0], (1, "alice".to_string()));

    assert_eq!(user_id_plus_one(&f.conn, 3).unwrap(), 4);
}

#[sqlite_query]
fn user_blob(id: i64) -> Vec<Option<Vec<u8>>> {
    User::filter(|u| u.id == id).select(|u| u.avatar).all()
}

#[sqlite_query]
fn unicode_user_name() -> String {
    User::filter(|u| u.id == 5).select(|u| u.name).one()
}

#[test]
fn type_roundtrips() {
    let f = Fixture::new();
    let row = user_blob(&f.conn, 3).unwrap();
    assert_eq!(row, vec![Some(vec![0xcau8, 0xfe])]);
    let row = user_blob(&f.conn, 4).unwrap();
    assert_eq!(row, vec![Some(vec![0xdeu8, 0xad, 0xbe, 0xef])]);
    let row = user_blob(&f.conn, 1).unwrap();
    assert_eq!(row, vec![None]);
    assert_eq!(unicode_user_name(&f.conn).unwrap(), "한국어 √");
}

#[sqlite_query]
fn rename_user(id: i64, name: String) {
    User::filter(|u| u.id == id).update(|u| u.name = name)
}

#[sqlite_query]
fn set_user_fields(id: i64, name: String, age: i32, score: f64) {
    User::filter(|u| u.id == id).update(|u| {
        u.name = name;
        u.age = age;
        u.score = score;
    })
}

#[sqlite_query]
fn delete_user(id: i64) {
    User::filter(|u| u.id == id).delete()
}

#[test]
fn update_and_delete() {
    let f = Fixture::new();
    rename_user(&f.conn, 2, "robert".into()).unwrap();
    assert_eq!(user_by_id(&f.conn, 2).unwrap().name, "robert");

    set_user_fields(&f.conn, 2, "bobby".into(), 26, 7.5).unwrap();
    let u = user_by_id(&f.conn, 2).unwrap();
    assert_eq!(u.name, "bobby");
    assert_eq!(u.age, 26);
    assert_eq!(u.score, 7.5);

    delete_user(&f.conn, 4).unwrap();
    assert!(maybe_user_by_id(&f.conn, 4).unwrap().is_none());
}

#[sqlite_query]
fn insert_user(id: i64, name: String, age: i32, score: f64, active: bool) {
    User::insert(|u| {
        u.id = id;
        u.name = name;
        u.age = age;
        u.score = score;
        u.active = active;
    })
}

#[sqlite_query]
fn insert_user_returning_all(
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

#[sqlite_query]
fn insert_user_returning_one(id: i64, name: String, age: i32, score: f64, active: bool) -> User {
    User::insert(|u| {
        u.id = id;
        u.name = name;
        u.age = age;
        u.score = score;
        u.active = active;
    })
    .returning_one()
}

#[sqlite_query]
fn insert_user_returning_first(
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

#[sqlite_query]
fn copy_users_min_age(min_age: i32, offset: i64) {
    User::insert_from(User::filter(|u| u.age >= min_age), |t, src| {
        t.id = src.id + offset;
        t.name = src.name;
        t.age = src.age;
        t.score = src.score;
        t.active = src.active;
    })
}

#[test]
fn insert_variants() {
    let f = Fixture::new();
    insert_user(&f.conn, 100, "ned".into(), 50, 9.0, true).unwrap();
    let u = user_by_id(&f.conn, 100).unwrap();
    assert_eq!(u.name, "ned");

    let rows = insert_user_returning_all(&f.conn, 101, "olive".into(), 0, 0.0, true).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, 101);
    assert_eq!(rows[0].name, "olive");

    let row = insert_user_returning_one(&f.conn, 102, "pete".into(), 0, 0.0, true).unwrap();
    assert_eq!(row.id, 102);

    let row = insert_user_returning_first(&f.conn, 103, "quinn".into(), 0, 0.0, true).unwrap();
    assert_eq!(row.unwrap().id, 103);

    copy_users_min_age(&f.conn, 30, 1000).unwrap();
    let names: Vec<String> = (1001..=1005)
        .filter_map(|id| maybe_user_by_id(&f.conn, id).unwrap().map(|u| u.name))
        .collect();
    assert!(names.contains(&"alice".to_string()));
    assert!(names.contains(&"carol".to_string()));
}

#[sqlite_query]
fn insert_user_on_conflict_do_nothing(id: i64, name: String, age: i32, score: f64, active: bool) {
    User::insert(|u| {
        u.id = id;
        u.name = name;
        u.age = age;
        u.score = score;
        u.active = active;
    })
    .on_conflict_do_nothing()
}

#[sqlite_query]
fn upsert_user_name(id: i64, name: String, upd_name: String, age: i32, score: f64, active: bool) {
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

#[sqlite_query]
fn insert_user_target_do_nothing(id: i64, name: String, age: i32, score: f64, active: bool) {
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

#[test]
fn insert_on_conflict() {
    let f = Fixture::new();
    insert_user_on_conflict_do_nothing(&f.conn, 1, "ignored".into(), 0, 0.0, true).unwrap();
    assert_eq!(user_by_id(&f.conn, 1).unwrap().name, "alice");

    upsert_user_name(&f.conn, 1, "ignored".into(), "ALICE".into(), 0, 0.0, true).unwrap();
    assert_eq!(user_by_id(&f.conn, 1).unwrap().name, "ALICE");

    upsert_user_name(&f.conn, 200, "new".into(), "new-upd".into(), 0, 0.0, true).unwrap();
    assert_eq!(user_by_id(&f.conn, 200).unwrap().name, "new");

    insert_user_target_do_nothing(&f.conn, 1, "still-ignored".into(), 0, 0.0, true).unwrap();
    assert_eq!(user_by_id(&f.conn, 1).unwrap().name, "ALICE");
}

#[sqlite_query]
fn update_returning(id: i64, name: String) -> User {
    User::filter(|u| u.id == id)
        .update(|u| u.name = name)
        .returning_one()
}

#[sqlite_query]
fn delete_returning(id: i64) -> Option<User> {
    User::filter(|u| u.id == id).delete().returning_first()
}

#[sqlite_query]
fn delete_all_inactive_returning() -> Vec<User> {
    User::filter(|u| u.active == false).delete().returning_all()
}

#[test]
fn returning_clauses() {
    let f = Fixture::new();
    let u = update_returning(&f.conn, 1, "AAA".into()).unwrap();
    assert_eq!(u.name, "AAA");

    let u = delete_returning(&f.conn, 2).unwrap().unwrap();
    assert_eq!(u.name, "bob");

    let removed = delete_all_inactive_returning(&f.conn).unwrap();
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0].id, 3);
}

#[sqlite_query]
fn users_eq_age(age: i32) -> Vec<User> {
    User::filter(|u| u.age == age).order_by(|u| u.id).all()
}
#[sqlite_query]
fn users_ne_age(age: i32) -> Vec<User> {
    User::filter(|u| u.age != age).order_by(|u| u.id).all()
}
#[sqlite_query]
fn users_gt_age(age: i32) -> Vec<User> {
    User::filter(|u| u.age > age).order_by(|u| u.id).all()
}
#[sqlite_query]
fn users_lt_age(age: i32) -> Vec<User> {
    User::filter(|u| u.age < age).order_by(|u| u.id).all()
}
#[sqlite_query]
fn users_ge_age(age: i32) -> Vec<User> {
    User::filter(|u| u.age >= age).order_by(|u| u.id).all()
}
#[sqlite_query]
fn users_le_age(age: i32) -> Vec<User> {
    User::filter(|u| u.age <= age).order_by(|u| u.id).all()
}
#[sqlite_query]
fn users_like(pat: String) -> Vec<User> {
    User::filter(|u| u.name.like(pat)).order_by(|u| u.id).all()
}
#[sqlite_query]
fn users_not_like(pat: String) -> Vec<User> {
    User::filter(|u| u.name.not_like(pat))
        .order_by(|u| u.id)
        .all()
}
#[sqlite_query]
fn users_glob(pat: String) -> Vec<User> {
    User::filter(|u| u.name.glob(pat)).order_by(|u| u.id).all()
}
#[sqlite_query]
fn users_nickname_some() -> Vec<User> {
    User::filter(|u| u.nickname.is_some())
        .order_by(|u| u.id)
        .all()
}
#[sqlite_query]
fn users_nickname_none() -> Vec<User> {
    User::filter(|u| u.nickname.is_none())
        .order_by(|u| u.id)
        .all()
}
#[sqlite_query]
fn users_and(min_age: i32, active: bool) -> Vec<User> {
    User::filter(|u| u.age >= min_age && u.active == active)
        .order_by(|u| u.id)
        .all()
}
#[sqlite_query]
fn users_or(low: i32, high: i32) -> Vec<User> {
    User::filter(|u| u.age <= low || u.age >= high)
        .order_by(|u| u.id)
        .all()
}
#[sqlite_query]
fn users_nested(min_age: i32, max_age: i32, active: bool) -> Vec<User> {
    User::filter(|u| (u.age >= min_age && u.age <= max_age) || u.active == active)
        .order_by(|u| u.id)
        .all()
}
#[sqlite_query]
fn users_not_via_ne(active: bool) -> Vec<User> {
    User::filter(|u| u.active != active)
        .order_by(|u| u.id)
        .all()
}

#[test]
fn predicates() {
    let f = Fixture::new();
    assert_eq!(users_eq_age(&f.conn, 25).unwrap().len(), 1);
    assert_eq!(users_ne_age(&f.conn, 25).unwrap().len(), 4);
    assert_eq!(users_gt_age(&f.conn, 30).unwrap().len(), 1);
    assert_eq!(users_lt_age(&f.conn, 30).unwrap().len(), 3);
    assert_eq!(users_ge_age(&f.conn, 30).unwrap().len(), 2);
    assert_eq!(users_le_age(&f.conn, 30).unwrap().len(), 4);
    assert_eq!(users_like(&f.conn, "a%".into()).unwrap().len(), 1);
    assert_eq!(users_not_like(&f.conn, "a%".into()).unwrap().len(), 4);
    assert_eq!(users_glob(&f.conn, "[ab]*".into()).unwrap().len(), 2);
    assert_eq!(users_nickname_some(&f.conn).unwrap().len(), 3);
    assert_eq!(users_nickname_none(&f.conn).unwrap().len(), 2);
    assert_eq!(users_and(&f.conn, 25, true).unwrap().len(), 3);
    assert_eq!(users_or(&f.conn, 22, 40).unwrap().len(), 2);
    assert_eq!(users_nested(&f.conn, 25, 35, true).unwrap().len(), 4);
    assert_eq!(users_not_via_ne(&f.conn, true).unwrap().len(), 1);
}

#[sqlite_query]
fn users_ordered_desc() -> Vec<i64> {
    User::filter(|_u| true)
        .order_by_desc(|u| u.age)
        .select(|u| u.id)
        .all()
}
#[sqlite_query]
fn users_limited() -> Vec<i64> {
    User::filter(|_u| true)
        .order_by(|u| u.id)
        .limit(2)
        .select(|u| u.id)
        .all()
}
#[sqlite_query]
fn users_offset() -> Vec<i64> {
    User::filter(|_u| true)
        .order_by(|u| u.id)
        .limit(2)
        .offset(2)
        .select(|u| u.id)
        .all()
}
#[sqlite_query]
fn distinct_ages() -> Vec<i32> {
    User::filter(|_u| true)
        .distinct()
        .order_by(|u| u.age)
        .select(|u| u.age)
        .all()
}
#[sqlite_query]
fn users_limit_param(n: i64) -> Vec<User> {
    User::filter(|_u| true).order_by(|u| u.id).limit(n).all()
}

#[test]
fn ordering_and_limits() {
    let f = Fixture::new();
    let ids = users_ordered_desc(&f.conn).unwrap();
    assert_eq!(ids, vec![3, 1, 5, 2, 4]);
    assert_eq!(users_limited(&f.conn).unwrap(), vec![1, 2]);
    assert_eq!(users_offset(&f.conn).unwrap(), vec![3, 4]);
    let ages = distinct_ages(&f.conn).unwrap();
    let mut expected = vec![22, 25, 29, 30, 40];
    expected.sort();
    let mut got = ages;
    got.sort();
    assert_eq!(got, expected);
    assert_eq!(users_limit_param(&f.conn, 3).unwrap().len(), 3);
}

#[sqlite_query]
fn count_all() -> i64 {
    User::filter(|_u| true).count()
}
#[sqlite_query]
fn count_active(active: bool) -> i64 {
    User::filter(|u| u.active == active).count()
}
#[sqlite_query]
fn sum_ages() -> Option<i64> {
    User::filter(|_u| true).sum(|u| u.age)
}
#[sqlite_query]
fn avg_age() -> Option<f64> {
    User::filter(|_u| true).avg(|u| u.age)
}
#[sqlite_query]
fn min_age() -> Option<i32> {
    User::filter(|_u| true).min(|u| u.age)
}
#[sqlite_query]
fn max_score() -> Option<f64> {
    User::filter(|_u| true).max(|u| u.score)
}
#[sqlite_query]
fn widgets_count_per_bucket() -> Vec<(i32, i64)> {
    Widget::filter(|_w| true)
        .group_by(|w| w.bucket)
        .order_by(|w| w.bucket)
        .count()
}
#[sqlite_query]
fn widgets_sum_per_bucket() -> Vec<(i32, Option<i64>)> {
    Widget::filter(|_w| true)
        .group_by(|w| w.bucket)
        .order_by(|w| w.bucket)
        .sum(|w| w.value)
}
#[sqlite_query]
fn widgets_having(min: i64) -> Vec<(i32, i64)> {
    Widget::filter(|_w| true)
        .group_by(|w| w.bucket)
        .having(|_w, agg| agg.count() >= min)
        .order_by(|w| w.bucket)
        .count()
}

#[test]
fn aggregates() {
    let f = Fixture::new();
    assert_eq!(count_all(&f.conn).unwrap(), 5);
    assert_eq!(count_active(&f.conn, true).unwrap(), 4);
    assert_eq!(sum_ages(&f.conn).unwrap(), Some(30 + 25 + 40 + 22 + 29));
    let avg = avg_age(&f.conn).unwrap().unwrap();
    assert!((avg - (30.0 + 25.0 + 40.0 + 22.0 + 29.0) / 5.0).abs() < 1e-9);
    assert_eq!(min_age(&f.conn).unwrap(), Some(22));
    let m = max_score(&f.conn).unwrap().unwrap();
    assert!((m - 9.5).abs() < 1e-9);
    assert_eq!(
        widgets_count_per_bucket(&f.conn).unwrap(),
        vec![(1, 2), (2, 2), (3, 1)]
    );
    let sums = widgets_sum_per_bucket(&f.conn).unwrap();
    assert_eq!(sums, vec![(1, Some(30)), (2, Some(70)), (3, Some(50))]);
    assert_eq!(widgets_having(&f.conn, 2).unwrap(), vec![(1, 2), (2, 2)]);
}

#[sqlite_query]
fn user_with_posts() -> Vec<(User, Post)> {
    User::join::<Post>(|u, p| u.id == p.author_id)
        .filter(|_u, _p| true)
        .order_by(|_u, p| p.id)
        .all()
}
#[sqlite_query]
fn user_left_join_posts() -> i64 {
    User::left_join::<Post>(|u, p| u.id == p.author_id)
        .filter(|_u, _p| true)
        .count()
}
#[sqlite_query]
fn user_post_comment() -> Vec<(User, Post, Comment)> {
    User::join::<Post>(|u, p| u.id == p.author_id)
        .join::<Comment>(|_u, p, c| p.id == c.post_id)
        .filter(|_u, _p, _c| true)
        .order_by(|_u, _p, c| c.id)
        .all()
}
#[sqlite_query]
fn user_post_comment_tag() -> Vec<(User, Post, Comment, Tag)> {
    User::join::<Post>(|u, p| u.id == p.author_id)
        .join::<Comment>(|_u, p, c| p.id == c.post_id)
        .join::<Tag>(|_u, p, _c, t| p.id == t.post_id)
        .filter(|_u, _p, _c, _t| true)
        .order_by(|_u, _p, c, _t| c.id)
        .all()
}
#[sqlite_query]
fn count_user_posts() -> i64 {
    User::join::<Post>(|u, p| u.id == p.author_id)
        .filter(|_u, _p| true)
        .count()
}
#[sqlite_query]
fn post_titles_by_user(uid: i64) -> Vec<String> {
    User::join::<Post>(|u, p| u.id == p.author_id)
        .filter(|u, _p| u.id == uid)
        .select(|_u, p| p.title)
        .all()
}

#[test]
fn joins() {
    let f = Fixture::new();
    let rows = user_with_posts(&f.conn).unwrap();
    assert_eq!(rows.len(), 4);

    let n = user_left_join_posts(&f.conn).unwrap();
    assert!(n >= 5);

    let rows3 = user_post_comment(&f.conn).unwrap();
    assert_eq!(rows3.len(), 3);

    let rows4 = user_post_comment_tag(&f.conn).unwrap();
    assert!(!rows4.is_empty());

    assert_eq!(count_user_posts(&f.conn).unwrap(), 4);

    let titles = post_titles_by_user(&f.conn, 1).unwrap();
    assert_eq!(titles, vec!["hello", "second"]);
}

#[sqlite_query]
fn users_union() -> Vec<User> {
    User::filter(|u| u.id <= 1)
        .union(User::filter(|u| u.id >= 4))
        .order_by(|u| u.id)
        .all()
}
#[sqlite_query]
fn users_union_all() -> Vec<User> {
    User::filter(|u| u.id <= 1)
        .union_all(User::filter(|u| u.id == 1))
        .order_by(|u| u.id)
        .all()
}
#[sqlite_query]
fn users_intersect() -> Vec<User> {
    User::filter(|u| u.age >= 25)
        .intersect(User::filter(|u| u.active == true))
        .order_by(|u| u.id)
        .all()
}
#[sqlite_query]
fn users_except() -> Vec<User> {
    User::filter(|_u| true)
        .except(User::filter(|u| u.active == false))
        .order_by(|u| u.id)
        .all()
}

#[test]
fn set_ops() {
    let f = Fixture::new();
    let r = users_union(&f.conn).unwrap();
    assert_eq!(r.len(), 3);
    let ids: Vec<i64> = r.iter().map(|u| u.id).collect();
    assert_eq!(ids, vec![1, 4, 5]);

    let r = users_union_all(&f.conn).unwrap();
    assert_eq!(r.len(), 2);

    let r = users_intersect(&f.conn).unwrap();
    assert_eq!(r.len(), 3);

    let r = users_except(&f.conn).unwrap();
    assert_eq!(r.len(), 4);
}

#[sqlite_query]
fn cte_active_users() -> Vec<User> {
    let active = User::filter(|u| u.active == true).cte();
    active.filter(|u| u.age >= 25).order_by(|u| u.id).all()
}

#[sqlite_query]
fn cte_union(min_age: i32) -> Vec<User> {
    let young = User::filter(|u| u.age < 30).cte();
    let actives = User::filter(|u| u.active == true).cte();
    young
        .filter(|u| u.age >= min_age)
        .union(actives.filter(|u| u.age >= min_age))
        .order_by(|u| u.id)
        .all()
}

#[test]
fn ctes() {
    let f = Fixture::new();
    let r = cte_active_users(&f.conn).unwrap();
    let ids: Vec<i64> = r.iter().map(|u| u.id).collect();
    assert_eq!(ids, vec![1, 2, 5]);

    let r = cte_union(&f.conn, 22).unwrap();
    let ids: Vec<i64> = r.iter().map(|u| u.id).collect();
    assert!(ids.contains(&2));
    assert!(ids.contains(&4));
    assert!(ids.contains(&5));
}

#[sqlite_query]
fn users_with_any_post() -> Vec<User> {
    User::filter(|u| exists(Post::filter(|p| p.author_id == u.id)))
        .order_by(|u| u.id)
        .all()
}
#[sqlite_query]
fn users_without_post() -> Vec<User> {
    User::filter(|u| not_exists(Post::filter(|p| p.author_id == u.id)))
        .order_by(|u| u.id)
        .all()
}
#[sqlite_query]
fn user_post_counts() -> Vec<(i64, i64)> {
    User::filter(|_u| true)
        .order_by(|u| u.id)
        .select(|u| (u.id, Post::filter(|p| p.author_id == u.id).count()))
        .all()
}

#[test]
fn subqueries() {
    let f = Fixture::new();
    let r = users_with_any_post(&f.conn).unwrap();
    let ids: Vec<i64> = r.iter().map(|u| u.id).collect();
    assert_eq!(ids, vec![1, 2, 3]);

    let r = users_without_post(&f.conn).unwrap();
    let ids: Vec<i64> = r.iter().map(|u| u.id).collect();
    assert_eq!(ids, vec![4, 5]);

    let counts = user_post_counts(&f.conn).unwrap();
    assert_eq!(counts, vec![(1, 2), (2, 1), (3, 1), (4, 0), (5, 0)]);
}

#[sqlite_query]
fn user_stream() -> Vec<User> {
    User::filter(|_u| true).order_by(|u| u.id).all()
}

#[test]
fn stream_eager_matches_all() {
    let f = Fixture::new();
    let all = user_stream(&f.conn).unwrap();
    assert_eq!(all.len(), 5);
}

#[sqlite_query]
fn item_by_key(a: i32, b: i32) -> Option<Item> {
    Item::filter(|i| i.a == a && i.b == b).first()
}
#[sqlite_query]
fn insert_item(a: i32, b: i32, payload: String) {
    Item::insert(|i| {
        i.a = a;
        i.b = b;
        i.payload = payload;
    })
}

#[test]
fn composite_pk_and_default_table_name() {
    let f = Fixture::new();
    insert_item(&f.conn, 1, 2, "hi".into()).unwrap();
    insert_item(&f.conn, 1, 3, "ho".into()).unwrap();
    let it = item_by_key(&f.conn, 1, 2).unwrap().unwrap();
    assert_eq!(it.payload, "hi");
    assert!(item_by_key(&f.conn, 2, 2).unwrap().is_none());

    assert_eq!(Item::__CARTEL_PK_COL, "a,b");
    assert_eq!(Item::__CARTEL_TABLE, "items");
    assert_eq!(Widget::__CARTEL_TABLE, "widgets");
}

#[sqlite_query]
fn count_widgets_arith(min: i64) -> Option<i64> {
    Widget::filter(|w| w.value >= min).sum(|w| w.value * 2)
}

#[test]
fn arithmetic_in_aggregate() {
    let f = Fixture::new();
    let s = count_widgets_arith(&f.conn, 0).unwrap();
    assert_eq!(s, Some((10 + 20 + 30 + 40 + 50) * 2));
}

#[sqlite_query]
fn user_row_numbers() -> Vec<(i64, i64)> {
    User::filter(|_u| true)
        .order_by(|u| u.id)
        .select(|u| (u.id, row_number().over(|w| w.order_by(u.id))))
        .all()
}

#[test]
fn window_functions() {
    let f = Fixture::new();
    let rows = user_row_numbers(&f.conn).unwrap();
    assert_eq!(rows.len(), 5);
    let rns: Vec<i64> = rows.iter().map(|(_, n)| *n).collect();
    assert_eq!(rns, vec![1, 2, 3, 4, 5]);
}

#[test]
fn transaction_with_query_fn() {
    let mut f = Fixture::new();
    let tx = f.conn.transaction().unwrap();
    let u = user_by_id(&tx, 1).unwrap();
    assert_eq!(u.name, "alice");
    rename_user(&tx, 1, "TX-renamed".into()).unwrap();
    let u2 = user_by_id(&tx, 1).unwrap();
    assert_eq!(u2.name, "TX-renamed");
    tx.rollback().unwrap();
    let u3 = user_by_id(&f.conn, 1).unwrap();
    assert_eq!(u3.name, "alice");
}

#[allow(dead_code)]
const REJECTED_OPS_DOC: () = ();
