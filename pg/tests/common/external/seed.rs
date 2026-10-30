use crate::common::external::Db;
use cartel_pg::{
    api::traits::{Client, Unchecked as _},
    reply,
};
use dope_fiber::abi;
pub(super) const SCHEMA: &[&str] = &[
    "CREATE TABLE q1_0 (id BIGINT PRIMARY KEY, value BIGINT NOT NULL)",
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

pub(super) const ROWS: &[&str] = &[
    "INSERT INTO q1_0 (id, value) VALUES (1, 0), (2, 0), (3, 0)",
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

pub trait Seed<'d> {
    fn seed_types(self) -> impl abi::Fiber<'d, Output = Result<(), reply::Error<'d>>>;
}

impl<'d, C: Client<'d, Schema = Db>> Seed<'d> for C {
    fn seed_types(self) -> impl abi::Fiber<'d, Output = Result<(), reply::Error<'d>>> {
        self.unchecked().command(
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
    }
}
