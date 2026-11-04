use crate::common::query;
use std::{iter, path, ptr};

cartel_sqlite::catalog! {
    pub A;
    statements! { r#BC(0, 1) = "SELECT 1" }
}

cartel_sqlite::catalog! {
    pub AB;
    statements! { C(0, 1) = "SELECT 2" }
}

/// Creates a native UTF-16 fixture before this binary installs Cartel's allocator.
///
/// # Safety
/// SQLite must have no installed Cartel runtime and no concurrent callers.
pub unsafe fn utf16(path: &path::Path) -> Result<(), std::ffi::NulError> {
    use crate::common::catalog::STABLE_COMMANDS;

    // SAFETY: the caller excludes installed allocators and concurrent SQLite callers.
    unsafe {
        initialize(
            path,
            iter::once("PRAGMA encoding='UTF-16le'").chain(STABLE_COMMANDS.iter().copied()),
        )
    }
}

/// Creates a statistics scan that exceeds one native progress interval.
///
/// # Safety
/// SQLite must have no installed Cartel runtime and no concurrent callers.
pub unsafe fn statistics(path: &path::Path) -> Result<(), std::ffi::NulError> {
    use crate::common::catalog::STABLE_COMMANDS;

    let stats = [
        "ANALYZE",
        "WITH RECURSIVE copies(n) AS (
            VALUES(1) UNION ALL SELECT n + 1 FROM copies WHERE n < 1024
        ) INSERT INTO sqlite_stat1 SELECT 'items', NULL, '1' FROM copies",
    ];
    // SAFETY: the caller excludes installed allocators and concurrent SQLite callers.
    unsafe { initialize(path, STABLE_COMMANDS.iter().copied().chain(stats)) }
}

unsafe fn initialize<'sql>(
    path: &path::Path,
    commands: impl Iterator<Item = &'sql str>,
) -> Result<(), std::ffi::NulError> {
    let path = std::ffi::CString::new(path.as_os_str().as_encoded_bytes())?;
    let mut db = ptr::null_mut();
    assert_eq!(
        unsafe { libsqlite3_sys::sqlite3_open(path.as_ptr(), &mut db) },
        libsqlite3_sys::SQLITE_OK
    );
    for sql in commands {
        let sql = std::ffi::CString::new(sql)?;
        assert_eq!(
            unsafe {
                libsqlite3_sys::sqlite3_exec(
                    db,
                    sql.as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut(),
                )
            },
            libsqlite3_sys::SQLITE_OK
        );
    }
    assert_eq!(
        unsafe { libsqlite3_sys::sqlite3_close(db) },
        libsqlite3_sys::SQLITE_OK
    );
    assert_eq!(
        unsafe { libsqlite3_sys::sqlite3_shutdown() },
        libsqlite3_sys::SQLITE_OK
    );
    Ok(())
}

cartel_sqlite::catalog! {
pub IntegrationDatabase;

queries! {
    query::dispatch::r#user_by_id,
    query::dispatch::maybe_user_by_id,
    query::dispatch::all_users,
    query::dispatch::user_names_min_id,
    query::dispatch::user_id_name_pairs,
    query::dispatch::user_id_plus_one,
    query::dispatch::keyword_value,
    query::dispatch::user_blob,
    query::dispatch::unicode_user_name,
    query::mutation::rename_user,
    query::mutation::set_fields,
    query::mutation::insert_keyword,
    query::mutation::rewrite_keyword,
    query::mutation::upsert_keyword,
    query::mutation::upsert_correlated,
    query::mutation::delete_user,
    query::mutation::insert_user,
    query::mutation::insert_all,
    query::mutation::insert_one,
    query::mutation::insert_first,
    query::mutation::copy_older,
    query::mutation::insert_or_ignore,
    query::mutation::upsert_name,
    query::mutation::insert_or_keep,
    query::mutation::update_one,
    query::mutation::delete_one,
    query::mutation::delete_inactive,
    query::filter::users_eq_age,
    query::filter::users_ne_age,
    query::filter::users_gt_age,
    query::filter::users_lt_age,
    query::filter::users_ge_age,
    query::filter::users_le_age,
    query::filter::users_like,
    query::filter::users_not_like,
    query::filter::users_glob,
    query::filter::users_nickname_some,
    query::filter::users_nickname_none,
    query::filter::users_and,
    query::filter::users_or,
    query::filter::users_nested,
    query::filter::users_not_via_ne,
    query::filter::users_ordered_desc,
    query::filter::users_limited,
    query::filter::users_offset,
    query::filter::distinct_ages,
    query::filter::users_limit_param,
    query::filter::users_offset_param,
    query::filter::count_all,
    query::filter::count_active,
    query::filter::sum_ages,
    query::filter::avg_age,
    query::filter::min_age,
    query::filter::max_score,
    query::filter::widgets_count_per_bucket,
    query::filter::widgets_sum_per_bucket,
    query::filter::widgets_having,
    query::relation::user_with_posts,
    query::relation::user_left_join_posts,
    query::relation::user_post_comment,
    query::relation::user_post_comment_tag,
    query::relation::count_user_posts,
    query::relation::post_titles_by_user,
    query::relation::users_union,
    query::relation::users_union_all,
    query::relation::users_intersect,
    query::relation::users_except,
    query::relation::cte_active_users,
    query::relation::cte_union,
    query::relation::users_with_any_post,
    query::relation::users_without_post,
    query::relation::user_post_counts,
    query::stream::users,
    query::item::by_key,
    query::item::insert,
    query::analytic::count_widgets_arith,
    query::analytic::user_row_numbers,
}

statements! {
    VALUE_CLASSES(0, 4) = "SELECT NULL, '', X'', '한국어 √'",
    NULL_COMPOSITES(0, 6) = "SELECT NULL, NULL, 23, NULL, 'mixed', 47",
    NUMERIC_CLASSES(0, 3) = "SELECT -7, 1.25, 1",
    INVALID_TEXT(0, 1) = "SELECT CAST(X'80' AS TEXT)",
    MULTIPLE_ROWS(0, 1) = "SELECT 1 UNION ALL SELECT 2",
    MULTIPLE_TEXT_ROWS(0, 1) = "SELECT 'first' UNION ALL SELECT 'second'",
    ROWS_OVER_LIMIT(0, 1) = "WITH RECURSIVE n(x) AS (
        VALUES(1) UNION ALL SELECT x + 1 FROM n WHERE x < 1025
    ) SELECT x FROM n",
}

pub const COMMANDS: &[&str] = &[
    "CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        age INTEGER NOT NULL,
        score REAL NOT NULL,
        active INTEGER NOT NULL,
        nickname TEXT,
        avatar BLOB
    )",
    "CREATE TABLE posts (
        id INTEGER PRIMARY KEY,
        author_id INTEGER NOT NULL,
        title TEXT NOT NULL,
        body TEXT NOT NULL,
        likes INTEGER NOT NULL DEFAULT 0
    )",
    "CREATE TABLE comments (
        id INTEGER PRIMARY KEY,
        post_id INTEGER NOT NULL,
        author_id INTEGER NOT NULL,
        text TEXT NOT NULL
    )",
    "CREATE TABLE tags (
        id INTEGER PRIMARY KEY,
        post_id INTEGER NOT NULL,
        label TEXT NOT NULL
    )",
    "CREATE TABLE widgets (
        id INTEGER PRIMARY KEY,
        bucket INTEGER NOT NULL,
        value INTEGER NOT NULL
    )",
    "CREATE TABLE items (
        a INTEGER NOT NULL,
        b INTEGER NOT NULL,
        payload TEXT NOT NULL,
        PRIMARY KEY (a, b)
    )",
    "CREATE TABLE \"select\" (\"type\" INTEGER NOT NULL UNIQUE)",
    "CREATE TABLE q1_0 (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)",
    "INSERT INTO users (id, name, age, score, active, nickname, avatar) VALUES
        (1, 'alice',   30, 9.5, 1, 'al',    NULL),
        (2, 'bob',     25, 7.0, 1, NULL,    NULL),
        (3, 'carol',   40, 8.2, 0, 'caz',   x'cafe'),
        (4, 'dave',    22, 6.1, 1, NULL,    x'deadbeef'),
        (5, '한국어 √',  29, 5.0, 1, '한',    NULL)",
    "INSERT INTO posts (id, author_id, title, body, likes) VALUES
        (10, 1, 'hello',  'world',   3),
        (11, 1, 'second', 'post',    1),
        (12, 2, 'bob-1',  'b-body',  7),
        (13, 3, 'carol',  'c-body',  0)",
    "INSERT INTO comments (id, post_id, author_id, text) VALUES
        (100, 10, 2, 'nice'),
        (101, 10, 3, 'cool'),
        (102, 12, 1, 'lol')",
    "INSERT INTO tags (id, post_id, label) VALUES
        (1000, 10, 'rust'),
        (1001, 10, 'sql'),
        (1002, 12, 'rust')",
    "INSERT INTO widgets (id, bucket, value) VALUES
        (1, 1, 10), (2, 1, 20), (3, 2, 30), (4, 2, 40), (5, 3, 50)",
    "INSERT INTO \"select\" (\"type\") VALUES (7)",
    "INSERT INTO q1_0 (id, value) VALUES (1, 0), (2, 0), (3, 0)",
    ];

}
