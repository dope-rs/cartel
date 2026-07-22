use cartel_pg::PgTable;
use cartel_pg::dsl::*;

#[derive(PgTable)]
#[allow(dead_code)]
struct User {
    #[pk]
    id: i64,
    name: String,
    age: i32,
}

#[derive(PgTable)]
#[allow(dead_code)]
struct Post {
    #[pk]
    id: i64,
    author_id: i64,
    title: String,
}

#[derive(PgTable)]
#[allow(dead_code)]
struct Doc {
    #[pk]
    id: i64,
    payload: Vec<u8>,
}

#[allow(dead_code, unreachable_code, clippy::let_unit_value)]
fn typecheck_basic() {
    let _: User = User::filter(|user| user.id == 1).one();
    let _: Option<User> = User::filter(|user| user.id == 1).first();
    let _: Vec<User> = User::filter(|user| user.id >= 1).all();
    let _: i64 = User::filter(|user| user.age >= 18).count();
    let _: Option<i32> = User::filter(|user| user.age >= 18).max(|user| user.age);
    let _: Option<f64> = User::filter(|user| user.age >= 18).avg(|user| user.age);

    let _: Vec<User> = User::filter(|user| user.id >= 1)
        .order_by_desc(|user| user.id)
        .limit(10)
        .offset(5)
        .all();
    let _: Vec<User> = User::filter(|user| user.id >= 1).distinct().all();
    let _: User = User::filter(|user| user.id == 1).for_update().one();

    let name_pattern = String::from("a%");
    let _: Vec<User> = User::filter(|user| user.name.like(&name_pattern)).all();

    let user_ids = vec![1_i64, 2, 3];
    let _: Vec<User> = User::filter(|user| user.id.in_(&user_ids)).all();
}

#[allow(dead_code, unreachable_code)]
fn typecheck_join() {
    let _: Vec<Post> = Post::join::<User>(|post, user| post.author_id == user.id)
        .filter(|_post, user| user.id >= 1)
        .all();
    let _: Vec<(Post, User)> = Post::join::<User>(|post, user| post.author_id == user.id)
        .filter(|post, user| post.id >= 1 && user.id >= 1)
        .all();
}

#[allow(dead_code, unreachable_code)]
fn typecheck_aggregate() {
    let _: Vec<(i32, i64)> = User::filter(|user| user.age >= 18)
        .group_by(|user| user.age)
        .count();
    let _: Vec<(i32, i64)> = User::filter(|user| user.age >= 18)
        .group_by(|user| user.age)
        .having(|_user, aggregate| aggregate.count() > 10)
        .count();
}

#[allow(dead_code, unreachable_code)]
fn typecheck_mutations() {
    User::filter(|user| user.id == 1).update(|user| {
        user.name = String::new();
    });
    let _: User = User::filter(|user| user.id == 1)
        .update(|user| {
            user.name = String::new();
        })
        .returning_one();
    User::filter(|user| user.id == 1).delete();

    User::insert(|user| {
        user.id = 1;
    });
    User::insert(|user| {
        user.id = 1;
    })
    .on_conflict_do_nothing();
    User::insert(|user| {
        user.id = 1;
    })
    .on_conflict(|user| user.id)
    .do_nothing();
}

#[allow(dead_code, unreachable_code)]
fn typecheck_set_operations() {
    let _: Vec<User> = User::filter(|user| user.age < 30)
        .union(User::filter(|user| user.age > 60))
        .all();
}

#[allow(dead_code, unreachable_code)]
fn typecheck_subqueries() {
    let _: Vec<User> =
        User::filter(|user| exists(Post::filter(|post| post.author_id == user.id))).all();
    let _: Vec<User> =
        User::filter(|user| not_exists(Post::filter(|post| post.author_id == user.id))).all();
}

#[allow(dead_code, unreachable_code)]
fn typecheck_common_table_expressions() {
    let young_users = User::filter(|user| user.age < 30).cte();
    let older_users = User::filter(|user| user.age > 60).cte();
    let _: Vec<User> = young_users.union(older_users).all();
}

#[allow(dead_code, unreachable_code)]
fn typecheck_window_expressions() {
    let _: Vec<i64> = User::filter(|user| user.age >= 18)
        .select(|user| user.id)
        .all();
    let _: Vec<(i64, i64)> = User::filter(|user| user.age >= 18)
        .select(|user| {
            (
                user.id,
                row_number().over(|window| window.partition_by(user.age).order_by(user.id)),
            )
        })
        .all();
}
