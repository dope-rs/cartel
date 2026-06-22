use std::time::Instant;

use cartel_sqlite::{Connection, params};

fn main() -> cartel_sqlite::Result<()> {
    let iterations: usize = std::env::args()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000);

    let conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE kv (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)")?;

    let started = Instant::now();
    for i in 0..iterations {
        let value = (i as i64) % 1024;
        conn.execute("INSERT INTO kv (v) VALUES (?1)", params![value])?;

        if i % 8 == 0 {
            let count: i64 =
                conn.query_row("SELECT COUNT(*) FROM kv WHERE v >= 0", [], |row| row.get(0))?;
            assert!(count > 0, "expected positive row count");
        }
    }

    let tx = conn.unchecked_transaction()?;
    tx.execute("UPDATE kv SET v = v + 1 WHERE (id % 2) = 0", [])?;
    tx.commit()?;

    let elapsed = started.elapsed();
    let throughput = iterations as f64 / elapsed.as_secs_f64();
    println!(
        "sqlite mixed stress: iterations={iterations} elapsed_ms={} throughput_ops_s={throughput:.0}",
        elapsed.as_millis()
    );
    Ok(())
}
