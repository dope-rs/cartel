use std::io;
use std::time::{Duration, Instant};

use cartel_sqlite::{Connection, params};

const DEFAULT_OPERATION_COUNT: usize = 100_000;
const DEFAULT_KEY_COUNT: usize = 1_024;
const OPERATIONS_PER_CYCLE: usize = 5;
const READ_OPERATIONS_PER_CYCLE: usize = 4;
const WRITE_INCREMENT: i64 = 1;

type ExampleError = Box<dyn std::error::Error>;

struct WorkloadConfig {
    operation_count: usize,
    key_count: usize,
}

impl WorkloadConfig {
    fn from_arguments() -> Result<Self, ExampleError> {
        let mut arguments = std::env::args().skip(1);
        let operation_count =
            positive_count(arguments.next(), DEFAULT_OPERATION_COUNT, "operation count")?;
        let key_count = positive_count(arguments.next(), DEFAULT_KEY_COUNT, "key count")?;
        if let Some(unexpected_argument) = arguments.next() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unexpected argument: {unexpected_argument}"),
            )
            .into());
        }
        i64::try_from(key_count).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "key count exceeds SQLite INTEGER",
            )
        })?;
        Ok(Self {
            operation_count,
            key_count,
        })
    }
}

struct WorkloadStats {
    read_operations: usize,
    write_operations: usize,
    checksum: i64,
    elapsed: Duration,
}

impl WorkloadStats {
    fn operation_count(&self) -> usize {
        self.read_operations + self.write_operations
    }

    fn throughput(&self) -> f64 {
        if self.elapsed.is_zero() {
            f64::INFINITY
        } else {
            self.operation_count() as f64 / self.elapsed.as_secs_f64()
        }
    }
}

fn positive_count(
    argument: Option<String>,
    default: usize,
    name: &str,
) -> Result<usize, ExampleError> {
    let count = match argument {
        Some(value) => value.parse::<usize>().map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid {name} {value:?}: {error}"),
            )
        })?,
        None => default,
    };
    if count == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} must be positive"),
        )
        .into());
    }
    Ok(count)
}

fn create_schema(connection: &Connection) -> cartel_sqlite::Result<()> {
    connection.execute_batch(
        "CREATE TABLE kv (
            id INTEGER PRIMARY KEY,
            value INTEGER NOT NULL
        )",
    )
}

fn seed_keys(connection: &mut Connection, key_count: usize) -> Result<(), ExampleError> {
    let transaction = connection.transaction()?;
    {
        let mut insert_statement =
            transaction.prepare("INSERT INTO kv (id, value) VALUES (?1, ?2)")?;
        for key_index in 0..key_count {
            let key = i64::try_from(key_index + 1)?;
            insert_statement.execute(params![key, key])?;
        }
    }
    transaction.commit()?;
    Ok(())
}

fn execute_workload(
    connection: &mut Connection,
    config: &WorkloadConfig,
) -> Result<WorkloadStats, ExampleError> {
    let started = Instant::now();
    let transaction = connection.transaction()?;
    let mut read_operations = 0;
    let mut write_operations = 0;
    let mut checksum = 0_i64;
    {
        let mut point_lookup = transaction.prepare("SELECT value FROM kv WHERE id = ?1")?;
        let mut point_update =
            transaction.prepare("UPDATE kv SET value = value + ?2 WHERE id = ?1")?;
        for operation_index in 0..config.operation_count {
            let key_index = operation_index % config.key_count;
            let key = i64::try_from(key_index + 1)?;
            let cycle_index = operation_index % OPERATIONS_PER_CYCLE;
            if cycle_index < READ_OPERATIONS_PER_CYCLE {
                let value = point_lookup.query_row(params![key], |row| row.get::<_, i64>(0))?;
                checksum = checksum.wrapping_add(value);
                read_operations += 1;
            } else {
                let changed_rows = point_update.execute(params![key, WRITE_INCREMENT])?;
                if changed_rows != 1 {
                    return Err(io::Error::other(format!(
                        "point update changed {changed_rows} rows for key {key}"
                    ))
                    .into());
                }
                write_operations += 1;
            }
        }
    }
    transaction.commit()?;
    let elapsed = started.elapsed();
    let stats = WorkloadStats {
        read_operations,
        write_operations,
        checksum,
        elapsed,
    };
    if stats.operation_count() != config.operation_count {
        return Err(io::Error::other(format!(
            "completed {} of {} workload operations",
            stats.operation_count(),
            config.operation_count
        ))
        .into());
    }
    Ok(stats)
}

fn main() -> Result<(), ExampleError> {
    let config = WorkloadConfig::from_arguments()?;
    let mut connection = Connection::open_in_memory()?;
    create_schema(&connection)?;
    seed_keys(&mut connection, config.key_count)?;
    let stats = execute_workload(&mut connection, &config)?;

    println!(
        "sqlite mixed workload: operations={} reads={} writes={} keys={} elapsed_ms={:.3} throughput_ops_s={:.0} checksum={}",
        stats.operation_count(),
        stats.read_operations,
        stats.write_operations,
        config.key_count,
        stats.elapsed.as_secs_f64() * 1_000.0,
        stats.throughput(),
        stats.checksum,
    );
    Ok(())
}
