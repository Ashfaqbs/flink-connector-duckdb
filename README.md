# flink-connector-duckdb

An Apache Flink Table/SQL **sink** connector for [DuckDB](https://duckdb.org) — write a stream
into a DuckDB table via SQL, insert-only or upsert-by-key.

Targets Flink **1.20 (LTS)**. Same motivation and structural shape as
[flink-connector-qdrant](https://github.com/Ashfaqbs/flink-connector-qdrant) in this account, and
both follow [itinycheng/flink-connector-clickhouse](https://github.com/itinycheng/flink-connector-clickhouse)'s
pattern: there's no official (or even well-known community) Flink SQL connector for DuckDB despite
it being a widely used embedded analytical database — the same kind of gap ClickHouse had before
that connector existed.

## Status

v1, sink-only. Insert-only by default; set `key-fields` for upsert (`INSERT ... ON CONFLICT (...)
DO UPDATE`, DuckDB's native upsert syntax).

## Important constraint: sink parallelism

DuckDB locks a local database file for **exclusive single-process read-write access**. More than
one concurrent writer against the same file — whether two Flink subtasks or two unrelated
processes — fails at the file-lock level, not gracefully. Because of this, `sink.parallelism`
**defaults to 1** and the connector always pins the sink operator to that parallelism regardless
of the job's overall parallelism. Only raise it if the target is a concurrency-safe remote
endpoint (e.g. a `jdbc:duckdb:md:...` MotherDuck database), not a local file.

## Usage

```sql
CREATE TABLE sink_table (
    id    BIGINT,
    name  STRING,
    score DOUBLE
) WITH (
    'connector'     = 'duckdb',
    'database-path' = '/data/my.db',
    'table-name'    = 't',
    'key-fields'    = 'id'
);

INSERT INTO sink_table SELECT id, name, score FROM source_stream;
```

### Options

| Option | Required | Default | Notes |
|---|---|---|---|
| `database-path` | yes | — | A file path (turned into `jdbc:duckdb:<path>`), or a full JDBC URL (e.g. `jdbc:duckdb:md:mydb` for MotherDuck) |
| `table-name` | yes | — | Must already exist — this connector doesn't create tables |
| `key-fields` | no | — | Comma-separated column names matching the table's primary/unique key. When set, writes upsert via `ON CONFLICT`. When unset, writes are plain inserts |
| `sink.batch-size` | no | `1000` | Rows buffered before a batch insert/upsert |
| `sink.flush-interval-ms` | no | `1000` | Max time a partial batch is held |
| `sink.parallelism` | no | `1` | See "Important constraint" above — don't raise this against a local file target |

Supported column types: `BOOLEAN`, `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, `FLOAT`, `DOUBLE`,
`DECIMAL`, `CHAR`, `VARCHAR`, `DATE`, `TIMESTAMP` (without time zone). Anything else fails fast at
job-submission time with a clear error.

## Build

```
mvn clean install
```

- `flink-connector-duckdb/` — the connector itself, depends on Flink as `provided`.
- `flink-sql-connector-duckdb/` — shaded fat-jar bundling the DuckDB JDBC driver (including its
  native binaries for multiple platforms, hence ~80MB), for dropping into Flink's `lib/` or the
  SQL client's jar path.

## Test

```
mvn test
```

Unlike the Qdrant connector, this one's integration tests need no Docker/testcontainers at all —
DuckDB is embedded, so the tests open a real temporary `.db` file directly
(`DuckDBSinkWriterTest`) and verify plain inserts, upserts, and batch-size-triggered flushing
against it for real, not mocked.

## Manually trying it against a local Flink cluster

```
mvn clean package
mkdir -p data
python -c "import duckdb; duckdb.connect('data/test.db').execute('CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR, score DOUBLE)')"
docker compose up -d
docker compose exec jobmanager ./bin/sql-client.sh
```

Then, inside the SQL client, `CREATE TABLE ... WITH ('connector' = 'duckdb', 'database-path' =
'/data/test.db', ...)` and `INSERT INTO ... VALUES (...)`. Verified end-to-end this way, including
the upsert path (re-inserting an existing key overwrites in place, confirmed by querying the file
directly with Python's `duckdb` module after each run).

## License

Apache License 2.0.
