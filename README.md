# flink-connector-duckdb

[![CI](https://github.com/Ashfaqbs/flink-connector-duckdb/actions/workflows/ci.yml/badge.svg)](https://github.com/Ashfaqbs/flink-connector-duckdb/actions/workflows/ci.yml)

**Write Apache Flink SQL results straight into [DuckDB](https://duckdb.org) — no custom
`DataStream` job, no hand-rolled JDBC plumbing.**

```sql
CREATE TABLE sink_table (id BIGINT, name STRING, score DOUBLE)
WITH ('connector' = 'duckdb', 'database-path' = '/data/my.db', 'table-name' = 't', 'key-fields' = 'id');

INSERT INTO sink_table SELECT id, name, score FROM source_stream;
```

That's it — plain insert by default, or upsert-by-key when you set `key-fields`.

License: Apache 2.0 · Targets Flink **1.20 (LTS)** · Status: **v1, sink-only**

---

## Why this exists

DuckDB is one of the most widely used embedded analytical databases, and as of this writing there
is no official Flink SQL connector for it, and no well-known community one either — a widely-used
store, a widely-used streaming engine, nothing connecting them in SQL. This connector (and its
sibling, [flink-connector-qdrant](https://github.com/Ashfaqbs/flink-connector-qdrant)) close that
gap with a proper dynamic table factory, not a one-off `DataStream` job per project.

## Quickstart

```bash
git clone https://github.com/Ashfaqbs/flink-connector-duckdb.git && cd flink-connector-duckdb
mvn clean install
```

Try it against a local Flink cluster with one command:

```bash
mkdir -p data
python -c "import duckdb; duckdb.connect('data/test.db').execute('CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR, score DOUBLE)')"
docker compose up -d
# Flink UI: http://localhost:8081
```

Then run the SQL from the top of this README via `docker compose exec jobmanager
./bin/sql-client.sh` (`'database-path' = '/data/test.db'`, `'table-name' = 't'`). Full walkthrough
in [Manually trying it](#manually-trying-it-against-a-local-flink-cluster) below.

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
| `sink.parallelism` | no | `1` | See "Status / limitations" below — rejected above 1 against a local file target |

Supported column types: `BOOLEAN`, `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, `FLOAT`, `DOUBLE`,
`DECIMAL`, `CHAR`, `VARCHAR`, `DATE`, `TIMESTAMP` (without time zone). Anything else fails fast
with a clear error at SQL-planning time, not silently at runtime.

## Status / limitations

- **Sink only**, insert or upsert-by-key — no changelog/retract-stream upsert (the kind an
  aggregation or join upstream of the sink would produce), only `ON CONFLICT`-on-duplicate-insert.
- **Table must already exist** — this connector doesn't create it for you.
- **`sink.parallelism` is enforced, not just documented**: DuckDB locks its database file for
  exclusive access from one OS *process* — but not from a second connection within the *same*
  JVM, which is exactly how Flink runs multiple subtasks sharing a TaskManager slot by default. So
  this connector validates `sink.parallelism` at table-creation time and rejects anything above 1
  against a local file, rather than letting it through to a stuck/racing job. Only raise it
  against a concurrency-safe remote target (`jdbc:duckdb:md:...` for MotherDuck).
- **At-least-once for plain inserts, exactly-once for upserts**: each flush commits independently
  of Flink's checkpoint barriers (no two-phase-commit). On restart-from-checkpoint, rows written
  after the last successful checkpoint get replayed — harmless with `key-fields` set (`ON
  CONFLICT` makes it idempotent), duplicated without it.

## Build & test

```bash
mvn clean install   # builds both modules — 14/14 tests passing, no Docker needed
```

- `flink-connector-duckdb/` — the connector itself, depends on Flink as `provided`.
- `flink-sql-connector-duckdb/` — shaded fat-jar bundling the DuckDB JDBC driver (including its
  native binaries for multiple platforms, hence ~80MB), for dropping into Flink's `lib/` or the
  SQL client's jar path.

Tests need no Docker/testcontainers at all — DuckDB is embedded, so they open a real temporary
`.db` file directly and verify plain inserts, upserts, DECIMAL/DATE/TIMESTAMP/null round-tripping,
and batch-size-triggered flushing against it for real, not mocked.

> **Note**: if you change code in `flink-connector-duckdb` and rebuild, use `mvn install` (not
> just `package`) before testing the shaded jar in `flink-sql-connector-duckdb/target/` — a plain
> `package` across both modules can silently repackage a stale cached copy of the connector jar
> from `~/.m2` instead of your fresh build.

## Manually trying it against a local Flink cluster

```bash
mvn clean package
mkdir -p data
python -c "import duckdb; duckdb.connect('data/test.db').execute('CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR, score DOUBLE)')"
docker compose up -d
docker compose exec jobmanager ./bin/sql-client.sh
```

Then, inside the SQL client, run the `CREATE TABLE` + `INSERT INTO` from the top of this README
(`'database-path' = '/data/test.db'`). Verified end-to-end this way, including the upsert path
(re-inserting an existing key overwrites in place) and the parallelism guard (setting
`sink.parallelism = 4` is rejected at planning time with a clear error, instead of leaving the job
stuck waiting on task slots it will never get).

## License

Apache License 2.0.
