package io.github.ashfaqbs.flink.connector.duckdb;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Options accepted by {@code WITH (...)} on a table using {@code connector = 'duckdb'}. */
public final class DuckDBConnectorOptions {

    public static final ConfigOption<String> DATABASE_PATH =
            ConfigOptions.key("database-path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Path to the DuckDB database file (or a full DuckDB JDBC URL, e.g. "
                                    + "'jdbc:duckdb:md:mydb' for MotherDuck). A plain path is "
                                    + "turned into 'jdbc:duckdb:<path>'.");

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Target table name. Must already exist.");

    public static final ConfigOption<String> KEY_FIELDS =
            ConfigOptions.key("key-fields")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Comma-separated column names forming the primary/unique key the "
                                    + "target table was created with. When set, writes use "
                                    + "'INSERT ... ON CONFLICT (key-fields) DO UPDATE' (upsert). "
                                    + "When unset, writes are plain inserts.");

    public static final ConfigOption<Integer> SINK_BATCH_SIZE =
            ConfigOptions.key("sink.batch-size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Number of rows to buffer before a batch insert/upsert.");

    public static final ConfigOption<Long> SINK_FLUSH_INTERVAL_MS =
            ConfigOptions.key("sink.flush-interval-ms")
                    .longType()
                    .defaultValue(1000L)
                    .withDescription(
                            "Max time a batch is held before it's flushed, even if not full.");

    public static final ConfigOption<Integer> SINK_PARALLELISM =
            ConfigOptions.key("sink.parallelism")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "Fixed parallelism for the sink operator. DuckDB locks a local "
                                    + "database file for exclusive single-process read-write "
                                    + "access, so more than one concurrent writer against the "
                                    + "same file will fail at runtime -- this defaults to 1 for "
                                    + "that reason. Only raise it if the target is a "
                                    + "concurrency-safe remote endpoint (e.g. MotherDuck).");

    private DuckDBConnectorOptions() {}
}
