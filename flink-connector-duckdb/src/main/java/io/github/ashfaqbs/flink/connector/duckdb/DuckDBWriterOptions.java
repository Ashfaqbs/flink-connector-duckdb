package io.github.ashfaqbs.flink.connector.duckdb;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/** Immutable, serializable bundle of everything the sink writer needs at runtime. */
public final class DuckDBWriterOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String jdbcUrl;
    private final String tableName;
    private final List<String> keyFields; // empty if plain insert, not upsert
    private final int batchSize;
    private final long flushIntervalMs;

    public DuckDBWriterOptions(
            String jdbcUrl,
            String tableName,
            List<String> keyFields,
            int batchSize,
            long flushIntervalMs) {
        this.jdbcUrl = Objects.requireNonNull(jdbcUrl, "jdbcUrl");
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.keyFields = Objects.requireNonNull(keyFields, "keyFields");
        this.batchSize = batchSize;
        this.flushIntervalMs = flushIntervalMs;
    }

    public String jdbcUrl() {
        return jdbcUrl;
    }

    public String tableName() {
        return tableName;
    }

    public List<String> keyFields() {
        return keyFields;
    }

    public int batchSize() {
        return batchSize;
    }

    public long flushIntervalMs() {
        return flushIntervalMs;
    }
}
