package io.github.ashfaqbs.flink.connector.duckdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Binds each incoming row onto a reused {@link PreparedStatement} immediately (a Flink {@code
 * RowData} is not guaranteed to stay valid after {@link #write} returns -- the runtime may reuse
 * or mutate the underlying object on the next record), batching via JDBC's {@code addBatch} and
 * flushing on batch-size, on a timer, or when Flink asks for a final flush (checkpoint / end of
 * input).
 *
 * <p>DuckDB locks its database file for exclusive single-process read-write access, so this
 * writer must not run with sink parallelism above 1 against a local file target -- see {@link
 * DuckDBConnectorOptions#SINK_PARALLELISM}.
 */
public final class DuckDBSinkWriter implements SinkWriter<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(DuckDBSinkWriter.class);

    private final DuckDBWriterOptions options;
    private final DuckDBRowConverter converter;
    private final Connection connection;
    private final PreparedStatement statement;

    private int pendingCount;
    private long lastFlushMillis;

    public DuckDBSinkWriter(
            DuckDBWriterOptions options, DuckDBRowConverter converter, String insertSql) {
        this.options = options;
        this.converter = converter;
        this.pendingCount = 0;
        this.lastFlushMillis = System.currentTimeMillis();
        try {
            this.connection = DriverManager.getConnection(options.jdbcUrl());
            this.connection.setAutoCommit(false);
            this.statement = connection.prepareStatement(insertSql);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to open DuckDB connection: " + options.jdbcUrl(), e);
        }
    }

    @Override
    public void write(RowData element, Context context) {
        try {
            converter.bind(statement, element);
            statement.addBatch();
            pendingCount++;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to bind row for DuckDB insert", e);
        }
        if (pendingCount >= options.batchSize()
                || System.currentTimeMillis() - lastFlushMillis >= options.flushIntervalMs()) {
            flushBuffer();
        }
    }

    @Override
    public void flush(boolean endOfInput) {
        flushBuffer();
    }

    @Override
    public void close() throws Exception {
        flushBuffer();
        statement.close();
        connection.close();
    }

    private void flushBuffer() {
        if (pendingCount == 0) {
            lastFlushMillis = System.currentTimeMillis();
            return;
        }
        int batchSize = pendingCount;
        pendingCount = 0;
        lastFlushMillis = System.currentTimeMillis();

        // No retry here, unlike the Qdrant sink: once a JDBC driver's executeBatch() call
        // fails, re-invoking it does not re-submit the same rows (the batch it had buffered
        // is already consumed), so a retry loop would risk silently "succeeding" on an empty
        // batch instead of actually writing anything. A local embedded DB's failures (bad
        // data, constraint violation, disk full) are also not the kind that transient retry
        // helps with, unlike a network call. Fail the job and let Flink's own restart
        // strategy handle it instead.
        try {
            statement.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            rollbackQuietly();
            throw new RuntimeException(
                    "Failed to write "
                            + batchSize
                            + " row(s) to DuckDB table '"
                            + options.tableName()
                            + "'",
                    e);
        }
    }

    private void rollbackQuietly() {
        try {
            connection.rollback();
        } catch (SQLException e) {
            LOG.warn("Rollback after a failed batch also failed", e);
        }
    }
}
