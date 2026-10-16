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
 * flushing on batch-size, when the flush interval has elapsed by the time the next row arrives
 * (checked opportunistically in {@link #write}, not on a background timer -- a quiet stream with
 * checkpointing disabled can leave a partial batch unflushed), or when Flink asks for a final
 * flush (checkpoint / end of input).
 *
 * <p>Each flush commits its own JDBC transaction independently of Flink's checkpoint barriers
 * (no two-phase-commit {@code Committer}). In upsert mode ({@code key-fields} set) a replay
 * after restart-from-checkpoint is harmless -- {@code ON CONFLICT} makes it idempotent -- but in
 * plain-insert mode, rows committed after the last successful checkpoint and before a crash will
 * be re-inserted as duplicates on restart. This is an at-least-once sink for inserts, exactly-once
 * only for upserts.
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

        // Built into locals first, only assigned to the final fields once both steps succeed --
        // otherwise a failure in setAutoCommit/prepareStatement after getConnection() succeeds
        // would leak an open connection holding DuckDB's exclusive file lock for the rest of
        // this JVM's lifetime (Flink would keep retrying writer creation in the same process,
        // and every retry would fail to acquire a lock the previous leaked connection never
        // released -- a self-inflicted permanent deadlock).
        Connection conn;
        try {
            conn = DriverManager.getConnection(options.jdbcUrl());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to open DuckDB connection: " + options.jdbcUrl(), e);
        }
        try {
            conn.setAutoCommit(false);
            this.statement = conn.prepareStatement(insertSql);
        } catch (SQLException e) {
            closeQuietly(conn);
            throw new RuntimeException("Failed to prepare DuckDB statement: " + insertSql, e);
        }
        this.connection = conn;
    }

    private static void closeQuietly(Connection conn) {
        try {
            conn.close();
        } catch (SQLException e) {
            LOG.warn("Failed to close DuckDB connection after a setup failure", e);
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
        try {
            flushBuffer();
        } finally {
            statement.close();
            connection.close();
        }
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
            clearBatchQuietly();
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

    private void clearBatchQuietly() {
        // JDBC doesn't guarantee a driver clears its internal batch queue when executeBatch()
        // throws (only guaranteed on success). This writer is currently always discarded after
        // a RuntimeException propagates out of flushBuffer() -- Flink creates a fresh writer on
        // restart -- so this is defense-in-depth against a future change that reuses the writer
        // instance after catching the failure, which would otherwise resubmit stale rows
        // alongside new ones on the next flush.
        try {
            statement.clearBatch();
        } catch (SQLException e) {
            LOG.warn("Failed to clear the batch after a failed flush", e);
        }
    }
}
