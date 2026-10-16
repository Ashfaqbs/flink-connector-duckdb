package io.github.ashfaqbs.flink.connector.duckdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.math.BigDecimal;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Real integration test against an actual DuckDB file -- no Docker/testcontainers needed since
 * DuckDB is embedded, unlike the Qdrant connector's tests.
 */
class DuckDBSinkWriterTest {

    private static final RowType SCHEMA =
            RowType.of(
                    new LogicalType[] {new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH)},
                    new String[] {"id", "name"});

    @TempDir File tempDir;
    private String jdbcUrl;

    @BeforeEach
    void setUp() throws Exception {
        jdbcUrl = "jdbc:duckdb:" + new File(tempDir, "test.db").getAbsolutePath();
        try (Connection setupConn = DriverManager.getConnection(jdbcUrl);
                Statement statement = setupConn.createStatement()) {
            statement.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)");
        }
    }

    @AfterEach
    void tearDown() {
        // Each test opens its own temp DB file; nothing shared to clean up beyond @TempDir.
    }

    @Test
    void plainInsertWritesAllRows() throws Exception {
        DuckDBWriterOptions options = new DuckDBWriterOptions(jdbcUrl, "t", Collections.emptyList(), 10, 60_000L);
        DuckDBRowConverter converter = new DuckDBRowConverter(SCHEMA);
        String sql = DuckDBStatementFactory.buildStatement("t", SCHEMA, Collections.emptyList());

        try (DuckDBSinkWriter writer = new DuckDBSinkWriter(options, converter, sql)) {
            writer.write(GenericRowData.of(1L, StringData.fromString("alice")), null);
            writer.write(GenericRowData.of(2L, StringData.fromString("bob")), null);
            writer.flush(true);
        }

        assertThat(rowCount()).isEqualTo(2);
    }

    @Test
    void upsertOverwritesOnConflictingKey() throws Exception {
        DuckDBWriterOptions options = new DuckDBWriterOptions(jdbcUrl, "t", List.of("id"), 10, 60_000L);
        DuckDBRowConverter converter = new DuckDBRowConverter(SCHEMA);
        String sql = DuckDBStatementFactory.buildStatement("t", SCHEMA, List.of("id"));

        try (DuckDBSinkWriter writer = new DuckDBSinkWriter(options, converter, sql)) {
            writer.write(GenericRowData.of(1L, StringData.fromString("alice")), null);
            writer.flush(true);
            writer.write(GenericRowData.of(1L, StringData.fromString("alice-updated")), null);
            writer.flush(true);
        }

        assertThat(rowCount()).isEqualTo(1);
        try (Connection conn = DriverManager.getConnection(jdbcUrl);
                Statement statement = conn.createStatement();
                ResultSet rs = statement.executeQuery("SELECT name FROM t WHERE id = 1")) {
            rs.next();
            assertThat(rs.getString("name")).isEqualTo("alice-updated");
        }
    }

    @Test
    void batchSizeTriggersFlushWithoutExplicitFlushCall() throws Exception {
        DuckDBWriterOptions options = new DuckDBWriterOptions(jdbcUrl, "t", Collections.emptyList(), 2, 60_000L);
        DuckDBRowConverter converter = new DuckDBRowConverter(SCHEMA);
        String sql = DuckDBStatementFactory.buildStatement("t", SCHEMA, Collections.emptyList());

        DuckDBSinkWriter writer = new DuckDBSinkWriter(options, converter, sql);
        writer.write(GenericRowData.of(1L, StringData.fromString("a")), null);
        writer.write(GenericRowData.of(2L, StringData.fromString("b")), null); // hits batch-size=2

        assertThat(rowCount()).isEqualTo(2);
        writer.close();
    }

    @Test
    void plainInsertAcrossMultipleFlushCyclesAppendsRows() throws Exception {
        DuckDBWriterOptions options = new DuckDBWriterOptions(jdbcUrl, "t", Collections.emptyList(), 10, 60_000L);
        DuckDBRowConverter converter = new DuckDBRowConverter(SCHEMA);
        String sql = DuckDBStatementFactory.buildStatement("t", SCHEMA, Collections.emptyList());

        try (DuckDBSinkWriter writer = new DuckDBSinkWriter(options, converter, sql)) {
            writer.write(GenericRowData.of(1L, StringData.fromString("a")), null);
            writer.flush(true);
            writer.write(GenericRowData.of(2L, StringData.fromString("b")), null);
            writer.flush(true);
            writer.write(GenericRowData.of(3L, StringData.fromString("c")), null);
            writer.flush(true);
        }

        assertThat(rowCount()).isEqualTo(3);
    }

    @Test
    void decimalDateTimestampAndNullColumnsRoundTripCorrectly() throws Exception {
        try (Connection setupConn = DriverManager.getConnection(jdbcUrl);
                Statement statement = setupConn.createStatement()) {
            statement.execute(
                    "CREATE TABLE rich (id BIGINT PRIMARY KEY, amount DECIMAL(10,2), "
                            + "d DATE, ts TIMESTAMP, note VARCHAR)");
        }

        RowType richSchema =
                RowType.of(
                        new LogicalType[] {
                            new BigIntType(),
                            new DecimalType(10, 2),
                            new DateType(),
                            new TimestampType(6),
                            new VarCharType(VarCharType.MAX_LENGTH)
                        },
                        new String[] {"id", "amount", "d", "ts", "note"});
        DuckDBWriterOptions options = new DuckDBWriterOptions(jdbcUrl, "rich", Collections.emptyList(), 10, 60_000L);
        DuckDBRowConverter converter = new DuckDBRowConverter(richSchema);
        String sql = DuckDBStatementFactory.buildStatement("rich", richSchema, Collections.emptyList());

        java.time.LocalDate date = java.time.LocalDate.of(2026, 10, 16);
        java.time.LocalDateTime timestamp = java.time.LocalDateTime.of(2026, 10, 16, 12, 30, 0);
        try (DuckDBSinkWriter writer = new DuckDBSinkWriter(options, converter, sql)) {
            writer.write(
                    GenericRowData.of(
                            1L,
                            DecimalData.fromBigDecimal(new BigDecimal("123.45"), 10, 2),
                            (int) date.toEpochDay(),
                            TimestampData.fromLocalDateTime(timestamp),
                            StringData.fromString("has note")),
                    null);
            // second row has a null decimal and a null note -- confirms setNull uses the right
            // SQL type code rather than erroring or silently coercing to something wrong
            GenericRowData nullRow = new GenericRowData(5);
            nullRow.setField(0, 2L);
            nullRow.setField(1, null);
            nullRow.setField(2, (int) date.toEpochDay());
            nullRow.setField(3, TimestampData.fromLocalDateTime(timestamp));
            nullRow.setField(4, null);
            writer.write(nullRow, null);
            writer.flush(true);
        }

        try (Connection conn = DriverManager.getConnection(jdbcUrl);
                Statement statement = conn.createStatement();
                ResultSet rs = statement.executeQuery("SELECT * FROM rich ORDER BY id")) {
            rs.next();
            assertThat(rs.getBigDecimal("amount")).isEqualByComparingTo("123.45");
            assertThat(rs.getDate("d").toLocalDate()).isEqualTo(date);
            assertThat(rs.getTimestamp("ts").toLocalDateTime()).isEqualTo(timestamp);
            assertThat(rs.getString("note")).isEqualTo("has note");

            rs.next();
            assertThat(rs.getObject("amount")).isNull();
            assertThat(rs.getString("note")).isNull();
        }
    }

    private long rowCount() throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl);
                Statement statement = conn.createStatement();
                ResultSet rs = statement.executeQuery("SELECT COUNT(*) AS c FROM t")) {
            rs.next();
            return rs.getLong("c");
        }
    }
}
