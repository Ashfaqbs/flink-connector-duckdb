package io.github.ashfaqbs.flink.connector.duckdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

class DuckDBStatementFactoryTest {

    private static RowType schema() {
        return RowType.of(
                new LogicalType[] {
                    new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH), new BigIntType()
                },
                new String[] {"id", "name", "score"});
    }

    @Test
    void plainInsertWhenNoKeyFields() {
        String sql = DuckDBStatementFactory.buildStatement("t", schema(), Collections.emptyList());
        assertThat(sql).isEqualTo("INSERT INTO \"t\" (\"id\", \"name\", \"score\") VALUES (?, ?, ?)");
    }

    @Test
    void upsertWithOnConflictDoUpdateWhenKeyFieldsSet() {
        String sql = DuckDBStatementFactory.buildStatement("t", schema(), List.of("id"));
        assertThat(sql)
                .isEqualTo(
                        "INSERT INTO \"t\" (\"id\", \"name\", \"score\") VALUES (?, ?, ?) "
                                + "ON CONFLICT (\"id\") DO UPDATE SET \"name\" = excluded.\"name\", "
                                + "\"score\" = excluded.\"score\"");
    }

    @Test
    void onConflictDoNothingWhenEveryColumnIsPartOfTheKey() {
        String sql =
                DuckDBStatementFactory.buildStatement(
                        "t", schema(), List.of("id", "name", "score"));
        assertThat(sql)
                .isEqualTo(
                        "INSERT INTO \"t\" (\"id\", \"name\", \"score\") VALUES (?, ?, ?) "
                                + "ON CONFLICT (\"id\", \"name\", \"score\") DO NOTHING");
    }

    @Test
    void quotesIdentifiersThatCollideWithReservedWordsOrContainSpecialCharacters() {
        RowType schemaWithReservedWord =
                RowType.of(
                        new LogicalType[] {new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH)},
                        new String[] {"select", "my-column"});
        String sql =
                DuckDBStatementFactory.buildStatement(
                        "order", schemaWithReservedWord, Collections.emptyList());
        assertThat(sql)
                .isEqualTo("INSERT INTO \"order\" (\"select\", \"my-column\") VALUES (?, ?)");
    }
}
