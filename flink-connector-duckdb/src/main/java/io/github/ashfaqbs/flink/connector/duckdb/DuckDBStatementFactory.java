package io.github.ashfaqbs.flink.connector.duckdb;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.table.types.logical.RowType;

/** Builds the parameterized INSERT / upsert SQL text for a given schema + key configuration. */
public final class DuckDBStatementFactory implements Serializable {

    private static final long serialVersionUID = 1L;

    private DuckDBStatementFactory() {}

    public static String buildStatement(String tableName, RowType rowType, List<String> keyFields) {
        List<String> columns = rowType.getFieldNames();
        String columnList = String.join(", ", columns);
        String placeholders = columns.stream().map(c -> "?").collect(Collectors.joining(", "));

        String sql = "INSERT INTO " + tableName + " (" + columnList + ") VALUES (" + placeholders + ")";

        if (keyFields.isEmpty()) {
            return sql;
        }

        String conflictTarget = String.join(", ", keyFields);
        String updateAssignments =
                columns.stream()
                        .filter(c -> !keyFields.contains(c))
                        .map(c -> c + " = excluded." + c)
                        .collect(Collectors.joining(", "));

        if (updateAssignments.isEmpty()) {
            // every column is part of the key: nothing to update, just ignore duplicates
            return sql + " ON CONFLICT (" + conflictTarget + ") DO NOTHING";
        }
        return sql + " ON CONFLICT (" + conflictTarget + ") DO UPDATE SET " + updateAssignments;
    }
}
