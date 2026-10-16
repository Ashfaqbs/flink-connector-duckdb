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
        String columnList = columns.stream().map(DuckDBStatementFactory::quote)
                .collect(Collectors.joining(", "));
        String placeholders = columns.stream().map(c -> "?").collect(Collectors.joining(", "));

        String sql =
                "INSERT INTO " + quote(tableName) + " (" + columnList + ") VALUES (" + placeholders + ")";

        if (keyFields.isEmpty()) {
            return sql;
        }

        String conflictTarget =
                keyFields.stream().map(DuckDBStatementFactory::quote).collect(Collectors.joining(", "));
        String updateAssignments =
                columns.stream()
                        .filter(c -> !keyFields.contains(c))
                        .map(c -> quote(c) + " = excluded." + quote(c))
                        .collect(Collectors.joining(", "));

        if (updateAssignments.isEmpty()) {
            // every column is part of the key: nothing to update, just ignore duplicates
            return sql + " ON CONFLICT (" + conflictTarget + ") DO NOTHING";
        }
        return sql + " ON CONFLICT (" + conflictTarget + ") DO UPDATE SET " + updateAssignments;
    }

    /** Double-quotes an identifier so it survives being a DuckDB reserved word, containing
     * spaces/hyphens, or needing case preserved (DuckDB is case-insensitive unless quoted).
     * table-name/column names/key-fields all come from operator-trusted connector config, not
     * external input, but string-concatenating them unquoted into SQL text is still a
     * correctness bug waiting to happen, not just a theoretical injection vector. */
    private static String quote(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
}
