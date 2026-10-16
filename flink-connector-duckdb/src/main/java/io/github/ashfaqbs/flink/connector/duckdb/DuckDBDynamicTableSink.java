package io.github.ashfaqbs.flink.connector.duckdb;

import java.util.List;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.types.logical.RowType;

/** {@link DynamicTableSink} for DuckDB: insert-only, or upsert-by-key when {@code key-fields}
 * is configured (translated to {@code INSERT ... ON CONFLICT ... DO UPDATE}). */
public final class DuckDBDynamicTableSink implements DynamicTableSink {

    private final DuckDBWriterOptions writerOptions;
    private final RowType rowType;
    private final List<String> keyFields;
    private final int sinkParallelism;

    public DuckDBDynamicTableSink(
            DuckDBWriterOptions writerOptions,
            RowType rowType,
            List<String> keyFields,
            int sinkParallelism) {
        this.writerOptions = writerOptions;
        this.rowType = rowType;
        this.keyFields = keyFields;
        this.sinkParallelism = sinkParallelism;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        DuckDBRowConverter converter = new DuckDBRowConverter(rowType);
        String insertSql =
                DuckDBStatementFactory.buildStatement(writerOptions.tableName(), rowType, keyFields);
        DuckDBSink sink = new DuckDBSink(writerOptions, converter, insertSql);
        return SinkV2Provider.of(sink, sinkParallelism);
    }

    @Override
    public DynamicTableSink copy() {
        return new DuckDBDynamicTableSink(writerOptions, rowType, keyFields, sinkParallelism);
    }

    @Override
    public String asSummaryString() {
        return "DuckDB";
    }
}
