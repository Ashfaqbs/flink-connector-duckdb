package io.github.ashfaqbs.flink.connector.duckdb;

import static io.github.ashfaqbs.flink.connector.duckdb.DuckDBConnectorOptions.DATABASE_PATH;
import static io.github.ashfaqbs.flink.connector.duckdb.DuckDBConnectorOptions.KEY_FIELDS;
import static io.github.ashfaqbs.flink.connector.duckdb.DuckDBConnectorOptions.SINK_BATCH_SIZE;
import static io.github.ashfaqbs.flink.connector.duckdb.DuckDBConnectorOptions.SINK_FLUSH_INTERVAL_MS;
import static io.github.ashfaqbs.flink.connector.duckdb.DuckDBConnectorOptions.SINK_PARALLELISM;
import static io.github.ashfaqbs.flink.connector.duckdb.DuckDBConnectorOptions.TABLE_NAME;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.RowType;

/** SQL/Table factory for {@code connector = 'duckdb'}. */
public final class DuckDBDynamicTableFactory implements DynamicTableSinkFactory {

    public static final String IDENTIFIER = "duckdb";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig config = helper.getOptions();

        RowType rowType =
                (RowType) context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType()
                        .getLogicalType();

        String databasePath = config.get(DATABASE_PATH);
        String jdbcUrl = databasePath.startsWith("jdbc:") ? databasePath : "jdbc:duckdb:" + databasePath;

        String keyFieldsRaw = config.get(KEY_FIELDS);
        List<String> keyFields =
                keyFieldsRaw == null || keyFieldsRaw.isBlank()
                        ? Collections.emptyList()
                        : Arrays.asList(keyFieldsRaw.split("\\s*,\\s*"));
        validateKeyFields(rowType, keyFields);

        int sinkParallelism = config.get(SINK_PARALLELISM);
        validateParallelism(sinkParallelism, jdbcUrl);

        DuckDBWriterOptions writerOptions =
                new DuckDBWriterOptions(
                        jdbcUrl,
                        config.get(TABLE_NAME),
                        keyFields,
                        config.get(SINK_BATCH_SIZE),
                        config.get(SINK_FLUSH_INTERVAL_MS));

        return new DuckDBDynamicTableSink(writerOptions, rowType, keyFields, sinkParallelism);
    }

    private static void validateKeyFields(RowType rowType, List<String> keyFields) {
        List<String> columns = rowType.getFieldNames();
        Set<String> seen = new HashSet<>();
        for (String key : keyFields) {
            if (!columns.contains(key)) {
                throw new IllegalArgumentException(
                        "key-fields entry '" + key + "' not found in table schema: " + columns);
            }
            if (!seen.add(key)) {
                throw new IllegalArgumentException(
                        "key-fields contains '" + key + "' more than once: " + keyFields);
            }
        }
    }

    private static void validateParallelism(int sinkParallelism, String jdbcUrl) {
        if (sinkParallelism <= 1) {
            return;
        }
        // DuckDB locks a local file for exclusive single-process access, but that lock is
        // per-process, not per-connection -- two writers in the SAME JVM (the common case for
        // Flink subtasks sharing a TaskManager slot) can both open connections to the same file
        // without any lock conflict at all, racing on writes instead of failing loudly. A
        // MotherDuck URL (jdbc:duckdb:md:...) is a managed remote service that's actually safe
        // for concurrent writers, so only that's allowed above parallelism 1.
        if (!jdbcUrl.contains(":md:")) {
            throw new IllegalArgumentException(
                    "sink.parallelism = "
                            + sinkParallelism
                            + " is not supported against a local DuckDB file ('"
                            + jdbcUrl
                            + "'): DuckDB's file lock only blocks a second writer from a "
                            + "different OS process, not a second writer in the same JVM, so "
                            + "concurrent Flink subtasks sharing a TaskManager slot would race "
                            + "on writes instead of failing loudly. Use sink.parallelism = 1 "
                            + "(the default), or point database-path at a concurrency-safe "
                            + "remote target such as MotherDuck ('jdbc:duckdb:md:...').");
        }
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DATABASE_PATH);
        options.add(TABLE_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(KEY_FIELDS);
        options.add(SINK_BATCH_SIZE);
        options.add(SINK_FLUSH_INTERVAL_MS);
        options.add(SINK_PARALLELISM);
        return options;
    }
}
