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

        DuckDBWriterOptions writerOptions =
                new DuckDBWriterOptions(
                        jdbcUrl,
                        config.get(TABLE_NAME),
                        keyFields,
                        config.get(SINK_BATCH_SIZE),
                        config.get(SINK_FLUSH_INTERVAL_MS));

        return new DuckDBDynamicTableSink(
                writerOptions, rowType, keyFields, config.get(SINK_PARALLELISM));
    }

    private static void validateKeyFields(RowType rowType, List<String> keyFields) {
        List<String> columns = rowType.getFieldNames();
        for (String key : keyFields) {
            if (!columns.contains(key)) {
                throw new IllegalArgumentException(
                        "key-fields entry '" + key + "' not found in table schema: " + columns);
            }
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
