package io.github.ashfaqbs.flink.connector.duckdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.junit.jupiter.api.Test;

class DuckDBDynamicTableFactoryTest {

    private static DynamicTableFactory.Context contextWith(Map<String, String> options) {
        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical("id", DataTypes.BIGINT()),
                        Column.physical("name", DataTypes.STRING()));
        CatalogTable catalogTable =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(schema).build(),
                        null,
                        Collections.emptyList(),
                        options);
        return new FactoryUtil.DefaultDynamicTableContext(
                ObjectIdentifier.of("cat", "db", "t"),
                new ResolvedCatalogTable(catalogTable, schema),
                Collections.emptyMap(),
                new Configuration(),
                Thread.currentThread().getContextClassLoader(),
                false);
    }

    private static Map<String, String> baseOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "duckdb");
        options.put("database-path", "C:/tmp/whatever.db");
        options.put("table-name", "t");
        return options;
    }

    @Test
    void plainDatabasePathIsAccepted() {
        DuckDBDynamicTableFactory factory = new DuckDBDynamicTableFactory();
        DynamicTableSink sink = factory.createDynamicTableSink(contextWith(baseOptions()));
        assertThat(sink.asSummaryString()).isEqualTo("DuckDB");
    }

    @Test
    void rejectsParallelismAboveOneAgainstLocalFile() {
        Map<String, String> options = baseOptions();
        options.put("sink.parallelism", "4");
        DuckDBDynamicTableFactory factory = new DuckDBDynamicTableFactory();

        assertThatThrownBy(() -> factory.createDynamicTableSink(contextWith(options)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sink.parallelism")
                .hasMessageContaining("MotherDuck");
    }

    @Test
    void allowsParallelismAboveOneAgainstMotherDuckUrl() {
        Map<String, String> options = baseOptions();
        options.put("database-path", "jdbc:duckdb:md:mydb");
        options.put("sink.parallelism", "4");
        DuckDBDynamicTableFactory factory = new DuckDBDynamicTableFactory();

        DynamicTableSink sink = factory.createDynamicTableSink(contextWith(options));
        assertThat(sink.asSummaryString()).isEqualTo("DuckDB");
    }

    @Test
    void rejectsDuplicateKeyFields() {
        Map<String, String> options = baseOptions();
        options.put("key-fields", "id,id");
        DuckDBDynamicTableFactory factory = new DuckDBDynamicTableFactory();

        assertThatThrownBy(() -> factory.createDynamicTableSink(contextWith(options)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("more than once");
    }

    @Test
    void rejectsUnknownKeyField() {
        Map<String, String> options = baseOptions();
        options.put("key-fields", "does_not_exist");
        DuckDBDynamicTableFactory factory = new DuckDBDynamicTableFactory();

        assertThatThrownBy(() -> factory.createDynamicTableSink(contextWith(options)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does_not_exist");
    }
}
