package io.github.ashfaqbs.flink.connector.duckdb;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

/** Binds a Flink {@link RowData}'s fields onto the {@code ?} placeholders of a {@link
 * PreparedStatement}, in column order. */
public final class DuckDBRowConverter implements Serializable {

    private static final long serialVersionUID = 1L;

    private final LogicalType[] fieldTypes;

    public DuckDBRowConverter(RowType rowType) {
        int arity = rowType.getFieldCount();
        this.fieldTypes = new LogicalType[arity];
        for (int i = 0; i < arity; i++) {
            LogicalType type = rowType.getTypeAt(i);
            if (!isSupported(type.getTypeRoot())) {
                throw new IllegalArgumentException(
                        "Unsupported column type "
                                + type
                                + " at position "
                                + i
                                + ". Supported types: BOOLEAN, TINYINT, SMALLINT, INTEGER, "
                                + "BIGINT, FLOAT, DOUBLE, DECIMAL, CHAR, VARCHAR, DATE, "
                                + "TIMESTAMP (without zone). Cast or exclude this column.");
            }
            fieldTypes[i] = type;
        }
    }

    private static boolean isSupported(LogicalTypeRoot root) {
        switch (root) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case CHAR:
            case VARCHAR:
            case DATE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return true;
            default:
                return false;
        }
    }

    public void bind(PreparedStatement statement, RowData row) throws SQLException {
        for (int i = 0; i < fieldTypes.length; i++) {
            int jdbcIndex = i + 1;
            if (row.isNullAt(i)) {
                statement.setNull(jdbcIndex, sqlTypeOf(fieldTypes[i].getTypeRoot()));
                continue;
            }
            switch (fieldTypes[i].getTypeRoot()) {
                case BOOLEAN:
                    statement.setBoolean(jdbcIndex, row.getBoolean(i));
                    break;
                case TINYINT:
                    statement.setByte(jdbcIndex, row.getByte(i));
                    break;
                case SMALLINT:
                    statement.setShort(jdbcIndex, row.getShort(i));
                    break;
                case INTEGER:
                    statement.setInt(jdbcIndex, row.getInt(i));
                    break;
                case BIGINT:
                    statement.setLong(jdbcIndex, row.getLong(i));
                    break;
                case FLOAT:
                    statement.setFloat(jdbcIndex, row.getFloat(i));
                    break;
                case DOUBLE:
                    statement.setDouble(jdbcIndex, row.getDouble(i));
                    break;
                case DECIMAL:
                    int precision = ((org.apache.flink.table.types.logical.DecimalType) fieldTypes[i]).getPrecision();
                    int scale = ((org.apache.flink.table.types.logical.DecimalType) fieldTypes[i]).getScale();
                    BigDecimal decimal = row.getDecimal(i, precision, scale).toBigDecimal();
                    statement.setBigDecimal(jdbcIndex, decimal);
                    break;
                case CHAR:
                case VARCHAR:
                    statement.setString(jdbcIndex, row.getString(i).toString());
                    break;
                case DATE:
                    statement.setDate(jdbcIndex, java.sql.Date.valueOf(
                            java.time.LocalDate.ofEpochDay(row.getInt(i))));
                    break;
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    statement.setTimestamp(
                            jdbcIndex, Timestamp.valueOf(row.getTimestamp(i, 6).toLocalDateTime()));
                    break;
                default:
                    // Unreachable: the constructor rejects unsupported types up front.
                    throw new IllegalStateException("Unsupported type: " + fieldTypes[i]);
            }
        }
    }

    private static int sqlTypeOf(LogicalTypeRoot root) {
        switch (root) {
            case BOOLEAN:
                return Types.BOOLEAN;
            case TINYINT:
                return Types.TINYINT;
            case SMALLINT:
                return Types.SMALLINT;
            case INTEGER:
                return Types.INTEGER;
            case BIGINT:
                return Types.BIGINT;
            case FLOAT:
                return Types.FLOAT;
            case DOUBLE:
                return Types.DOUBLE;
            case DECIMAL:
                return Types.DECIMAL;
            case CHAR:
            case VARCHAR:
                return Types.VARCHAR;
            case DATE:
                return Types.DATE;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return Types.TIMESTAMP;
            default:
                return Types.NULL;
        }
    }
}
