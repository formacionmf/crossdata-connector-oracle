package com.stratio.connector.oracle.utils;

import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.ResultSet;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import org.apache.log4j.Logger;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by carlos on 23/12/14.
 */
public class Utils {
    /**
     * Class logger.
     */
    private static final Logger LOG = Logger.getLogger(Utils.class);

    /**
     * Get a {@link com.stratio.crossdata.common.data.Cell} with the column contents of a Row.
     *
     * @param type       The type of the column.
     * @param resultSet  The row that contains the column.
     * @param columnName The column name.
     * @return A {@link com.stratio.crossdata.common.data.Cell} with the contents.
     * @throws java.lang.reflect.InvocationTargetException If the required method cannot be invoked.
     * @throws IllegalAccessException                      If the method cannot be accessed.
     */
    protected Cell getCell(String type, java.sql.ResultSet resultSet, String columnName) throws SQLException {
        Object value;
        switch (type) {
            case "CHAR":
            case "VARCHAR":
            case "LONGVARCHAR":
                value=resultSet.getString(columnName);
                break;
            case "NUMERIC":
            case "DECIMAL":
                value=resultSet.getDouble(columnName);
                break;
            case "BIT":
            case "BOOLEAN":
                value=resultSet.getBoolean(columnName);
                break;
            case "TINYINT":
                value=resultSet.getByte(columnName);
                break;
            case "SMALLINT":
                value=resultSet.getShort(columnName);
                break;
            case "INTEGER":
                value=resultSet.getInt(columnName);
                break;
            case "NUMBER":
                value=resultSet.getLong(columnName);
                break;
            case "BIGINT":
                value=resultSet.getLong(columnName);
                break;
            case "REAL":
                value=resultSet.getFloat(columnName);
                break;
            case "FLOAT":
            case "DOUBLE":
                value=resultSet.getDouble(columnName);
                break;
            case "BINARY":
            case "VARBINARY":
            case "LONGVARBINARY":
                value=resultSet.getBytes(columnName);
                break;
            case "DATE":
                value=resultSet.getDate(columnName);
                break;
            case "TIME":
                value=resultSet.getTime(columnName);
                break;
            case "TIMESTAMP":
                value=resultSet.getTimestamp(columnName);
                break;
            case "CLOB":
                value=resultSet.getClob(columnName);
                break;
            case "BLOB":
                value=resultSet.getBlob(columnName);
                break;
            case "ARRAY":
                value=resultSet.getArray(columnName);
                break;
            case "DISTINCT":
            case "STRUCT":
            case "REF":
            case "DATALINK":
            case "JAVA_OBJECT":
            default:
                value=null;
        }
        return new Cell(value);
    }

    /**
     * Transforms a JDBC resultset into a
     * com.stratio.crossdata.common.data.ResultSet}.
     *
     * @param resultSet The input Cassandra result set.
     * @return An equivalent Meta ResultSet
     */
    public ResultSet transformToMetaResultSet(java.sql.ResultSet resultSet) throws SQLException {
        ResultSet crossdataResult = new ResultSet();

        OracleMetadataHelper helper = new OracleMetadataHelper();

        //Get the columns in order
        ResultSetMetaData resultSetMetaData = null;
        int numColumns = 0;
        try {
            resultSetMetaData = resultSet.getMetaData();
            numColumns = resultSetMetaData.getColumnCount();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        List<ColumnMetadata> columnList = new ArrayList<>();
        ColumnMetadata columnMetadata = null;
        com.stratio.crossdata.common.data.Row crossdataRow = null;
        for (int i = 0; i < numColumns; i++) {
            try {
                ColumnName columnName = new ColumnName(resultSetMetaData.getCatalogName(i),
                        resultSetMetaData.getTableName(i), resultSetMetaData.getColumnName(i));
                ColumnType type = helper.toColumnType(resultSetMetaData.getColumnTypeName(i));
                resultSetMetaData.getColumnType(i);
                columnMetadata = new ColumnMetadata(columnName, null, type);
                columnList.add(columnMetadata);

                crossdataRow = new com.stratio.crossdata.common.data.Row();
                Cell crossdataCell = getCell(resultSetMetaData.getColumnTypeName(i), resultSet,
                        resultSetMetaData.getColumnName(i));
                crossdataRow.addCell(resultSetMetaData.getColumnName(i), crossdataCell);
                crossdataResult.add(crossdataRow);
            } catch (SQLException e) {
                LOG.error("Cannot transform result set", e);
                crossdataResult = new ResultSet();
            }
        }

        crossdataResult.setColumnMetadata(columnList);


        return crossdataResult;
    }

    /**
     * Transforms a JDBC ResultSet into a {@link
     * com.stratio.crossdata.common.data.ResultSet}.
     *
     * @param resultSet The input Cassandra result set.
     * @param alias     The map with the relations between ColumnName and Alias.
     * @return An equivalent Meta ResultSet.
     */
    public ResultSet transformToMetaResultSet(java.sql.ResultSet resultSet, Map<ColumnName, String> alias) {
        ResultSet crossdataResult = new ResultSet();

        OracleMetadataHelper helper = new OracleMetadataHelper();

        //Get the columns in order
        ResultSetMetaData resultSetMetaData = null;
        int numColumns = 0;
        try {
            resultSetMetaData = resultSet.getMetaData();
            numColumns = resultSetMetaData.getColumnCount();
        } catch (SQLException e) {
            e.printStackTrace();
        }


        List<ColumnMetadata> columnList = new ArrayList<>();
        ColumnMetadata columnMetadata = null;
        com.stratio.crossdata.common.data.Row crossdataRow = null;
        for (int i = 1; i <= numColumns; i++) {
            try {
                ColumnName columnName = new ColumnName(resultSetMetaData.getCatalogName(i),
                        resultSetMetaData.getTableName(i), resultSetMetaData.getColumnName(i));
                ColumnType type = helper.toColumnType(resultSetMetaData.getColumnTypeName(i));
                if (alias
                        .containsKey(new ColumnName(resultSetMetaData.getCatalogName(i),
                                resultSetMetaData.getTableName(i),
                                resultSetMetaData.getColumnName(i)))) {
                    columnMetadata = new ColumnMetadata(columnName, null, type);
                    columnMetadata.getName()
                            .setAlias(alias.get(new ColumnName(resultSetMetaData.getCatalogName(i),
                                    resultSetMetaData.getTableName(i),
                                    resultSetMetaData.getColumnName(i))));
                } else {
                    columnMetadata = new ColumnMetadata(columnName, null, type);
                }
                columnList.add(columnMetadata);

                crossdataRow = new com.stratio.crossdata.common.data.Row();
                if (alias
                        .containsKey(new ColumnName(resultSetMetaData.getCatalogName(i),
                                resultSetMetaData.getTableName(i),
                                resultSetMetaData.getColumnName(i)))) {
                    Cell crossdataCell = getCell(resultSetMetaData.getColumnTypeName(i), resultSet,
                            resultSetMetaData.getColumnName(i));
                    crossdataRow.addCell(alias
                            .get(new ColumnName(resultSetMetaData.getCatalogName(i),
                                    resultSetMetaData.getTableName(i),
                                    resultSetMetaData.getColumnName(i))), crossdataCell);
                } else {
                    Cell crossdataCell = getCell(resultSetMetaData.getColumnTypeName(i), resultSet,
                            resultSetMetaData.getColumnName(i));
                    crossdataRow.addCell(resultSetMetaData.getColumnName(i), crossdataCell);
                }

                crossdataResult.add(crossdataRow);
            } catch (SQLException e) {
                LOG.error("Cannot transform result set", e);
                crossdataResult = new ResultSet();
            }
        }
        crossdataResult.setColumnMetadata(columnList);

        return crossdataResult;
    }
}
