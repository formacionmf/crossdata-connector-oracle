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
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.Selector;

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
        switch (type.toUpperCase()) {
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
            case "INT":
            case "INTEGER":
                value=resultSet.getInt(columnName);
                break;
            case "BIGINT":
            case "NUMBER":
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
            case "SET":
            case "LIST":
            case "MAP":
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

        for (int i = 1; i <= numColumns; i++) {
            try {
                ColumnName columnName = new ColumnName(resultSetMetaData.getCatalogName(i),
                        resultSetMetaData.getTableName(i), resultSetMetaData.getColumnName(i));
                ColumnType type = helper.toColumnType(resultSetMetaData.getColumnTypeName(i));

                columnMetadata = new ColumnMetadata(columnName, null, type);
                columnList.add(columnMetadata);

            } catch (SQLException e) {
                LOG.error("Cannot transform result set", e);
                crossdataResult = new ResultSet();
            }
        }
        while (resultSet.next()) {
            crossdataRow = new com.stratio.crossdata.common.data.Row();
            for(ColumnMetadata column:columnList) {
                Cell crossdataCell = getCell(column.getColumnType().getCrossdataType(), resultSet,
                        column.getName().getName());
                crossdataRow.addCell(column.getName().getName(), crossdataCell);
            }
            crossdataResult.add(crossdataRow);
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
    public ResultSet transformToMetaResultSet(java.sql.ResultSet resultSet, Map<Selector, String> alias)
            throws SQLException {
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
//                ColumnName columnName = new ColumnName(resultSetMetaData.getCatalogName(i),
//                        resultSetMetaData.getTableName(i), resultSetMetaData.getColumnName(i));
                ColumnName columnName = new ColumnName("",
                        "", resultSetMetaData.getColumnName(i));
                ColumnType type = helper.toColumnType(resultSetMetaData.getColumnTypeName(i));
//                if (alias
//                        .containsKey(new ColumnSelector(new ColumnName(resultSetMetaData.getCatalogName(i),
//                                resultSetMetaData.getTableName(i),
//                                resultSetMetaData.getColumnName(i))))) {
                if (alias
                        .containsKey(new ColumnSelector(new ColumnName("",
                                "",
                                resultSetMetaData.getColumnName(i))))) {
                    columnMetadata = new ColumnMetadata(columnName, null, type);
                    columnMetadata.getName()
                            .setAlias(alias.get(new ColumnSelector(new ColumnName("",
                                    "",
                                    resultSetMetaData.getColumnName(i)))));
//                    columnMetadata.getName()
//                            .setAlias(alias.get(new ColumnSelector(new ColumnName(resultSetMetaData.getCatalogName(i),
//                                    resultSetMetaData.getTableName(i),
//                                    resultSetMetaData.getColumnName(i)))));
                } else {
                    columnMetadata = new ColumnMetadata(columnName, null, type);
                }
                columnList.add(columnMetadata);
            } catch (SQLException e) {
                LOG.error("Cannot transform result set", e);
                crossdataResult = new ResultSet();
            }
        }

        while (resultSet.next()) {
            crossdataRow = new com.stratio.crossdata.common.data.Row();
            for(ColumnMetadata column:columnList) {


//                if (alias
//                        .containsKey(new ColumnSelector(new ColumnName(column.getName().getTableName().getCatalogName
//                                ().getName(), column.getName().getTableName().getName(),
//                                column.getName().getName())))) {
                if (alias
                        .containsKey(new ColumnSelector(new ColumnName("", "",
                                column.getName().getName())))) {
                    Cell crossdataCell = getCell(column.getColumnType().getCrossdataType(), resultSet,
                            column.getName().getName());
                    crossdataRow.addCell(alias
                            .get(new ColumnSelector(new ColumnName("", "",
                                    column.getName().getName()))), crossdataCell);
//                    crossdataRow.addCell(alias
//                            .get(new ColumnSelector(new ColumnName(column.getName().getTableName().getCatalogName
//                                    ().getName(), column.getName().getTableName().getName(),
//                                    column.getName().getName()))), crossdataCell);
                } else {
                    Cell crossdataCell = getCell(column.getColumnType().getCrossdataType(), resultSet,
                            column.getName().getName());
                    crossdataRow.addCell(column.getName().getName(), crossdataCell);
                }
            }
            crossdataResult.add(crossdataRow);
        }
        crossdataResult.setColumnMetadata(columnList);

        return crossdataResult;
    }
}
