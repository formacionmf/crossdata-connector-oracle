package com.stratio.connector.oracle.utils;

import com.stratio.crossdata.common.metadata.ColumnType;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by carlos on 23/12/14.
 */
public class OracleMetadataHelper {
    /**
     * Class logger.
     */
    private static final Logger LOG = Logger.getLogger(OracleMetadataHelper.class.getName());
    /**
     * Mapping of native datatypes to SQL types.
     */
    private static Map<String, String> nativeODBCType = new HashMap<>();
    private static Map<com.stratio.crossdata.common.metadata.DataType, String> dbType = new HashMap<>();
    private static Map<com.stratio.crossdata.common.metadata.DataType, Class<?>> dbClass = new HashMap<>();
    private static Map<String,com.stratio.crossdata.common.metadata.DataType> dbType2 = new HashMap<>();

    static {

        dbClass.put(com.stratio.crossdata.common.metadata.DataType.BIGINT, Long.class);
        dbClass.put(com.stratio.crossdata.common.metadata.DataType.BOOLEAN, Boolean.class);
        dbClass.put(com.stratio.crossdata.common.metadata.DataType.DOUBLE, Double.class);
        dbClass.put(com.stratio.crossdata.common.metadata.DataType.FLOAT, Float.class);
        dbClass.put(com.stratio.crossdata.common.metadata.DataType.INT, Integer.class);
        dbClass.put(com.stratio.crossdata.common.metadata.DataType.TEXT, String.class);
        dbClass.put(com.stratio.crossdata.common.metadata.DataType.VARCHAR, String.class);

        dbType.put(com.stratio.crossdata.common.metadata.DataType.BIGINT, "BIGINT");
        dbType.put(com.stratio.crossdata.common.metadata.DataType.BOOLEAN, "BOOLEAN");
        dbType.put(com.stratio.crossdata.common.metadata.DataType.DOUBLE, "DOUBLE");
        dbType.put(com.stratio.crossdata.common.metadata.DataType.FLOAT, "FLOAT");
        dbType.put(com.stratio.crossdata.common.metadata.DataType.INT, "INT");
        dbType.put(com.stratio.crossdata.common.metadata.DataType.TEXT, "TEXT");
        dbType.put(com.stratio.crossdata.common.metadata.DataType.VARCHAR, "VARCHAR");


        dbType2.put("BIGINT", com.stratio.crossdata.common.metadata.DataType.BIGINT);
        dbType2.put("SQL_BIGINT", com.stratio.crossdata.common.metadata.DataType.BIGINT);
        dbType2.put("BOOLEAN", com.stratio.crossdata.common.metadata.DataType.BOOLEAN);
        dbType2.put("DOUBLE", com.stratio.crossdata.common.metadata.DataType.DOUBLE);
        dbType2.put("SQL_DOUBLE", com.stratio.crossdata.common.metadata.DataType.DOUBLE);
        dbType2.put("FLOAT", com.stratio.crossdata.common.metadata.DataType.FLOAT);
        dbType2.put("SQL_FLOAT", com.stratio.crossdata.common.metadata.DataType.FLOAT);
        dbType2.put("INT", com.stratio.crossdata.common.metadata.DataType.INT);
        dbType2.put("SQL_INTEGER", com.stratio.crossdata.common.metadata.DataType.INT);
        dbType2.put("TEXT", com.stratio.crossdata.common.metadata.DataType.TEXT);
        dbType2.put("VARCHAR", com.stratio.crossdata.common.metadata.DataType.VARCHAR);
        dbType2.put("SQL_VARCHAR", com.stratio.crossdata.common.metadata.DataType.VARCHAR);
        dbType2.put("NUMBER", com.stratio.crossdata.common.metadata.DataType.DOUBLE);
        dbType2.put("VARCHAR2", com.stratio.crossdata.common.metadata.DataType.VARCHAR);
        dbType2.put("BINARY_DOUBLE", com.stratio.crossdata.common.metadata.DataType.DOUBLE);
        dbType2.put("BINARY_FLOAT", com.stratio.crossdata.common.metadata.DataType.FLOAT);
        dbType2.put("CHAR", com.stratio.crossdata.common.metadata.DataType.VARCHAR);

    }

    /**
     * Mapping between Cassandra datatypes and META datatypes.
     */
    private static Map<String, com.stratio.crossdata.common.metadata.DataType> typeMapping = new HashMap<>();

    /**
     * Class constructor.
     */
    public OracleMetadataHelper() {
        for (Map.Entry<String,com.stratio.crossdata.common.metadata.DataType> entry : dbType2.entrySet()) {
            typeMapping.put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Transform a Oracle type to Crossdata type.
     *
     * @param dbTypeName The Cassandra column type.
     * @return The Crossdata ColumnType.
     */
    public ColumnType toColumnType(String dbTypeName) {
        ColumnType result;;
        if (typeMapping.get(dbTypeName.toUpperCase()) == null) {
            try {
                String jdbcType = (dbTypeName.toUpperCase());
                result = new ColumnType(com.stratio.crossdata.common.metadata.DataType.NATIVE);
                result.setDBMapping(jdbcType, getJavaClass(jdbcType));
                result.setODBCType(nativeODBCType.get(jdbcType));
            } catch (IllegalArgumentException iae) {
                LOG.error("Invalid database type: " + dbTypeName, iae);
                result = null;
            }
        } else {
            result = new ColumnType(typeMapping.get(dbTypeName.toUpperCase()));
            result.setDBMapping(dbType.get(result), dbClass.get(result));
        }

        return result;
    }

    private Class<?> getJavaClass(String type) {
        switch (type) {
            case "CHAR":
            case "VARCHAR":
                return String.class;
            case "NUMERIC":
            case "DECIMAL":
                return BigDecimal.class;
            case "BIT":
            case "BOOLEAN":
                return Boolean.class;
            case "TINYINT":
                return Byte.class;
            case "SMALLINT":
                return Short.class;
            case "INTEGER":
                return Integer.class;
            case "BIGINT":
                return Long.class;
            case "REAL":
                return Float.class;
            case "FLOAT":
            case "DOUBLE":
                return Double.class;
            case "BINARY":
            case "VARBINARY":
            case "LONGVARBINARY":
                return Byte[].class;
            case "DATE":
                return Date.class;
            case "TIME":
                return Time.class;
            case "TIMESTAMP":
                return Timestamp.class;
            case "CLOB":
                return Clob.class;
            case "BLOB":
                return Blob.class;
            case "ARRAY":
                return Array.class;
            case "DISTINCT":
            case "STRUCT":
            case "REF":
            case "DATALINK":
            case "JAVA_OBJECT":
            default:
                return Object.class;
        }
    }
}
