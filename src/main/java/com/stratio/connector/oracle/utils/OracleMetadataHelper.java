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
    private static Map<ColumnType, String> dbType = new HashMap<>();
    private static Map<ColumnType, Class<?>> dbClass = new HashMap<>();
    private static Map<String,ColumnType> dbType2 = new HashMap<>();

    static {

        dbClass.put(ColumnType.BIGINT, Long.class);
        dbClass.put(ColumnType.BOOLEAN, Boolean.class);
        dbClass.put(ColumnType.DOUBLE, Double.class);
        dbClass.put(ColumnType.FLOAT, Float.class);
        dbClass.put(ColumnType.INT, Integer.class);
        dbClass.put(ColumnType.TEXT, String.class);
        dbClass.put(ColumnType.VARCHAR, String.class);

        dbType.put(ColumnType.BIGINT, "BIGINT");
        dbType.put(ColumnType.BOOLEAN, "BOOLEAN");
        dbType.put(ColumnType.DOUBLE, "DOUBLE");
        dbType.put(ColumnType.FLOAT, "FLOAT");
        dbType.put(ColumnType.INT, "INT");
        dbType.put(ColumnType.TEXT, "TEXT");
        dbType.put(ColumnType.VARCHAR, "VARCHAR");


        dbType2.put("BIGINT", ColumnType.BIGINT);
        dbType2.put("SQL_BIGINT", ColumnType.BIGINT);
        dbType2.put("BOOLEAN", ColumnType.BOOLEAN);
        dbType2.put("DOUBLE", ColumnType.DOUBLE);
        dbType2.put("SQL_DOUBLE", ColumnType.DOUBLE);
        dbType2.put("FLOAT", ColumnType.FLOAT);
        dbType2.put("SQL_FLOAT", ColumnType.FLOAT);
        dbType2.put("INT", ColumnType.INT);
        dbType2.put("SQL_INTEGER", ColumnType.INT);
        dbType2.put("TEXT", ColumnType.TEXT);
        dbType2.put("VARCHAR", ColumnType.VARCHAR);
        dbType2.put("SQL_VARCHAR", ColumnType.VARCHAR);
        dbType2.put("NUMBER", ColumnType.DOUBLE);
        dbType2.put("VARCHAR2", ColumnType.VARCHAR);
        dbType2.put("BINARY_DOUBLE", ColumnType.DOUBLE);
        dbType2.put("BINARY_FLOAT", ColumnType.FLOAT);
        dbType2.put("CHAR", ColumnType.VARCHAR);

    }

    /**
     * Mapping between Cassandra datatypes and META datatypes.
     */
    private static Map<String, ColumnType> typeMapping = new HashMap<>();

    /**
     * Class constructor.
     */
    public OracleMetadataHelper() {
        for (Map.Entry<String,ColumnType> entry : dbType2.entrySet()) {
            typeMapping.put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Transform a Cassandra type to Crossdata type.
     *
     * @param dbTypeName The Cassandra column type.
     * @return The Crossdata ColumnType.
     */
    public ColumnType toColumnType(String dbTypeName) {
        ColumnType result = typeMapping.get(dbTypeName.toUpperCase());
        if (result == null) {
            try {
                String jdbcType = (dbTypeName.toUpperCase());
                result = ColumnType.NATIVE;
                result.setDBMapping(jdbcType, getJavaClass(jdbcType));
                result.setODBCType(nativeODBCType.get(jdbcType));
            } catch (IllegalArgumentException iae) {
                LOG.error("Invalid database type: " + dbTypeName, iae);
                result = null;
            }
        } else {
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
