package com.stratio.connector.oracle.statements;

import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.TableMetadata;

import java.util.*;

/**
 * Created by carlos on 18/12/14.
 */
public class CreateTableStatement {

    /**
     * The name of the target table.
     */
    private String tableName;
    /**
     * A map with the name of the columns in the table and the associated data type.
     */
    private Map<ColumnName, ColumnMetadata> tableColumns;
    /**
     * The list of columns that are part of the primary key.
     */
    private List<ColumnName> primaryKey;
    /**
     * The list of properties of the table.
     */
    private int defaultLength;

    /**
     * The list of properties of the table.
     */
    private Map<String, Object> properties;

    /**
     * Catalog.
     */
    private String catalog;

    private static final Map<String, String> columnConvert;
    static {
        Map<String, String> aMap = new HashMap<String,String>();
        aMap.put("text", "varchar2");
        aMap.put("bigint","number");
        aMap.put("double","binary_double");
        aMap.put("float","binary_float");
        aMap.put("boolean","char");
        columnConvert = Collections.unmodifiableMap(aMap);
    }

    /**
     * Class Constructor.
     * @param tableMetadata  The metadata of the table.
     * @param primaryKey The primary key of the table.
     * @param textOptions The specific properties of the table that will be created.
     * @param defaultLength Default length for Oracle varchar types
     * @throws com.stratio.crossdata.common.exceptions.ExecutionException
     */
    public CreateTableStatement(TableMetadata tableMetadata,
                                List<ColumnName> primaryKey,
                                Map<String, Object> textOptions,
                                int defaultLength) throws ExecutionException {
        this.tableName = tableMetadata.getName().getName();
        this.catalog = tableMetadata.getName().getCatalogName().getName();
        this.tableColumns = tableMetadata.getColumns();
        this.primaryKey = primaryKey;
        this.properties = textOptions;
        this.defaultLength = defaultLength;

    }


    /**
     * Obtain the composite primary key.
     * @return a string with the primary key.
     */
    public String getCompositePKString() {
        StringBuilder sb = new StringBuilder("PRIMARY KEY");
        sb.append("(");

        Iterator<ColumnName> pks = primaryKey.iterator();
        while (pks.hasNext()) {
            sb.append(pks.next().getName());
            if (pks.hasNext()) {
                sb.append(", ");
            }
        }
        sb.append(")");

        return sb.toString();
    }

    /**
     * Get the query of create table in Cassandra language.
     * @return th String with the query.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Create table ");

        sb.append(catalog).append(".");
        sb.append(tableName);

        Set<ColumnName> keySet = tableColumns.keySet();
        sb.append(" (");
        for (ColumnName key : keySet) {
            String vp = tableColumns.get(key).getColumnType().toString();
            sb.append(getCompositeColumnString(key.getName(),vp)).append(", ");
        }
        sb.append(getCompositePKString()).append(")");


        return sb.toString();
    }

    private String getCompositeColumnString(String columnName, String typeColumn){
        StringBuilder sb = new StringBuilder(columnName);
        sb.append(" ");
        if(columnConvert.get(typeColumn.toLowerCase())!=null) {
            String valColumnConvert = columnConvert.get(typeColumn.toLowerCase());
            sb.append(valColumnConvert);
            /* any conversion*/
            if (valColumnConvert.equals("varchar2")) {
                if (properties.containsKey(columnName)) {
                    sb.append("(").append(properties.get(columnName)).append(" char)");
                } else {
                    sb.append("(").append(defaultLength).append(" char)");
                }
            }

        }else {
            sb.append(typeColumn);
        }

        return sb.toString();
    }
}
