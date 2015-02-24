package com.stratio.connector.oracle.statements;

import com.stratio.connector.oracle.utils.ColumnInsertOracle;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.utils.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by carlos on 23/12/14.
 */
public class InsertIntoStatement {
    /**
     * The name of the target table.
     */
    private String tableName;

    /**
     * The list of columns to be assigned.
     */
    private List<String> ids;

    private Map<String, ColumnInsertOracle> cellValues;

    /**
     * Catalog.
     */
    private String catalog;

    /**
     * Indicates if there is a catalog specified in the table name.
     */
    private boolean catalogInc;

    /**
     * InsertIntoStatement general constructor.
     *
     * @param targetTable     Table target.
     * @param columnsMetadata List of {@link com.stratio.connector.oracle.utils.ColumnInsertOracle} to insert.
     */
    public InsertIntoStatement(TableMetadata targetTable,
                               Map<String, ColumnInsertOracle> columnsMetadata) {
        ids = new ArrayList<>();
        this.tableName = targetTable.getName().getQualifiedName();
        if (tableName.contains(".")) {
            String[] ksAndTableName = tableName.split("\\.");
            catalog = ksAndTableName[0];
            this.tableName = ksAndTableName[1];
            catalogInc = true;
        }

        for (String id : columnsMetadata.keySet()) {
            ids.add(id);
        }
        this.cellValues = columnsMetadata;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("INSERT INTO ");
        if (catalogInc) {
            sb.append(catalog).append(".");
        }
        sb.append(tableName).append(" (");
        sb.append(StringUtils.stringList(ids, ", ")).append(") ");

        sb.append("VALUES (");

        int cont = 0;
        for (String column : cellValues.keySet()) {
            String value = cellValues.get(column).getValue();
            ColumnType type = cellValues.get(column).getType();
            if (cont > 0) {
                sb.append(", ");
            }
            cont = 1;

            switch (type.getDataType()) {
                case TEXT:
                case VARCHAR:
                    sb.append("'" + value + "'");
                    break;
                default:
                    sb.append(value);
                    break;
            }
        }
        sb.append(")");

        return sb.toString();
    }
}
