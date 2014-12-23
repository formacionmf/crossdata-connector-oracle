package com.stratio.connector.oracle.statements;

import com.stratio.crossdata.common.data.CatalogName;
import com.stratio.crossdata.common.data.TableName;

/**
 * Created by carlos on 23/12/14.
 */
public class TruncateStatement {

    /**
     * The name of the table.
     */
    private TableName tableName;
    private boolean catalogInc;
    private String catalog;

    /**
     * Class Constructor.
     *
     * @param tableName The table name of the truncate statement.
     */
    public TruncateStatement(TableName tableName) {

        this.tableName = tableName;
        this.catalogInc= tableName.isCompletedName();
        this.catalog= tableName.getCatalogName().getName();
    }

    /**
     * Get the table of the truncate statement.
     *
     * @return com.stratio.crossdata.common.data.TableName
     */
    public TableName getTableName() {
        return tableName;
    }


    /**
     * Get the catalog of the truncate statement.
     *
     * @return com.stratio.crossdata.common.data.CatalogName
     */
    public CatalogName getCatalog() {
        return tableName.getCatalogName();
    }



    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("TRUNCATE ");
        if (catalogInc) {
            sb.append(catalog).append(".");
        }
        sb.append(tableName.getName());
        return sb.toString();
    }

}
