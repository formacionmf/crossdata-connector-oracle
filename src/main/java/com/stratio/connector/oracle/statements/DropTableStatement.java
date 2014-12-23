package com.stratio.connector.oracle.statements;

/**
 * Created by carlos on 22/12/14.
 */
public class DropTableStatement {

    /**
     * Catalog.
     */
    private String catalog;
    /**
     * The name of the target table.
     */
    private String tableName;
    /**
     * Indicates if there is a catalog specified in the table name.
     */
    private boolean catalogInc;

    /**
     * Class constructor.
     *
     * @param tableName The name of the table.
     */
    public DropTableStatement(String tableName) {
        if (tableName.contains(".")) {
            String[] ksAndTableName = tableName.split("\\.");
            catalog = ksAndTableName[0];
            this.tableName = ksAndTableName[1];
            catalogInc = true;
        } else {
            this.tableName = tableName;
        }
    }

    /**
     * Obtain the query in CQL language.
     * @return String with the query.
     */
    public String toString() {
        StringBuilder sb = new StringBuilder("DROP TABLE ");
        if (catalogInc) {
            sb.append(catalog).append(".");
        }
        sb.append(tableName);
        return sb.toString();
    }
}
