package com.stratio.connector.oracle.statements;

import com.stratio.crossdata.common.metadata.IndexMetadata;

/**
 * Created by carlos on 23/12/14.
 */
public class DropIndexStatement {

    private String catalog;
    private String indexName;

    /**
     * Indicates if there is a catalog specified in the table name.
     */
    private boolean catalogInc;

    /**
     * Whether the keyspace should be removed only if exists.
     */
    private boolean ifExists;

    /**
     * Class constructor.
     *
     * @param index    The name of the index.
     * @param ifExists Whether it should be removed only if exists.
     */
    public DropIndexStatement(IndexMetadata index, boolean ifExists) {

        if (index.getColumns() != null && index.getColumns().size() != 0) {
            this.catalogInc = true;
            this.catalog = index.getName().getTableName().getCatalogName().getName();

        } else {
            String[] indexQ = index.getName().getQualifiedName().split("\\.");
            if (indexQ.length > 1) {
                this.catalog = indexQ[0];
                catalogInc = true;
            } else {
                catalogInc = false;
            }
        }
        this.indexName = index.getName().getName();

        this.ifExists = ifExists;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("DROP INDEX ");

        if (catalogInc) {
            sb.append(catalog).append(".").append(indexName);
        } else {
            sb.append(indexName);
        }

        return sb.toString();
    }
}
