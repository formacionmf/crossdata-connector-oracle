package com.stratio.connector.oracle.statements;

import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.IndexType;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Map;

/**
 * Created by carlos on 22/12/14.
 */
public class CreateIndexStatement {

    /**
     * The list of columns covered by the index. Only one column is allowed for {@code DEFAULT}
     * indexes.
     */
    private Map<ColumnName, ColumnMetadata> targetColumns = null;
    /**
     * Whether the index should be created only if not exists.
     */
    private boolean createIfNotExists = false;
    private IndexType type = null;
    private String keyspace = null;
    /**
     * The name of the target table.
     */
    private String tableName = null;
    private boolean keyspaceIncluded = false;
    /**
     * The name of the index.
     */
    private String name = null;
    /**
     * Basic Constructor.
     * @param indexMetadata  Index metadata information .
     * @param createIfNotExists Condition of creation of the index.
     * @param session Session that the Index affect.
     * @throws com.stratio.crossdata.common.exceptions.ExecutionException
     */
    public CreateIndexStatement(IndexMetadata indexMetadata, boolean createIfNotExists,
                                Connection session)
            throws ExecutionException {
        this.targetColumns = indexMetadata.getColumns();
        this.createIfNotExists = createIfNotExists;
        this.type = indexMetadata.getType();
        this.tableName = indexMetadata.getName().getTableName().getName();
        this.keyspace = indexMetadata.getName().getTableName().getCatalogName().getName();
        if (keyspace != null) {
            this.keyspaceIncluded = true;
        }
        this.name = indexMetadata.getName().getName();

    }


    /**
     * Get the name of the index. If a LUCENE index is to be created, the name of the index is
     * prepended with {@code stratio_lucene_}. If a name for the index is not specified, the index
     * will be named using the concatenation of the target column names.
     *
     * @return The name of the index.
     */
    private String getIndexName() {
        String result = null;
        if (name == null) {
            StringBuilder sb = new StringBuilder();
            if (IndexType.FULL_TEXT.equals(type)) {
                sb.append("stratio_fulltext");
                for (ColumnMetadata columnMetadata:targetColumns.values()){
                    sb.append("_");
                    sb.append(columnMetadata.getName().getName());
                }
                sb.append(tableName);
            } else {
                sb.append(tableName);

                for (Map.Entry<ColumnName, ColumnMetadata> entry : targetColumns.entrySet()) {
                    sb.append("_");
                    sb.append(entry.getValue());
                }

                sb.append("_idx");
            }
            result = sb.toString();
        } else {
            result = name;
            if (IndexType.FULL_TEXT.equals(type)) {
                result = name;
            }
        }
        return result;
    }

    /**
     * Get the query in a String in CQL language.
     * @return String with the query
     */
    public String toString() {
        StringBuilder sb = new StringBuilder("CREATE ");

        sb.append(" INDEX ");
//        if (createIfNotExists) {
//            sb.append("IF NOT EXISTS ");
//        }

        if (name != null) {
            sb.append(getIndexName()).append(" ");
        }
        sb.append("ON ");
        if (keyspaceIncluded) {
            sb.append(keyspace).append(".");
        }
        sb.append(tableName);
        sb.append(" (");
        int i = 0;
        for (Map.Entry<ColumnName, ColumnMetadata> entry : targetColumns.entrySet()) {
            if (i != 0) {
                sb.append(",");
            }
            sb.append(entry.getValue().getName().getName());
            i = 1;
        }
        sb.append(")");

        return sb.toString();
    }
}
