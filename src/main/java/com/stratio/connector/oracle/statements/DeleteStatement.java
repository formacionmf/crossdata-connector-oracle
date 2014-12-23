package com.stratio.connector.oracle.statements;

import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.statements.structures.Relation;

import java.util.List;

/**
 * Created by carlos on 23/12/14.
 */
public class DeleteStatement {
    private String catalog;
    private boolean catalogInc = false;
    /**
     * The name of the targe table.
     */
    private TableName tableName = null;

    /**
     * The list of {@link com.stratio.crossdata.common.statements.structures.Relation} found
     * in the WHERE clause.
     */
    private List<Filter> whereClauses;

    /**
     * Constructor Class.
     * @param tableName The table Name
     * @param whereClauses A list with the conditions
     */
    public DeleteStatement(TableName tableName, List<Filter> whereClauses) {
        this.tableName = tableName;
        if (tableName.isCompletedName()) {
            catalogInc = true;
            catalog = tableName.getCatalogName().getName();
        }
        this.whereClauses = whereClauses;
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("DELETE FROM ");
        if (catalogInc) {
            sb.append(catalog).append(".");
        }
        sb.append(tableName.getName());
        if (!whereClauses.isEmpty()) {
            sb.append(" WHERE ");
            for (Filter filter : whereClauses) {
                Relation relation = filter.getRelation();
                String leftTerm = relation.getLeftTerm().getStringValue().substring(relation.getLeftTerm()
                        .getStringValue().lastIndexOf('.') + 1, relation.getLeftTerm().getStringValue().length());
                sb.append(leftTerm).append(relation.getOperator().toString()).append
                        (relation.getRightTerm().toString()).append(" AND ");
            }
            sb.delete(sb.lastIndexOf(" AND"), sb.length());

        }
        return sb.toString();
    }

    public TableName getTableName() {
        return tableName;
    }

    /**
     * Set the name of the table.
     *
     * @param tableName The name of the table.
     */
    public void setTableName(TableName tableName) {
        this.tableName = tableName;
    }

}
