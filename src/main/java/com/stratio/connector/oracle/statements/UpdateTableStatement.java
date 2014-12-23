package com.stratio.connector.oracle.statements;

import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.statements.structures.Relation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by carlos on 23/12/14.
 */
public class UpdateTableStatement {

    /**
     * The name of the table.
     */
    private TableName tableName;

    /**
     * The list of assignations.
     */
    private List<Relation> assignations = new ArrayList<>();

    /**
     * The list of relations.
     */
    private List<Filter> whereClauses = new ArrayList<>();

    /**
     * Class constructor.
     *
     * @param tableName    The name of the table.
     * @param assignations The list of assignations.
     * @param whereClauses The list of relations.
     */
    public UpdateTableStatement(TableName tableName, Collection<Relation> assignations,
                                Collection<Filter> whereClauses) {

        for (Filter filter : whereClauses) {
            this.whereClauses.add(filter);
        }

        for (Relation relation : assignations) {
            this.assignations.add(relation);
        }

        this.tableName = tableName;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("UPDATE ");
        sb.append(tableName.getQualifiedName());

        sb.append(" ").append("SET ");
        for (Relation relation : assignations) {
            String leftTerm = getLeftTerm(relation);
            sb.append(leftTerm).append(relation.getOperator().toString()).append
                    (relation.getRightTerm().toString()).append(", ");
        }

        sb.delete(sb.lastIndexOf(", "), sb.length());

        sb.append(" WHERE ");
        if ((whereClauses != null) && (!whereClauses.isEmpty())) {
            for (Filter filter : whereClauses) {
                Relation relation = filter.getRelation();
                String leftTerm = getLeftTerm(relation);
                sb.append(leftTerm).append(relation.getOperator().toString()).append
                        (relation.getRightTerm().toString()).append(" AND ");
            }
            sb.delete(sb.lastIndexOf(" AND"), sb.length());
        }

        return sb.toString();
    }

    private String getLeftTerm(Relation relation){
        String leftTerm = relation.getLeftTerm().getStringValue().substring(relation.getLeftTerm()
                .getStringValue().lastIndexOf('.') + 1, relation.getLeftTerm().getStringValue().length());

        return leftTerm;
    }

    public TableName getTableName() {
        return tableName;
    }

    public void setTableName(TableName tableName) {
        this.tableName = tableName;
    }

}
