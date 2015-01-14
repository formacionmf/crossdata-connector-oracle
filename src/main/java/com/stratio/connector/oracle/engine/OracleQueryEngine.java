/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2014 Stratio
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.stratio.connector.oracle.engine;

import com.stratio.connector.oracle.OracleExecutor;
import com.stratio.crossdata.common.connector.IQueryEngine;
import com.stratio.crossdata.common.connector.IResultHandler;
import com.stratio.crossdata.common.data.CatalogName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.*;
import com.stratio.crossdata.common.result.QueryResult;
import com.stratio.crossdata.common.result.Result;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.OrderByClause;
import com.stratio.crossdata.common.statements.structures.Relation;
import com.stratio.crossdata.common.statements.structures.Selector;

import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Oracle query engine
 */
public class OracleQueryEngine implements IQueryEngine{
    private static final int DEFAULT_LIMIT = 100;
    private Map<Selector, String> aliasColumns = new HashMap<>();
    private List<OrderByClause> orderByColumns = new ArrayList<>();
    private Statement session = null;
    private List<ColumnName> selectionClause;
    private boolean catalogInc;
    private String catalog;
    private TableName tableName;
    private boolean whereInc = false;
    private boolean limitInc = true;
    private List<Relation> where = new ArrayList<>();
    private int limit = DEFAULT_LIMIT;
    private Map<String, Statement> sessions;

    /**
     * Basic constructor.
     * @param sessions Map of sessions.
     * @param limitDefault Default limit for a query.
     */
    public OracleQueryEngine(Map<String, Statement> sessions, int limitDefault) {
        this.sessions = sessions;
        this.limit = limitDefault;
    }

    @Override
    public QueryResult execute(LogicalWorkflow workflow) throws ConnectorException {
        LogicalStep logicalStep = workflow.getInitialSteps().get(0);
        while (logicalStep != null) {
            if (logicalStep instanceof TransformationStep) {
                getTransformationStep(logicalStep);
            }
            logicalStep = logicalStep.getNextStep();
        }

        String query = parseQuery();

        Result result;
        if (session != null) {
            if (aliasColumns.isEmpty()) {
                result = OracleExecutor.execute(query, session);
            } else {
                result = OracleExecutor.execute(query, aliasColumns, session);
            }
        } else {
            throw new ExecutionException("No session to cluster established");
        }
        return (QueryResult) result;
    }

    private void getTransformationStep(LogicalStep logicalStep) {
        TransformationStep transformation = (TransformationStep) logicalStep;
        if (transformation instanceof Project) {
            Project project = (Project) transformation;
            session = sessions.get(project.getClusterName().getName());

            tableName = project.getTableName();

            catalogInc = tableName.isCompletedName();
            if (catalogInc) {
                CatalogName catalogName = tableName.getCatalogName();
                catalog = catalogName.getName();
            }
            selectionClause = project.getColumnList();
        } else {
            if (transformation instanceof Filter) {
                Filter filter = (Filter) transformation;
                whereInc = true;
                Relation relation = filter.getRelation();
                where.add(relation);
            } else if (transformation instanceof Limit) {
                Limit limitClause = (Limit) transformation;
                limit = limitClause.getLimit();
            } else if (transformation instanceof Select) {
                Select select = (Select) transformation;
                aliasColumns = select.getColumnMap();
            } else if (transformation instanceof OrderBy) {
                OrderBy orderBy = (OrderBy) transformation;
                orderByColumns = orderBy.getIds();
            }
        }
    }

    /**
     * Method that convert a query to a cassandra language.
     * @return java.lang.String with the Cassandra query.
     */
    public String parseQuery() {
        StringBuilder sb = new StringBuilder("SELECT ");
        if (aliasColumns!=null && aliasColumns.size()!=0){
            sb.append(getAliasClause());
        }else {
            sb.append(getSelectionClause());
        }
        sb.append(getFromClause());

        if (whereInc) {
            sb.append(getWhereClause());
        }

        if (!orderByColumns.isEmpty()){
            sb.append(getOrderByClause());
        }

        if (limitInc) {
            sb.append(" WHERE ROWNUM <= ").append(limit);
        }
        return sb.toString().replace("  ", " ");
    }

    @Override
    public void asyncExecute(String queryId, LogicalWorkflow workflow, IResultHandler resultHandler)
            throws ConnectorException {
        throw new UnsupportedException("Async execute not supported yet.");
    }

    @Override
    public void stop(String queryId) throws ConnectorException {
        throw new UnsupportedException("Stop for Async execute not supported yet.");
    }

    private String getOrderByClause() {
        StringBuffer sb=new StringBuffer();
        sb.append(" ORDER BY ");
        int count=0;
        for (OrderByClause orderByClause:orderByColumns){
            if (count!=0){
                sb.append(",");
            }
            count=1;
            ColumnSelector columnSelector=(ColumnSelector)orderByClause.getSelector();
            sb.append(columnSelector.getName().getName()).append(" ").append(orderByClause.getDirection().name());
        }
        return sb.toString();
    }

    private String getWhereClause() {
        StringBuilder sb = new StringBuilder();
        sb.append(" WHERE ");
        int count = 0;
        for (Relation relation : where) {
            if (count > 0) {
                sb.append(" AND ");
            }
            count = 1;
            switch (relation.getOperator()) {
                case IN:
                case BETWEEN:
                    break;
                default:
                    String whereWithQualification = relation.toString();
                    String parts[] = whereWithQualification.split(" ");
                    String columnName = parts[0].substring(parts[0].lastIndexOf('.') + 1);
                    sb.append(columnName);
                    for (int i = 1; i < parts.length; i++) {
                        sb.append(" ").append(parts[i]);
                    }
                    break;
            }
        }

        return sb.toString();

    }

    private String getFromClause() {
        StringBuilder sb = new StringBuilder();
        sb.append(" FROM ");
        if (catalogInc) {
            sb.append(catalog).append(".");
        }
        sb.append(tableName.getName());
        return sb.toString();
    }

    private String getSelectionClause() {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (ColumnName columnName : selectionClause) {
            if (i != 0) {
                sb.append(",");
            }
            i = 1;
            sb.append(columnName.getName());
        }
        return sb.toString();
    }

    private String getAliasClause() {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (Map.Entry<Selector,String> entry:aliasColumns.entrySet()) {
            if (i != 0) {
                sb.append(",");
            }
            i = 1;
            sb.append(entry.getKey().getColumnName().getName());
        }
        return sb.toString();
    }

    public Statement getSession() {
        return session;
    }
}
