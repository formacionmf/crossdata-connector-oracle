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

import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import com.stratio.connector.oracle.statements.DeleteStatement;
import com.stratio.connector.oracle.statements.InsertIntoStatement;
import com.stratio.connector.oracle.statements.TruncateStatement;
import com.stratio.connector.oracle.statements.UpdateTableStatement;
import com.stratio.connector.oracle.utils.ColumnInsertOracle;
import com.stratio.crossdata.common.connector.IStorageEngine;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.Relation;

/**
 * Oracle storage engine.
 */
public class OracleStorageEngine implements IStorageEngine{
    private Map<String, Statement> sessions;
    /**
     * Basic Constructor.
     *
     * @param sessions Map with the sessions
     */
    public OracleStorageEngine(Map<String, Statement> sessions) {
        this.sessions = sessions;
    }

    /**
     * Insert method to a table.
     *
     * @param targetCluster The target cluster.
     * @param targetTable   The target table.
     * @param row           The inserted row.
     * @throws ConnectorException
     */
    @Override public void insert(ClusterName targetCluster, TableMetadata targetTable, Row row)
            throws ConnectorException {
        Statement session = sessions.get(targetCluster.getName());
        String query = insertBlock(row, targetTable);
        try {
            session.execute(query);
        } catch (SQLException e) {
                e.printStackTrace();
                throw new ConnectorException("ERROR: Insert row " + e.getMessage() +
                        ". Error Code: " + ((SQLException) e).getErrorCode());
        }
    }

    @Override public void insert(ClusterName targetCluster, TableMetadata targetTable, Collection<Row> rows)
            throws ConnectorException {
        Statement session = sessions.get(targetCluster.getName());
        for (Row row : rows) {
            String query = insertBlock(row, targetTable);
            try {
                session.execute(query);
            } catch (SQLException e) {
                e.printStackTrace();
                throw new ConnectorException("ERROR: Insert row " + e.getMessage() +
                        ". Error Code: " + ((SQLException) e).getErrorCode());
            }
        }
    }

    private String insertBlock(Row row, TableMetadata targetTable) throws ExecutionException {
        Set<String> keys = row.getCells().keySet();
        Map<ColumnName, ColumnMetadata> columnsWithMetadata = targetTable.getColumns();
        Map<String, ColumnInsertOracle> columnsMetadata = new HashMap<>();
        try {
            for (String key : keys) {
                ColumnName col =
                        new ColumnName(targetTable.getName().getCatalogName().getName(),
                                targetTable.getName().getName(), key);
                columnsMetadata.put(key,
                        new ColumnInsertOracle(columnsWithMetadata.get(col).getColumnType(),
                                row.getCell(key).toString(), key));
            }
        } catch (Exception e) {
            throw new ExecutionException("Trying insert data in a not existing column", e);
        }

        InsertIntoStatement insertStatement =
                new InsertIntoStatement(targetTable, columnsMetadata);
        return insertStatement.toString();
    }

    @Override public void delete(ClusterName targetCluster, TableName tableName, Collection<Filter> whereClauses)
            throws ConnectorException {
        Statement session = sessions.get(targetCluster.getName());
        List<Filter> whereFilters = new ArrayList<>();
        for (Filter filter : whereClauses) {
            whereFilters.add(filter);
        }
        DeleteStatement deleteStatement = new DeleteStatement(tableName, whereFilters);
        String query = deleteStatement.toString();
        try {
            session.execute(query);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new ConnectorException("ERROR: Delete row " + e.getMessage() +
                    ". Error Code: " + ((SQLException) e).getErrorCode());
        }
    }

    @Override public void update(ClusterName targetCluster, TableName tableName, Collection<Relation> assignments,
            Collection<Filter> whereClauses) throws ConnectorException {
        Statement session = sessions.get(targetCluster.getName());
        UpdateTableStatement updateStatement = new UpdateTableStatement(tableName, assignments, whereClauses);
        String query = updateStatement.toString();
        try {
            session.execute(query);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new ConnectorException("ERROR: Update Table " + e.getMessage() +
                    ". Error Code: " + ((SQLException) e).getErrorCode());
        }
    }

    @Override public void truncate(ClusterName targetCluster, TableName tableName) throws ConnectorException {
        Statement session = sessions.get(targetCluster.getName());
        TruncateStatement truncateStatement = new TruncateStatement(tableName);
        String query = truncateStatement.toString();
        try {
            session.execute(query);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new ConnectorException("ERROR: Truncate Table " + e.getMessage() +
                    ". Error Code: " + ((SQLException) e).getErrorCode());
        }
    }
}
