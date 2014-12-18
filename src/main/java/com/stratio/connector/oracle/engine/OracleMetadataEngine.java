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

import com.stratio.connector.oracle.statements.CreateTableStatement;
import com.stratio.crossdata.common.connector.IMetadataEngine;
import com.stratio.crossdata.common.data.*;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.CatalogMetadata;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.TableMetadata;

import java.sql.Statement;
import java.util.List;
import java.util.Map;

/**
 * Oracle metadata engine implementation.
 */
public class OracleMetadataEngine implements IMetadataEngine{

    private Map<String, Statement> sessions;
    private Statement session = null;

    @Override
    public void createCatalog(ClusterName targetCluster, CatalogMetadata catalogMetadata)
            throws ConnectorException {
        throw new UnsupportedException("Method not implemented");
    }

    @Override
    public void createTable(ClusterName targetCluster, TableMetadata tableMetadata)
            throws ConnectorException {
        session = sessions.get(targetCluster.getName());

        Map<Selector, Selector> tableOptions = tableMetadata.getOptions();
        List<ColumnName> primaryKey = tableMetadata.getPrimaryKey();
        List<ColumnName> partitionKey = tableMetadata.getPartitionKey();
        List<ColumnName> clusterKey = tableMetadata.getClusterKey();

        int primaryKeyType;
        if (primaryKey.size() == 1) {
            primaryKeyType = PRIMARY_SINGLE;
        } else {
            if (clusterKey.isEmpty()) {
                primaryKeyType = PRIMARY_AND_CLUSTERING_SPECIFIED;
            } else {
                primaryKeyType = PRIMARY_COMPOSED;
            }
        }
        String stringOptions = getStringOptions(tableOptions);

        CreateTableStatement tableStatement =
                new CreateTableStatement(tableMetadata, primaryKey, partitionKey, clusterKey,
                        primaryKeyType, stringOptions, true);
        CassandraExecutor.execute(tableStatement.toString(), session);
    }

    @Override
    public void dropCatalog(ClusterName targetCluster, CatalogName name) throws ConnectorException {
        throw new UnsupportedException("Method not implemented");
    }

    @Override
    public void dropTable(ClusterName targetCluster, TableName name) throws ConnectorException {
        throw new UnsupportedException("Method not implemented");
    }

    @Override
    public void alterTable(ClusterName targetCluster, TableName name, AlterOptions alterOptions)
            throws ConnectorException {
        throw new UnsupportedException("Method not implemented");
    }

    @Override
    public void createIndex(ClusterName targetCluster, IndexMetadata indexMetadata)
            throws ConnectorException {
        throw new UnsupportedException("Method not implemented");
    }

    @Override
    public void dropIndex(ClusterName targetCluster, IndexMetadata indexMetadata) throws ConnectorException {
        throw new UnsupportedException("Method not implemented");
    }
}
