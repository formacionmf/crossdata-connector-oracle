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

import com.stratio.connector.oracle.statements.*;
import com.stratio.crossdata.common.connector.IMetadataEngine;
import com.stratio.crossdata.common.data.*;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.CatalogMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.StringSelector;
import com.stratio.crossdata.common.utils.StringUtils;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Oracle metadata engine implementation.
 */
public class OracleMetadataEngine implements IMetadataEngine{

    private Map<String, Statement> sessions;
    private Statement session = null;
    private int defaultLength = 50;

    /**
     * Basic constructor.
     *
     * @param sessions The map of sessions that affect the queries.
     */
    public OracleMetadataEngine(Map<String, Statement> sessions, int defaultLength) {
        this.sessions = sessions;
        this.defaultLength = defaultLength;
    }

    @Override
    public void createCatalog(ClusterName targetCluster, CatalogMetadata catalogMetadata)
            throws ConnectorException {

        int rowCount;
        session = sessions.get(targetCluster.getName());

        String catalogName = catalogMetadata.getName().getQualifiedName();


        CreateCatalogStatement catalogStatement =
                new CreateCatalogStatement(catalogName);

        try {
            java.sql.ResultSet rs = session.executeQuery(catalogStatement.toString());
            rowCount = rs.last() ? rs.getRow() : 0;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new ConnectorException("ERROR: Create Catalog " + e.getMessage());
        }
        if (rowCount == 0){
            throw new ExecutionException("ERROR: Catalog does not exists");
        }


    }

    @Override
    public void alterCatalog(ClusterName targetCluster, CatalogName catalogName, Map<Selector, Selector> options) throws ConnectorException {
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

        Map<String,Object> textoptions = getTextOptions(tableOptions);

        CreateTableStatement tableStatement =
                new CreateTableStatement(tableMetadata, primaryKey,
                        textoptions,defaultLength);
        try {
            session.execute(tableStatement.toString());
        } catch (SQLException e) {
            if (((SQLException)e).getErrorCode() != 955) //ORA-955 -> Tabla ya existe
            {
                e.printStackTrace();
                throw new ConnectorException("ERROR: Create Table " + e.getMessage() +
                        ". Error Code: " + ((SQLException) e).getErrorCode());
            }
        }
    }

    private Map<String,Object> getTextOptions(Map<Selector, Selector> options) {
        Map<String,Object> result = new HashMap<String, Object>();

        for (Map.Entry<Selector, Selector> entry : options.entrySet()) {
            StringSelector stringKeySelector = (StringSelector) entry.getKey();
            StringSelector optionSelector = (StringSelector) entry.getValue();

            if(stringKeySelector.getValue().equals("textlenght")){
                result = StringUtils.convertJsonToMap(optionSelector.getValue());
            }

        }
        return result;
    }

    private StringBuilder getStyleStringOption(String key, String value) {
        StringBuilder stringOption = new StringBuilder();

        stringOption.append(key).append(" = {")
                .append(value).append("}");

        return stringOption;
    }

    @Override
    public void dropCatalog(ClusterName targetCluster, CatalogName name) throws ConnectorException {
        throw new UnsupportedException("Method not implemented");
    }

    /**
     * Drop table that was created previously.
     *
     * @param targetCluster The target cluster.
     * @param name          The TableName of the Table.
     * @throws ConnectorException
     */
    @Override
    public void dropTable(ClusterName targetCluster, TableName name)
            throws ConnectorException {
        session = sessions.get(targetCluster.getName());
        DropTableStatement tableStatement = new DropTableStatement(name.getQualifiedName());
        try {
            session.execute(tableStatement.toString());
        } catch (SQLException e) {
            if (((SQLException)e).getErrorCode() != 942) //ORA-942 -> Tabla no existe en BBDD
            {
                e.printStackTrace();
                throw new ConnectorException("ERROR: Drop Table " + e.getMessage() +
                        ". Error Code: " + ((SQLException) e).getErrorCode());
            }
        }
    }

    @Override
    public void alterTable(ClusterName targetCluster, TableName name, AlterOptions alterOptions)
            throws ConnectorException {
        AlterTableStatement tableStatement;
        session = sessions.get(targetCluster.getName());
        switch (alterOptions.getOption()) {
            case ALTER_COLUMN:
            case ADD_COLUMN:
            case DROP_COLUMN:
                ColumnType type = alterOptions.getColumnMetadata().getColumnType();
                tableStatement = new AlterTableStatement(name, alterOptions.getColumnMetadata().getName()
                        , type, alterOptions.getOption(),defaultLength);
                try {
                    session.execute(tableStatement.toString());
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new ConnectorException("ERROR: Alter Table " + e.getMessage());
                }
                break;
            default:
                break;
        }
    }

    @Override
    public void createIndex(ClusterName targetCluster, IndexMetadata indexMetadata)
            throws ConnectorException {
        session = sessions.get(targetCluster.getName());
        CreateIndexStatement indexStatement =
                new CreateIndexStatement(indexMetadata, true, session);
        try {
            session.execute(indexStatement.toString());
        } catch (SQLException e) {
            if (((SQLException)e).getErrorCode() != 955) //ORA-955 -> Indice ya existe
            {
                e.printStackTrace();
                throw new ConnectorException("ERROR: Create Index " + e.getMessage() +
                        ". Error Code: " + ((SQLException) e).getErrorCode());
            }
        }
    }

    @Override
    public void dropIndex(ClusterName targetCluster, IndexMetadata indexName) throws ConnectorException {
        session = sessions.get(targetCluster.getName());
        DropIndexStatement indexStatement = new DropIndexStatement(indexName, true);

        try {
            session.execute(indexStatement.toString());
        } catch (SQLException e) {
            if (((SQLException)e).getErrorCode() != 1418) //ORA-1418 -> Indice no existe
            {
                e.printStackTrace();
                throw new ConnectorException("ERROR: Drop Index " + e.getMessage() +
                        ". Error Code: " + ((SQLException) e).getErrorCode());
            }
        }
    }

    @Override
    public List<CatalogMetadata> provideMetadata(ClusterName clusterName) throws ConnectorException {
        return null;
    }

    @Override
    public CatalogMetadata provideCatalogMetadata(ClusterName clusterName, CatalogName catalogName) throws ConnectorException {
        return null;
    }

    @Override
    public TableMetadata provideTableMetadata(ClusterName clusterName, TableName tableName) throws ConnectorException {
        return null;
    }
}
