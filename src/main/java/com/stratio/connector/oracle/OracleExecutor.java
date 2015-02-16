package com.stratio.connector.oracle;

import com.stratio.connector.oracle.utils.Utils;
import com.stratio.crossdata.common.data.*;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.*;
import com.stratio.crossdata.common.statements.structures.StringSelector;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import com.stratio.crossdata.common.statements.structures.Selector;

import javax.rmi.CORBA.Util;

/**
 * Created by carlos on 23/12/14.
 */
public final class OracleExecutor {
    /**
     * Class logger.
     */
    private static final Logger LOG = Logger.getLogger(OracleExecutor.class);

    /**
     * The {@link com.stratio.connector.oracle.utils.Utils}.
     */
    private static Utils utilidades = new com.stratio.connector.oracle.utils.Utils();

    /**
     * Private class constructor as all methods are static.
     */
    private OracleExecutor() {
    }

    /**
     * Executes a query from a String.
     *
     * @param query   The query in a String.
     * @param session Cassandra datastax java driver session.
     * @return a {@link com.stratio.crossdata.common.result.Result}.
     */
    public static com.stratio.crossdata.common.result.Result execute(String query, Connection session)
            throws ConnectorException, SQLException {
        ResultSet resultSet;
        Statement st = Utils.createStatement(session);
        try {
            resultSet = st.executeQuery(query);
            return com.stratio.crossdata.common.result
                    .QueryResult.createQueryResult(utilidades.transformToMetaResultSet(resultSet),0,true);
        } catch (UnsupportedOperationException unSupportException) {
            LOG.error("Oracle executor failed", unSupportException);
            throw new UnsupportedException(unSupportException.getMessage());
        } catch (Exception ex) {
            LOG.error("Oracle executor failed", ex);
            throw new ExecutionException(ex.getMessage());
        } finally {
            st.close();
        }
    }

    /**
     * Executes a query from a String and add the alias in the Result for Selects .
     *
     * @param query        The query in a String.
     * @param aliasColumns The Map with the alias
     * @param session      Cassandra datastax java driver session.
     * @return a {@link com.stratio.crossdata.common.result.Result}.
     */
    public static com.stratio.crossdata.common.result.Result execute(String query,
                                                                     Map<Selector, String> aliasColumns, Connection session)
            throws ConnectorException {
        try {
            Statement st = Utils.createStatement(session);
            ResultSet resultSet = st.executeQuery(query);
            return com.stratio.crossdata.common.result
                    .QueryResult
                    .createQueryResult(utilidades.transformToMetaResultSet(resultSet, aliasColumns),0,true);
        } catch (UnsupportedOperationException unSupportException) {
            LOG.error("Oracle executor failed", unSupportException);
            throw new UnsupportedException(unSupportException.getMessage());
        } catch (Exception ex) {
            LOG.error("Oracle executor failed", ex);
            throw new ExecutionException(ex.getMessage());
        }
    }

    /**
     * Obtain the existing keyspaces in oracle.
     * @param session The oracle statement.
     * @return A list of {@link com.stratio.crossdata.common.metadata.CatalogMetadata} .
     */
    public static List<CatalogMetadata> getKeyspaces(Statement session,String cluster) throws ConnectorException, InterruptedException {
        List<CatalogMetadata> catalogMetadataList=new ArrayList<>();
        List<String> schemaList = new ArrayList<>();

        Utils.semaphore.acquire();

        //try(java.sql.ResultSet rs = session.executeQuery("select username from dba_users where default_tablespace='USERS' and lock_date is null")) {
        try(java.sql.ResultSet rs = session.executeQuery("select schema_name from stratio_schemas")) {
            while(rs.next())
            {
                schemaList.add(rs.getString("schema_name"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new ConnectorException("ERROR: get Oracle Schemas " + e.getMessage());
        } finally {
            Utils.semaphore.release();
        }
        for(String mySchema : schemaList) {
            //Catalogs
            CatalogName name=new CatalogName(mySchema);

            Map<Selector, Selector> options=new HashMap<>();
            Map<String,String> replicationOptions=new HashMap<>();

            for (Map.Entry<String, String> entry : replicationOptions.entrySet()) {
                options.put(new StringSelector(entry.getKey()),new StringSelector(entry.getValue()));
            }

            //Tables
            Map<TableName, TableMetadata> tables=getTablesFromSchema(session, mySchema, cluster);

            CatalogMetadata catalogMetadata=new CatalogMetadata(name,options,tables);
            catalogMetadataList.add(catalogMetadata);
        }

        return catalogMetadataList;
    }

    /**
     * Get the tables from a specified keyspace.
     * @param session The oracle statement.
     * @param schema The schema metadata.
     * @return A map of tables.
     */
    private static Map<TableName, TableMetadata> getTablesFromSchema(Statement session,String
            schema, String cluster) throws ConnectorException, InterruptedException {
        Map<TableName, TableMetadata> tables=new HashMap<>();
        List<String> tableList = new ArrayList<>();
        //Collection<com.datastax.driver.core.TableMetadata> cassandraTables=keyspaceMetadata.getTables();

        Utils.semaphore.acquire();
        try (java.sql.ResultSet rs = session.executeQuery("select table_name from all_tables where owner = '" + schema + "'");){

            while (rs.next()) {
                tableList.add(rs.getString("table_name"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new ConnectorException("ERROR: get Oracle Table from schema " + e.getMessage());
        }finally {
            Utils.semaphore.release();
        }

        for (String myTable : tableList)
        {
            TableName tableName=new TableName(schema,myTable);
            TableMetadata tableMetadata=getXDTableMetadata(session,myTable,schema, cluster);
            tables.put(tableName,tableMetadata);
        }

        return tables;
    }


    /**
     * Get the crossdata table metadata from a oracle table metadata
     * @param session The oracle statement.
     * @param schema The oracle schema
     * @param oracleTableMetadata The oracle table metadata.
     * @return A {@link com.stratio.crossdata.common.metadata.TableMetadata} .
     */
    private static TableMetadata getXDTableMetadata(Statement session, String oracleTableMetadata, String schema, String cluster) throws ConnectorException, InterruptedException {
        Map<IndexName, IndexMetadata> indexes=new HashMap<>();
        LinkedHashMap<ColumnName, ColumnMetadata> columns=new LinkedHashMap<>();

        Utils.semaphore.acquire();
        try (java.sql.ResultSet rs = session.executeQuery("select column_name, data_type from all_tab_columns where owner = '"+ schema +"' and table_name = '"+oracleTableMetadata+"'");){

            while (rs.next()) {
                //Columns
                ColumnName columnName=new ColumnName(schema,
                        oracleTableMetadata, rs.getString("column_name"));
                ColumnType columnType=utilidades.getCrossdataColumn(rs.getString("data_type"));
                ColumnMetadata columnMetadata=new ColumnMetadata(columnName,null, columnType);
                columns.put(columnName,columnMetadata);



            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new ConnectorException("ERROR: get XD Table Metadata " + e.getMessage());
        }finally {
            Utils.semaphore.release();
        }

        ClusterName clusterRef=new ClusterName(cluster);

        List<ColumnName> partitionKey=new ArrayList<>();
        //List<com.datastax.driver.core.ColumnMetadata> partitionColumns=cassandraTableMetadata.getPartitionKey();
        String query = "SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner " +
                "FROM all_constraints cons, all_cons_columns cols " +
                "WHERE cols.table_name = '"+oracleTableMetadata +"' " +
                "AND cons.owner = '"+schema+"' " +
                "AND cons.constraint_type = 'P' " +
                "AND cons.constraint_name = cols.constraint_name " +
                "AND cons.owner = cols.owner " +
                "ORDER BY cols.table_name, cols.position";
        Utils.semaphore.acquire();
        try (java.sql.ResultSet rs = session.executeQuery(query);){

            while (rs.next()) {

                ColumnName columnName=new ColumnName(schema,
                        oracleTableMetadata,
                        rs.getString("column_name"));
                partitionKey.add(columnName);


            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new ConnectorException("ERROR: get XD Table Metadata. Schema: "+ schema + ". Table: " + oracleTableMetadata +". "+ e.getMessage());
        } finally {
            Utils.semaphore.release();
        }

        List<ColumnName> clusterKey=new ArrayList<>();

        TableName tableName=new TableName(schema,
                oracleTableMetadata);

        return new TableMetadata(tableName,null,columns,indexes,clusterRef, partitionKey,clusterKey);
    }

    /**
     * Get the specified Catalog from the cassandra keyspace.
     * @param session The oracle session.
     * @param catalogName The catalog name of the oracle schema.
     * @return A {@link com.stratio.crossdata.common.metadata.CatalogMetadata} .
     */
    public static CatalogMetadata getKeyspacesByName(Statement session, CatalogName catalogName, String cluster) throws ConnectorException, InterruptedException {

        //KeyspaceMetadata keyspace=session.getCluster().getMetadata().getKeyspace(catalogName.getName());

        CatalogName name=new CatalogName(catalogName.getName());

        Map<Selector, Selector> options=new HashMap<>();
        Map<String,String> replicationOptions=new HashMap<>();

        for (Map.Entry<String, String> entry : replicationOptions.entrySet()) {
            options.put(new StringSelector(entry.getKey()),new StringSelector(entry.getValue()));
        }

        Map<TableName, TableMetadata> tables=getTablesFromSchema(session, catalogName.getName(),cluster);
        CatalogMetadata catalogMetadata=new CatalogMetadata(name,options,tables);
        return catalogMetadata;
    }

    /**
     * Get the Crossdata TableMetadata from a tableName that is search into cassandra table metadata.
     * @param session The cassandra session.
     * @param tableName The table name to search.
     * @return A {@link com.stratio.crossdata.common.metadata.TableMetadata} .
     */
    public static TableMetadata getTablesByTableName(Statement session, TableName tableName, String cluster) throws ConnectorException, InterruptedException {

        return getXDTableMetadata(session,tableName.getName(), tableName.getCatalogName().getName() ,cluster);

    }


}
