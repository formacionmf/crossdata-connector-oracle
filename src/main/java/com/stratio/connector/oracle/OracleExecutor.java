package com.stratio.connector.oracle;

import com.stratio.connector.oracle.utils.Utils;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import org.apache.log4j.Logger;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

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
    private static Utils utils = new Utils();

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
    public static com.stratio.crossdata.common.result.Result execute(String query, Statement session)
            throws ConnectorException {
        ResultSet resultSet;
        try {
            resultSet = session.executeQuery(query);
            return com.stratio.crossdata.common.result
                    .QueryResult.createQueryResult(utils.transformToMetaResultSet(resultSet));
        } catch (UnsupportedOperationException unSupportException) {
            LOG.error("Oracle executor failed", unSupportException);
            throw new UnsupportedException(unSupportException.getMessage());
        } catch (Exception ex) {
            LOG.error("Oracle executor failed", ex);
            throw new ExecutionException(ex.getMessage());
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
                                                                     Map<ColumnName, String> aliasColumns, Statement session)
            throws ConnectorException {
        try {
            ResultSet resultSet = session.executeQuery(query);
            resultSet.next();
            return com.stratio.crossdata.common.result
                    .QueryResult
                    .createQueryResult(utils.transformToMetaResultSet(resultSet, aliasColumns));
        } catch (UnsupportedOperationException unSupportException) {
            LOG.error("Oracle executor failed", unSupportException);
            throw new UnsupportedException(unSupportException.getMessage());
        } catch (Exception ex) {
            LOG.error("Oracle executor failed", ex);
            throw new ExecutionException(ex.getMessage());
        }
    }
}
