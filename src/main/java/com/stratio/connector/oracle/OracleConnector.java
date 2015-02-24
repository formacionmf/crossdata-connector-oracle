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

package com.stratio.connector.oracle;

import com.stratio.connector.oracle.engine.*;
import com.stratio.crossdata.common.connector.*;
import com.stratio.crossdata.common.metadata.IMetadata;
import org.apache.log4j.Logger;

import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.InitializationException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.security.ICredentials;
import com.stratio.crossdata.connectors.ConnectorApp;

import javax.swing.plaf.nimbus.State;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

/**
 * Connector main class that launches the connector actor wrapper and implements the
 * {@link com.stratio.crossdata.common.connector.IConnector} interface.
 */
public class OracleConnector implements IConnector{

    /**
     * Class logger.
     */
    private static final Logger LOG = Logger.getLogger(OracleConnector.class);

    /**
     * DEFAULT_LIMIT for the select queries.
     */
    private static final int DEFAULT_LIMIT = 100;
    private static final int DEFAULT_LENGTH = 50;

    private int defaultLength;
    private int defaultLimit;

    private Map<String, Connection> sessions;

    @Override
    public String getConnectorName() {
        return "OracleConnector";
    }

    @Override
    public String[] getDatastoreName() {
        return new String[]{"Oracle"};
    }

    @Override
    public void init(IConfiguration configuration) throws InitializationException {
        //TODO Add init functionality. This method will be called once when the connector is launched.
        //IConfiguration currently does not provide any external information.
        LOG.info("init");
    }

    @Override public void connect(ICredentials credentials, ConnectorClusterConfig config) throws ConnectionException {
        //TODO Add connect functionality. The connector should establish the connection with the underlying
        //datastore. ICredentials is currently not supported.

        ClusterName clusterName = config.getName();
        Map<String, String> clusterOptions = config.getClusterOptions();
        Map<String, String> connectorOptions = config.getConnectorOptions();

        EngineConfig engineConfig = new EngineConfig();

        engineConfig.setOracleHost(
                clusterOptions.get("Hosts"));
        engineConfig.setOraclePort(Integer.parseInt(clusterOptions.get("Port")));
        engineConfig.setCredentials(credentials);

        engineConfig.setSID(clusterOptions.get("SID"));
        engineConfig.setDriverClass(clusterOptions.get("driverClass"));
        engineConfig.setUserBBDD(clusterOptions.get("user"));
        engineConfig.setPasswordBBDD(clusterOptions.get("password"));

        if (connectorOptions.get("DefaultLength") == null) {
            defaultLength = DEFAULT_LENGTH;
        } else {
            defaultLength = Integer.parseInt(connectorOptions.get("DefaultLength"));
        }
        if (connectorOptions.get("DefaultLimit") == null) {
            defaultLimit = DEFAULT_LIMIT;
        } else {
            defaultLimit = Integer.parseInt(connectorOptions.get("DefaultLimit"));
        }


        Engine engine = null;
        try {
            engine = new Engine(engineConfig);
            LOG.info("Oracle session created.");
        } catch (SQLException e) {
            e.printStackTrace();
            LOG.info("Oracle session NO created." + e.getMessage());
        }



        sessions.put(clusterName.getName(), engine.getSession());
    }

    @Override public void close(ClusterName name) throws ConnectionException {
        LOG.info("Closing oracle session");
        try {
            sessions.get(name.getName()).close();
            sessions.remove(name.getName());
        } catch (SQLException e) {
            LOG.info("ERROR oracle session");
            e.printStackTrace();
        }
    }

    @Override public void shutdown() throws ExecutionException {

        for (Connection s : sessions.values()) {

            try {
                s.close();
            } catch (SQLException e1) {
                throw new ExecutionException(e1.getMessage());
            }
        }

        sessions = new HashMap<>();
    }

    @Override public boolean isConnected(ClusterName name) {
        boolean connected;

        if (sessions.get(name.getName()) != null) {
            try {
                if (sessions.get(name.getName()).isClosed()) {
                    connected = false;
                } else {
                    connected = true;
                }
            } catch (SQLException e) {
                connected = false;
            }
        } else {
            connected = false;
        }
        return connected;
    }

    @Override
    public IStorageEngine getStorageEngine() throws UnsupportedException {
        return new OracleStorageEngine(sessions);
    }

    @Override
    public IQueryEngine getQueryEngine() throws UnsupportedException {
        return new OracleQueryEngine(sessions, defaultLimit);
    }

    @Override
    public IMetadataEngine getMetadataEngine() throws UnsupportedException {
        return new OracleMetadataEngine(sessions,defaultLength);
    }

    @Override
    public ISqlEngine getSqlEngine() throws UnsupportedException {
        return null;
    }

    /**
     * Constructor.
     */
    public OracleConnector() {
        sessions = new HashMap<>();
    }


    /**
     * Run a Oracle Connector using a {@link com.stratio.crossdata.connectors.ConnectorApp}.
     * @param args The arguments.
     */
    public static void main(String [] args){
        OracleConnector oracleConnector = new OracleConnector();
        ConnectorApp connectorApp = new ConnectorApp();
        connectorApp.startup(oracleConnector);
    }

}
