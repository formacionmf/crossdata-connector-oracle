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
import org.apache.log4j.Logger;

import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.connector.IConfiguration;
import com.stratio.crossdata.common.connector.IConnector;
import com.stratio.crossdata.common.connector.IMetadataEngine;
import com.stratio.crossdata.common.connector.IQueryEngine;
import com.stratio.crossdata.common.connector.IStorageEngine;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.InitializationException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.security.ICredentials;
import com.stratio.crossdata.connectors.ConnectorApp;

import javax.swing.plaf.nimbus.State;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Connector main class that launches the connector actor wrapper and implements the
 * {@link com.stratio.crossdata.common.connector.IConnector} interface.
 */
public class OracleConnector implements IConnector{

    /**
     * Class logger.
     */
    private static final Logger LOG = Logger.getLogger(OracleConnector.class);

    private Map<String, Statement> sessions;

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
        Map<String, String> clusterOptions = config.getOptions();

        EngineConfig engineConfig = new EngineConfig();

        engineConfig.setOracleHost(
                clusterOptions.get("Host"));
        engineConfig.setOraclePort(Integer.parseInt(clusterOptions.get("Port")));
        engineConfig.setCredentials(credentials);


        Engine engine = new Engine(engineConfig);

        LOG.info("Oracle session created.");

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
        for (Statement s : sessions.values()) {
            try {
                s.close();
            } catch (SQLException e) {
                LOG.info("ERROR oracle session");
                e.printStackTrace();
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
        return new OracleStorageEngine();
    }

    @Override
    public IQueryEngine getQueryEngine() throws UnsupportedException {
        return new OracleQueryEngine();
    }

    @Override
    public IMetadataEngine getMetadataEngine() throws UnsupportedException {
        return new OracleMetadataEngine();
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
