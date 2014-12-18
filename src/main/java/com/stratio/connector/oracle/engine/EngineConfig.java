package com.stratio.connector.oracle.engine;

import com.stratio.crossdata.common.security.ICredentials;

import java.util.Arrays;
import java.util.Random;

/**
 * Created by carlos on 16/12/14.
 */
public class EngineConfig {

    /**
     * Oracle host.
     */
    private String oracleHost;

    /**
     * Oracle port.
     */
    private int oraclePort;

    /**
     * Cluster Name.
     */

    private String clusterName;
    private ICredentials credentials;

    /**
     * Get Oracle host.
     *
     * @return an string host
     */
    public String getOracleHost() {
        return oracleHost;
    }

    /**
     * Set com.stratio.connector.oracle.com.stratio.connector.oracle hosts.
     *
     * @param oracleHost a String containing com.stratio.connector.oracle.com.stratio.connector.oracle host.
     */
    public void setOracleHost(String oracleHost) {
        this.oracleHost = oracleHost;
    }

    /**
     * Get com.stratio.connector.oracle.com.stratio.connector.oracle port.
     *
     * @return current com.stratio.connector.oracle.com.stratio.connector.oracle port.
     */
    public int getOraclePort() {
        return oraclePort;
    }

    /**
     * Set com.stratio.connector.oracle.com.stratio.connector.oracle port.
     *
     * @param oraclePort Port of com.stratio.connector.oracle.com.stratio.connector.oracle (CQL).
     */
    public void setOraclePort(int oraclePort) {
        this.oraclePort = oraclePort;
    }

    /**
     * Get com.stratio.connector.oracle.com.stratio.connector.oracle cluster name.
     *
     * @return the cluster name.
     */
    public String getClusterName() {
        return clusterName;
    }

    /**
     * Set com.stratio.connector.oracle.com.stratio.connector.oracle port.
     *
     * @param clusterName .
     */
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }


//    public String getRandomCassandraHost() {
//        Random rand = new Random();
//        return cassandraHosts[rand.nextInt(cassandraHosts.length)];
//    }

    /**
     * Get the credentials.
     * @return ICredentials.
     */
    public ICredentials getCredentials() {
        return credentials;
    }

    /**
     * Set the credentials.
     * @param credentials
     */
    public void setCredentials(ICredentials credentials) {
        this.credentials = credentials;
    }
}
