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

    private String driverClass;
    private String _sid;
    private String _userBBDD;
    private String _passwordBBDD;
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
     * Get com.stratio.connector.oracle.com.stratio.connector.oracle driverClass.
     *
     * @return current com.stratio.connector.oracle.com.stratio.connector.oracle driverClass.
     */
    public String getDriverClass() {
        return driverClass;
    }

    /**
     * Set com.stratio.connector.oracle.com.stratio.connector.oracle driverClass.
     *
     * @param driverClass Driver of com.stratio.connector.oracle.com.stratio.connector.oracle (CQL).
     */
    public void setDriverClass(String driverClass) {
        this.driverClass = driverClass;
    }





    /**
     * Get com.stratio.connector.oracle.com.stratio.connector.oracle cluster name.
     *
     * @return the cluster name.
     */
    public String getSID() {
        return _sid;
    }

    /**
     * Set com.stratio.connector.oracle.com.stratio.connector.oracle sid.
     *
     * @param sid .
     */
    public void setSID(String sid) {
        this._sid = sid;
    }


    /**
     * Get com.stratio.connector.oracle.com.stratio.connector.oracle cluster name.
     *
     * @return the user BBDD.
     */
    public String getUserBBDD() {
        return _userBBDD;
    }

    /**
     * Set com.stratio.connector.oracle.com.stratio.connector.oracle sid.
     *
     * @param userBBDD .
     */
    public void setUserBBDD(String userBBDD) {
        this._userBBDD = userBBDD;
    }


    /**
     * Get com.stratio.connector.oracle.com.stratio.connector.oracle cluster name.
     *
     * @return the user BBDD.
     */
    public String getPasswordBBDD() {
        return _passwordBBDD;
    }

    /**
     * Set com.stratio.connector.oracle.com.stratio.connector.oracle sid.
     *
     * @param passwordBBDD .
     */
    public void setPasswordBBDD(String passwordBBDD) {
        this._passwordBBDD = passwordBBDD;
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
