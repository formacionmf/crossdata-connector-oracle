package com.stratio.connector.oracle.engine;

import org.apache.log4j.Logger;

import java.sql.*;
import java.util.Arrays;

/**
 * Created by carlos on 16/12/14.
 */
public class Engine {
    /**
     * Class logger.
     */
    private static final Logger LOG = Logger.getLogger(Engine.class.getName());
    /**
     * Oracle Java Driver session.
     */
    private final Statement session;

    /**
     * Class constructor.
     *
     * @param config The {@link com.stratio.connector.oracle.engine.EngineConfig}.
     */
    public Engine(EngineConfig config) {
        this.session = initializeDB(config);
    }

    /**
     * Initialize the connection to the underlying database.
     *
     * @param config The {@link com.stratio.connector.oracle.engine.EngineConfig}.
     * @return A new Session.
     */
    private Statement initializeDB(EngineConfig config) {

        //conexión a BBDD
        LOG.info("-------- Oracle JDBC Connection Testing ------");

        try {

            Class.forName("oracle.jdbc.driver.OracleDriver");

        } catch (ClassNotFoundException e) {

            LOG.info("Where is your Oracle JDBC Driver?");
            e.printStackTrace();

        }

        LOG.info("Oracle JDBC Driver Registered!");

        Connection connection = null;

        try {

            connection = DriverManager.getConnection(
                    "jdbc:oracle:thin:@" + config.getOracleHost() +
                            ":" + config.getOraclePort() +
                            ":" + config.getClusterName(),
                            "SYSTEM",
                            "password");

        } catch (SQLException e) {

            LOG.error("Connection Failed! Check output console");
            e.printStackTrace();
        }

        Statement stmt = null;

        if (connection != null) {
            LOG.info("You made it, take control your database now!");

            try {
                stmt = connection.createStatement();

            } catch (SQLException e) {
                LOG.error("Statement Failed! Check output console");
                e.printStackTrace();
            }
        }

        return stmt;

    }

    /**
     * Close open connections.
     */
    public void shutdown() {

        try {
            session.close();
        } catch (SQLException e) {
            LOG.error("Close open connections Failed! Check output console");
            e.printStackTrace();
        }

    }

    public Statement getSession() {
        return session;
    }
}
