/*
 * #%L
 * protempa-handler-test
 * %%
 * Copyright (C) 2012 - 2013 Emory University
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package edu.emory.cci.aiw.i2b2etl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.arp.javautil.sql.ConnectionSpec;
import org.arp.javautil.sql.DatabaseAPI;
import org.arp.javautil.sql.InvalidConnectionSpecArguments;
import org.arp.javautil.sql.SQLExecutor;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.operation.DatabaseOperation;

/**
 * @author Andrew Post
 */
public final class DatabasePopulator implements AutoCloseable {

    private static final String JDBC_URL_POPULATE = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;INIT=RUNSCRIPT FROM 'src/test/resources/test-schema.sql'";
    
    private static final String JDBC_URL_NO_POPULATE = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1";

    /**
     * Sample data file
     */
    private static final String SAMPLE_DATA_FILE = "/testData.xml";

    private static final String SCHEMA = "TEST";

    private static final Logger logger = Logger
            .getLogger(DatabasePopulator.class.getName());

    private final ConnectionSpec connectionSpec;
    private final ConnectionSpec noPopulateConnectionSpec;

    public DatabasePopulator() {
        try {
            this.connectionSpec = DatabaseAPI.DRIVERMANAGER.newConnectionSpecInstance(JDBC_URL_POPULATE, null, null, false);
            this.noPopulateConnectionSpec = DatabaseAPI.DRIVERMANAGER.newConnectionSpecInstance(JDBC_URL_NO_POPULATE, null, null, false);
        } catch (InvalidConnectionSpecArguments ex) {
            throw new AssertionError(ex);
        }
    }

    public void doPopulate() throws SQLException, DatabaseUnitException {
        logger.log(Level.INFO, "Populating database");
        try (Connection conn = this.connectionSpec.getOrCreate()) {
            IDatabaseConnection dbunitConn = new DatabaseConnection(conn, SCHEMA);
            try {
                IDataSet dataSet = new FlatXmlDataSetBuilder().build(getClass().getResource(SAMPLE_DATA_FILE));
                DatabaseOperation.CLEAN_INSERT.execute(dbunitConn, dataSet);
                logger.log(Level.INFO, "Database populated");
                conn.commit();
                dbunitConn.close();
                dbunitConn = null;
            } catch (SQLException | DatabaseUnitException ex) {
                try {
                    conn.rollback();
                } catch (SQLException ignore) {}
                throw ex;
            } finally {
                if (dbunitConn != null) {
                    try {
                        dbunitConn.close();
                    } catch (Exception ignore) {}
                }
            }
        }

    }
    
    @Override
    public void close() throws Exception {
        SQLExecutor.executeSQL(this.noPopulateConnectionSpec, "DROP ALL OBJECTS", null);
    }
}
