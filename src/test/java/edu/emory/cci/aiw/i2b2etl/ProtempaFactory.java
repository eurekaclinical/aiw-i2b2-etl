/*
 * #%L
 * AIW i2b2 ETL
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
package edu.emory.cci.aiw.i2b2etl.dest;

import edu.emory.cci.aiw.i2b2etl.ConfigurationFactory;
import edu.emory.cci.aiw.i2b2etl.DatabasePopulator;
import edu.emory.cci.aiw.i2b2etl.I2b2DestinationFactory;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.sql.Connection;
import java.sql.SQLException;
import javax.naming.NamingException;
import org.arp.javautil.sql.DatabaseAPI;
import org.arp.javautil.sql.InvalidConnectionSpecArguments;
import org.dbunit.Assertion;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.SortedDataSet;
import org.dbunit.dataset.xml.FlatDtdWriter;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.protempa.Protempa;
import org.protempa.ProtempaException;
import org.protempa.SourceFactory;
import org.protempa.query.Query;
import org.protempa.query.QueryBuilder;

/**
 *
 * @author Andrew Post
 */
public class ProtempaFactory implements AutoCloseable {
    private final ConfigurationFactory configurationFactory;
    private final DatabasePopulator databasePopulator;

    public ProtempaFactory(ConfigurationFactory configurationFactory) throws DatabaseUnitException, SQLException, IOException {
        this.configurationFactory = configurationFactory;
        this.databasePopulator = new DatabasePopulator();
        this.databasePopulator.doPopulate();
    }

    public Protempa newInstance() throws ProtempaException {
        SourceFactory sourceFactory = new SourceFactory(this.configurationFactory.getProtempaConfiguration());
        return Protempa.newInstance(sourceFactory);
    }

    public void execute(QueryBuilder queryBuilder) throws IOException, ProtempaException, NamingException, SQLException {
        try (Protempa protempa = newInstance()) {
            Query query = protempa.buildQuery(queryBuilder);
            protempa.execute(query, new I2b2DestinationFactory().getInstance());
        }
    }

    public void exportI2b2DataSchema(OutputStream outputStream) throws InvalidConnectionSpecArguments, SQLException, DatabaseUnitException, IOException {
        try (Connection conn = DatabaseAPI.DATASOURCE.newConnectionSpecInstance(ConfigurationFactory.I2B2_DATA_JNDI_URI, null, null).getOrCreate()) {
            IDatabaseConnection dbUnitConn = new DatabaseConnection(conn);
            try {
                IDataSet dataSet = dbUnitConn.createDataSet();
                FlatXmlDataSet.write(dataSet, outputStream);
                dbUnitConn.close();
                dbUnitConn = null;
            } finally {
                if (dbUnitConn != null) {
                    try {
                        dbUnitConn.close();
                    } catch (Exception ignore) {
                    }
                }
            }
        }
    }
    
    public void exportI2b2DataSchemaDtd(Writer writer) throws InvalidConnectionSpecArguments, SQLException, DatabaseUnitException {
        try (Connection conn = DatabaseAPI.DATASOURCE.newConnectionSpecInstance(ConfigurationFactory.I2B2_DATA_JNDI_URI, null, null).getOrCreate()) {
            IDatabaseConnection dbUnitConn = new DatabaseConnection(conn);
            try {
                IDataSet dataSet = dbUnitConn.createDataSet();
                FlatDtdWriter dtdWriter = new FlatDtdWriter(writer);
                dtdWriter.write(dataSet);
                dbUnitConn.close();
                dbUnitConn = null;
            } finally {
                if (dbUnitConn != null) {
                    try {
                        dbUnitConn.close();
                    } catch (Exception ignore) {}
                }
            }
        }
    }

    public void testTable(String tableName, IDataSet expectedDataSet) throws InvalidConnectionSpecArguments, SQLException, DatabaseUnitException, IOException {
        try (Connection conn = DatabaseAPI.DATASOURCE.newConnectionSpecInstance(ConfigurationFactory.I2B2_DATA_JNDI_URI, null, null).getOrCreate()) {
            IDatabaseConnection dbUnitConn = new DatabaseConnection(conn);
            try {
                IDataSet actualDataSet = dbUnitConn.createDataSet();
                Assertion.assertEqualsIgnoreCols(new SortedDataSet(expectedDataSet), new SortedDataSet(actualDataSet), tableName, new String[]{"IMPORT_DATE", "DOWNLOAD_DATE"});
                dbUnitConn.close();
                dbUnitConn = null;
            } finally {
                if (dbUnitConn != null) {
                    try {
                        dbUnitConn.close();
                    } catch (Exception ignore) {
                    }
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        this.databasePopulator.close();
    }
}
