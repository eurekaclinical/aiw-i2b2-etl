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
package edu.emory.cci.aiw.i2b2etl;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.sql.Connection;
import java.sql.SQLException;
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
import org.protempa.dest.QueryResultsHandler;
import org.protempa.query.Query;
import org.protempa.query.QueryBuilder;

/**
 *
 * @author Andrew Post
 */
public class ProtempaFactory implements AutoCloseable {
    private final ConfigurationFactory configurationFactory;
    private final DatabasePopulator databasePopulator;
    private final I2b2DestinationFactory dest;

    public ProtempaFactory(ConfigurationFactory configurationFactory) throws IOException, SQLException, DatabaseUnitException {
        if (configurationFactory == null) {
            throw new IllegalArgumentException("configurationFactory cannot be null");
        }
        this.configurationFactory = configurationFactory;
        this.databasePopulator = new DatabasePopulator();
        this.databasePopulator.doPopulate();
        this.dest = new I2b2DestinationFactory(configurationFactory.getDestinationConfigResource());
    }

    public Protempa newInstance() throws ProtempaException {
        SourceFactory sourceFactory = new SourceFactory(this.configurationFactory.getProtempaConfiguration());
        return Protempa.newInstance(sourceFactory);
    }
    
    public QueryResultsHandler getQueryResultsHandler(QueryBuilder queryBuilder) throws ProtempaException {
        try (Protempa protempa = newInstance()) {
            Query query = protempa.buildQuery(queryBuilder);
            return this.dest.getInstance().getQueryResultsHandler(query, protempa.getDataSource(), protempa.getKnowledgeSource());
        }
    }

    public void execute(QueryBuilder queryBuilder) throws ProtempaException {
        try (Protempa protempa = newInstance()) {
            Query query = protempa.buildQuery(queryBuilder);
            protempa.execute(query, dest.getInstance());
        }
    }
    
    public void exportI2b2DataSchema(OutputStream outputStream) throws InvalidConnectionSpecArguments, SQLException, DatabaseUnitException, IOException {
        exportI2b2Schema(outputStream, this.configurationFactory.getDataJndiUri());
    }
    
    public void exportI2b2MetaSchema(OutputStream outputStream) throws InvalidConnectionSpecArguments, SQLException, DatabaseUnitException, IOException {
        exportI2b2Schema(outputStream, this.configurationFactory.getMetaJndiUri());
    }

    private void exportI2b2Schema(OutputStream outputStream, String jndiUri) throws InvalidConnectionSpecArguments, SQLException, DatabaseUnitException, IOException {
        try (Connection conn = DatabaseAPI.DATASOURCE.newConnectionSpecInstance(jndiUri, null, null, false).getOrCreate()) {
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
    
    public void exportI2b2SchemaDtdForDataSchema(Writer writer) throws InvalidConnectionSpecArguments, SQLException, DatabaseUnitException {
        exportI2b2SchemaDtd(writer, this.configurationFactory.getDataJndiUri());
    }
    
    public void exportI2b2SchemaDtdForMetaSchema(Writer writer) throws InvalidConnectionSpecArguments, SQLException, DatabaseUnitException {
        exportI2b2SchemaDtd(writer, this.configurationFactory.getMetaJndiUri());
    }
    
    private void exportI2b2SchemaDtd(Writer writer, String jndiUri) throws InvalidConnectionSpecArguments, SQLException, DatabaseUnitException {
        try (Connection conn = DatabaseAPI.DATASOURCE.newConnectionSpecInstance(jndiUri, null, null, false).getOrCreate()) {
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
    
    public void testDataTable(String tableName, IDataSet expectedDataSet) throws InvalidConnectionSpecArguments, SQLException, DatabaseUnitException, IOException {
        testTable(tableName, expectedDataSet, this.configurationFactory.getDataJndiUri());
    }
    
    public void testMetaTable(String tableName, IDataSet expectedDataSet) throws InvalidConnectionSpecArguments, SQLException, DatabaseUnitException, IOException {
        testTable(tableName, expectedDataSet, this.configurationFactory.getMetaJndiUri());
    }

    public void testTable(String tableName, IDataSet expectedDataSet, String jndiUri) throws InvalidConnectionSpecArguments, SQLException, DatabaseUnitException, IOException {
        try (Connection conn = DatabaseAPI.DATASOURCE.newConnectionSpecInstance(jndiUri, null, null, false).getOrCreate()) {
            IDatabaseConnection dbUnitConn = new DatabaseConnection(conn);
            try {
                IDataSet actualDataSet = dbUnitConn.createDataSet();
                Assertion.assertEqualsIgnoreCols(new SortedDataSet(expectedDataSet), new SortedDataSet(actualDataSet), tableName, new String[]{"IMPORT_DATE", "DOWNLOAD_DATE", "AGE_IN_YEARS_NUM", "INSTANCE_NUM", "UPDATE_DATE"});
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
