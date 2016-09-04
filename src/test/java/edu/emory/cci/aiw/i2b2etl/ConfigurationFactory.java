package edu.emory.cci.aiw.i2b2etl;

/*
 * #%L
 * AIW i2b2 ETL
 * %%
 * Copyright (C) 2012 - 2015 Emory University
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
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import javax.naming.NamingException;
import org.apache.commons.dbcp2.BasicDataSource;
import org.arp.javautil.sql.DataSourceInitialContextBinder;
import org.protempa.backend.Configuration;
import org.protempa.backend.ConfigurationsLoadException;
import org.protempa.backend.ConfigurationsNotFoundException;
import org.protempa.bconfigs.ini4j.INIConfigurations;

/**
 *
 * @author Andrew Post
 */
public class ConfigurationFactory implements AutoCloseable {

    static class Builder {

        private boolean populateMetaSchema;
        private boolean populateDataSchema;
        private String destinationConfigResource;

        public Builder() {
            this.populateMetaSchema = true;
            this.populateDataSchema = true;
        }

        public boolean isPopulateMetaSchema() {
            return populateMetaSchema;
        }

        public void setPopulateMetaSchema(boolean populateMetaSchema) {
            this.populateMetaSchema = populateMetaSchema;
        }

        public Builder populateMetaSchema(boolean populateMetaSchema) {
            setPopulateMetaSchema(populateMetaSchema);
            return this;
        }

        public boolean isPopulateDataSchema() {
            return populateDataSchema;
        }

        public void setPopulateDataSchema(boolean populateDataSchema) {
            this.populateDataSchema = populateDataSchema;
        }

        public String getDestinationConfigResource() {
            return destinationConfigResource;
        }

        public void setDestinationConfigResource(String destinationConfigResource) {
            this.destinationConfigResource = destinationConfigResource;
        }

        public Builder populateDataSchema(boolean populateDataSchema) {
            setPopulateDataSchema(populateDataSchema);
            return this;
        }

        public ConfigurationFactory build() throws NamingException, IOException, SQLException {
            return new ConfigurationFactory(this.populateMetaSchema, this.populateDataSchema, this.destinationConfigResource);
        }

    }

    private static final String DRIVER_CLASS_NAME = "org.h2.Driver";
    private static final String I2B2_DATASOURCE = "I2b2Data";
    public static final String I2B2_DATA_JNDI_URI = "java:/comp/env/jdbc/" + I2B2_DATASOURCE;
    private static final String I2B2_METASOURCE = "I2b2Meta";
    public static final String I2B2_META_JNDI_URI = "java:/comp/env/jdbc/" + I2B2_METASOURCE;
    private static final int MIN_IDLE = 1;
    private static final int MAX_IDLE = 5;
    private static final int MAX_TOTAL = 30;
    private static final String DEFAULT_DESTINATION_CONFIG_RESOURCE = "/conf.xml";

    private BasicDataSource metaDS;
    private BasicDataSource dataDS;
    private final String destinationConfigResource;
    /*
     * Binding for the H2 database connection pool
     */
    private final DataSourceInitialContextBinder initialContextBinder;

    public ConfigurationFactory(String destinationConfigResource) throws NamingException, IOException, SQLException {
        this(true, true, destinationConfigResource);
    }

    private ConfigurationFactory(boolean populateMetaSchema, boolean populateDataSchema, String destinationConfigResource) throws NamingException, IOException, SQLException {
        this.initialContextBinder = new DataSourceInitialContextBinder();
        if (destinationConfigResource != null) {
            this.destinationConfigResource = destinationConfigResource;
        } else {
            this.destinationConfigResource = DEFAULT_DESTINATION_CONFIG_RESOURCE;
        }

        try {
            if (populateMetaSchema) {
                File ksbDb = new I2b2MetadataSchemaPopulator().populate();
                this.metaDS = newBasicDataSource("jdbc:h2:" + ksbDb.getAbsolutePath() + ";DEFAULT_ESCAPE='';INIT=RUNSCRIPT FROM 'src/test/resources/i2b2_temp_tables.sql';LOG=0;LOCK_MODE=0;UNDO_LOG=0");
                this.initialContextBinder.bind(I2B2_METASOURCE, this.metaDS);
            }

            if (populateDataSchema) {
                File dsbDb = new I2b2DataSchemaPopulator().populate();
                this.dataDS = newBasicDataSource("jdbc:h2:" + dsbDb.getAbsolutePath() + ";LOG=0;LOCK_MODE=0;UNDO_LOG=0");
                this.initialContextBinder.bind(I2B2_DATASOURCE, this.dataDS);
            }
        } catch (IOException | SQLException ex) {
            try {
                this.initialContextBinder.close();
            } catch (Exception ignore) {
            }
            throw ex;
        }
    }
    
    public String getDataJndiUri() {
        return I2B2_DATA_JNDI_URI;
    }
    
    public String getMetaJndiUri() {
        return I2B2_META_JNDI_URI;
    }

    public Configuration getProtempaConfiguration() throws ConfigurationsLoadException, ConfigurationsNotFoundException {
        return new INIConfigurations(new File("src/test/resources")).load("i2b2-test-config");
    }
    
    public String getDestinationConfigResource() {
        return this.destinationConfigResource;
    }

    @Override
    public void close() throws Exception {
        try {
            this.initialContextBinder.close();
        } finally {
            try {
                if (this.metaDS != null) {
                    this.metaDS.close();
                }
            } finally {
                if (this.dataDS != null) {
                    this.dataDS.close();
                }
            }
        }
    }

    private static BasicDataSource newBasicDataSource(String url) {
        BasicDataSource bds = new BasicDataSource();
        bds.setDriverClassName(DRIVER_CLASS_NAME);
        bds.setUrl(url);
        bds.setMinIdle(MIN_IDLE);
        bds.setMaxIdle(MAX_IDLE);
        bds.setMaxTotal(MAX_TOTAL);
        return bds;
    }
}
