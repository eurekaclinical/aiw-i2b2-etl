/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
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
import edu.emory.cci.aiw.i2b2etl.ksb.I2b2KnowledgeSourceBackend;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import javax.naming.NamingException;
import org.apache.commons.dbcp2.BasicDataSource;
import org.arp.javautil.sql.DataSourceInitialContextBinder;
import org.protempa.backend.BackendInitializationException;
import org.protempa.backend.Configuration;
import org.protempa.backend.ConfigurationsLoadException;
import org.protempa.backend.ConfigurationsNotFoundException;
import org.protempa.bconfigs.ini4j.INIConfigurations;

/**
 *
 * @author Andrew Post
 */
public class ConfigurationFactory implements AutoCloseable {
    private static final String DRIVER_CLASS_NAME = "org.h2.Driver";
    private static final String I2B2_DATASOURCE = "I2b2Data";
    public static final String I2B2_DATA_JNDI_URI = "java:/comp/env/jdbc/" + I2B2_DATASOURCE;
    private static final int MIN_IDLE = 1;
    private static final int MAX_IDLE = 5;
    private static final int MAX_TOTAL = 30;

    private final BasicDataSource metaDS;
    private final BasicDataSource dataDS;
    /*
     * Binding for the H2 database connection pool
     */
    private final DataSourceInitialContextBinder initialContextBinder;

    public ConfigurationFactory() throws NamingException, IOException, SQLException {
        this.initialContextBinder = new DataSourceInitialContextBinder();

        File ksbDb = new I2b2MetadataSchemaPopulator().populate();
        this.metaDS = newBasicDataSource("jdbc:h2:" + ksbDb.getAbsolutePath() + ";DEFAULT_ESCAPE='';INIT=RUNSCRIPT FROM 'src/test/resources/i2b2_temp_tables.sql';CACHE_SIZE=262400");
        this.initialContextBinder.bind("I2b2Meta", this.metaDS);

        File dsbDb = new I2b2DataSchemaPopulator().populate();
        this.dataDS = newBasicDataSource("jdbc:h2:" + dsbDb.getAbsolutePath() + ";CACHE_SIZE=262400");
        this.initialContextBinder.bind("I2b2Data", this.dataDS);
    }

    public Configuration getProtempaConfiguration() throws ConfigurationsLoadException, ConfigurationsNotFoundException {
        return new INIConfigurations(new File("src/test/resources")).load("i2b2-test-config");
    }

    public I2b2KnowledgeSourceBackend newKnowledgeSourceBackend() throws ConfigurationsLoadException, ConfigurationsNotFoundException, BackendInitializationException {
        Configuration config = getProtempaConfiguration();
        I2b2KnowledgeSourceBackend ksb = new I2b2KnowledgeSourceBackend();
        ksb.initialize(config.getKnowledgeSourceBackendSections().get(0));
        return ksb;
    }

    @Override
    public void close() throws Exception {
        if (this.initialContextBinder != null) {
            this.initialContextBinder.close();
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
