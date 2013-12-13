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
package edu.emory.cci.aiw.i2b2etl.cli;

import java.io.File;
import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.protempa.FinderException;
import org.protempa.Protempa;
import org.protempa.ProtempaStartupException;
import org.protempa.query.DefaultQueryBuilder;
import org.protempa.query.Query;
import org.protempa.query.QueryBuildException;
import org.protempa.query.handler.test.DatabasePopulator;

import edu.emory.cci.aiw.i2b2etl.I2B2QueryResultsHandlerFactory;
import edu.emory.cci.aiw.i2b2etl.ProtempaFactory;
import org.protempa.query.handler.QueryResultsHandlerFactory;

/**
 * Integration tests for the i2b2 ETL. This assumes that there is an i2b2
 * instance somewhere to use for testing. Specify where it is in your
 * settings.xml file (future). 
 * 
 * @author Andrew Post
 */
public class I2b2ETLTestNoDerivedVariables {

    /**
     * Executes the i2b2 ETL load.
     * 
     * @throws ProtempaStartupException if Protempa could not be initialized.
     * @throws IOException if there was a problem reading the Protempa
     * configuration file or the i2b2 query results handler configuration file.
     * @throws QueryBuildException if constructing the Protempa query failed.
     * @throws FinderException if executing the Protempa query failed.
     */
    @BeforeClass
    public static void setUp() throws Exception {
        new DatabasePopulator().doPopulate();
        Protempa protempa = new ProtempaFactory().newInstance();
        try {
            File confXML = new I2b2ETLConfAsFile().getFile();
            
            DefaultQueryBuilder q = new DefaultQueryBuilder();
            q.setId("i2b2 ETL Test Query No Derived Variables");
            
            Query query = protempa.buildQuery(q);
            QueryResultsHandlerFactory tdqrh = 
                    new I2B2QueryResultsHandlerFactory(confXML);
            protempa.execute(query, tdqrh);
        } finally {
            protempa.close();
        }

    }

    @Test
    public void testSomeAspectOfI2b2Database() {
    }
    
    @AfterClass
    public static void shutdown() {
        //We leave the i2b2 load behind for post-mortum analyses.
    }
}
