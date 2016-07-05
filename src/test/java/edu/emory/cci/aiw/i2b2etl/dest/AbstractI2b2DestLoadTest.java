package edu.emory.cci.aiw.i2b2etl.dest;

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

import edu.emory.cci.aiw.i2b2etl.ConfigurationFactory;
import static edu.emory.cci.aiw.i2b2etl.dest.AbstractI2b2DestDataAndMetadataTest.getProtempaFactory;
import static edu.emory.cci.aiw.i2b2etl.dest.AbstractI2b2DestTest.getProtempaFactory;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import org.arp.javautil.sql.InvalidConnectionSpecArguments;
import org.dbunit.DatabaseUnitException;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.junit.Test;

/**
 * Parent class that contains some of the common methods used in other test classes.
 * Breaks down the test to test each table's data.
 *
 * @author Andrew Post
 */
public abstract class AbstractI2b2DestLoadTest extends AbstractI2b2DestTest {
    private static IDataSet expectedDataSet;
    
    protected static void setExpectedDataSet(String resource) throws IOException, DataSetException {
        expectedDataSet = new FlatXmlDataSetBuilder().build(AbstractI2b2DestTest.class.getResource(resource));
    }
    
    @Test
    public void testEKRejectedObservationFact() throws Exception {
        testTable("EK_REJECTED_OBSERVATION_FACT", expectedDataSet);
    }

    @Test
    public void testEKTempConcept() throws Exception {
        testTable("EK_TEMP_CONCEPT", expectedDataSet);
    }

    @Test
    public void testEKTempEncounterMapping() throws Exception {
        testTable("EK_TEMP_ENCOUNTER_MAPPING", expectedDataSet);
    }

    @Test
    public void testEKTempModifier() throws Exception {
        testTable("EK_TEMP_MODIFIER", expectedDataSet);
    }

    @Test
    public void testEKTempObservation() throws Exception {
        testTable("EK_TEMP_OBSERVATION", expectedDataSet);
    }

    @Test
    public void testEKTempPatient() throws Exception {
        testTable("EK_TEMP_PATIENT", expectedDataSet);
    }

    @Test
    public void testEKTempVisit() throws Exception {
        testTable("EK_TEMP_VISIT", expectedDataSet);
    }
    
    protected static void dumpTruth(String prefix) throws IOException, InvalidConnectionSpecArguments, SQLException, DatabaseUnitException {
        File file = File.createTempFile(prefix, ".xml");
        try (FileOutputStream out = new FileOutputStream(file)) {
            getProtempaFactory().exportI2b2MetaSchema(out);
            System.out.println("Dumped i2b2 data schema to " + file.getAbsolutePath());
        }
    }

    private void testTable(String tableName, IDataSet expectedDataSet) throws InvalidConnectionSpecArguments, SQLException, DatabaseUnitException, IOException {
        getProtempaFactory().testDataTable(tableName, expectedDataSet);
    }
}
