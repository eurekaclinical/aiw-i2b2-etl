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
import edu.emory.cci.aiw.i2b2etl.AbstractTest;
import java.io.IOException;
import java.sql.SQLException;
import org.arp.javautil.sql.InvalidConnectionSpecArguments;
import org.dbunit.DatabaseUnitException;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.junit.Test;

/**
 *
 * @author Andrew Post
 */
public abstract class AbstractI2b2LoadTest extends AbstractTest {

    private static IDataSet expectedDataSet;

    protected static void setExpectedDataSet(String resource) throws IOException, DataSetException {
        expectedDataSet = new FlatXmlDataSetBuilder().build(AbstractI2b2LoadTest.class.getResource(resource));
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

    private void testTable(String tableName, IDataSet expectedDataSet) throws InvalidConnectionSpecArguments, SQLException, DatabaseUnitException, IOException {
        getProtempaFactory().testTable(tableName, expectedDataSet);
    }
}
