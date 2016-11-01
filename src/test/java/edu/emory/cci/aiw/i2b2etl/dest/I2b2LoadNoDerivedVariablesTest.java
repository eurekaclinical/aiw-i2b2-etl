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


import org.junit.BeforeClass;
import org.protempa.ProtempaException;
import org.protempa.query.DefaultQueryBuilder;

/**
 * Data validation tests for the i2b2 ETL. The test initiates Protempa to access the
 * test data and execute a query before AIW ETL loads the processed data into an H2 database.
 * The new loaded data is compared to the one expected using DbUnit.
 *
 * @author Andrew Post
 */
public class I2b2LoadNoDerivedVariablesTest extends AbstractI2b2DestLoadTest {
    /**
     *  Executes the i2b2 ETL load.
     *
     * @throws Exception
     */
    @BeforeClass
    public static void setUp() throws Exception {
        DefaultQueryBuilder q = new DefaultQueryBuilder();
        q.setName("i2b2 ETL Test Query No Derived Variables");
        q.setPropositionIds(new String[]{"ICD9:Diagnoses", "ICD9:Procedures", "LAB:LabTest", "Encounter", "MED:medications", "VitalSign", "PatientDetails"});
        
        try {
            getProtempaFactory().execute(q);
        } catch (ProtempaException ex) {
            dumpTruth("i2b2LoadNoDerivedVariablesTest");
            throw ex;
        }
        
        setExpectedDataSet("/truth/i2b2LoadNoDerivedVariablesTestData.xml");
    }

}
