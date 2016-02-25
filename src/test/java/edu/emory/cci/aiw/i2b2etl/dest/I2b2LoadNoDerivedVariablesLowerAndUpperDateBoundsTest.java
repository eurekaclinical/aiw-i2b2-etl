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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

import org.junit.BeforeClass;
import org.protempa.QueryException;
import org.protempa.ProtempaStartupException;
import org.protempa.backend.dsb.filter.DateTimeFilter;
import org.protempa.proposition.interval.Interval.Side;
import org.protempa.proposition.value.AbsoluteTimeGranularity;
import org.protempa.query.DefaultQueryBuilder;
import org.protempa.query.QueryBuildException;
import org.protempa.query.QueryMode;

/**
 * Integration tests for the i2b2 ETL. This assumes that there is an i2b2
 * instance somewhere to use for testing. Specify where it is in your
 * settings.xml file (future).
 *
 * @author Andrew Post
 */
public class I2b2LoadNoDerivedVariablesLowerAndUpperDateBoundsTest extends AbstractI2b2DestLoadTest {

    /**
     * Executes the i2b2 ETL load.
     *
     * @throws ProtempaStartupException if Protempa could not be initialized.
     * @throws IOException if there was a problem reading the Protempa
     * configuration file or the i2b2 query results handler configuration file.
     * @throws QueryBuildException if constructing the Protempa query failed.
     * @throws QueryException if executing the Protempa query failed.
     */
    @BeforeClass
    public static void setUp() throws Exception {
        DefaultQueryBuilder q = new DefaultQueryBuilder();
        q.setName("i2b2 ETL Test Query No Derived Variables With Lower and Upper Date Bounds");
        q.setPropositionIds(new String[]{"ICD9:Diagnoses", "ICD9:Procedures", "LAB:LabTest", "Encounter", "MED:medications", "VitalSign", "PatientDetails"});
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(2007, Calendar.JANUARY, 1);
        Date first = c.getTime();
        c.clear();
        c.set(2009, Calendar.DECEMBER, 31);
        Date second = c.getTime();
        q.setFilters(new DateTimeFilter(new String[]{"Encounter"}, first, AbsoluteTimeGranularity.DAY, second, AbsoluteTimeGranularity.DAY, Side.FINISH, Side.FINISH));
        q.setQueryMode(QueryMode.UPDATE);
        getProtempaFactory().execute(q);
        
        File file = File.createTempFile("i2b2LoadNoDerivedVariablesLowerAndUpperDateBoundsTest", ".xml");
        try (FileOutputStream out = new FileOutputStream(file)) {
            getProtempaFactory().exportI2b2DataSchema(out);
            System.out.println("Dumped i2b2 data schema to " + file.getAbsolutePath());
        }
        
        setExpectedDataSet("/truth/i2b2LoadNoDerivedVariablesLowerAndUpperDateBoundsTestData.xml");
    }

}
