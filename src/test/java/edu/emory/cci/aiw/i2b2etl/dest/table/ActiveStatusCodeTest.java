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
package edu.emory.cci.aiw.i2b2etl.dest.table;

import edu.emory.cci.aiw.i2b2etl.dest.table.ActiveStatusCode;
import edu.stanford.smi.protege.util.Assert;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import org.junit.Test;

/**
 *
 * @author Andrew Post
 */
public class ActiveStatusCodeTest {
    
    @Test
    public void testActive() throws ParseException {
        doTest(false, "1/1/12", null, ActiveStatusCode.ACTIVE);
    }
    
    @Test
    public void testNoDates1() throws ParseException {
        doTest(false, null, null, ActiveStatusCode.NO_DATES);
    }
    
    @Test
    public void testNoDates2() throws ParseException {
        doTest(true, null, null, ActiveStatusCode.NO_DATES);
    }
    
    @Test
    public void testPrelim() throws ParseException {
        doTest(false, "1/1/12", "1/10/12", ActiveStatusCode.PRELIMINARY);
    }
    
    @Test
    public void testFinal() throws ParseException {
        doTest(true, "1/1/12", "1/10/12", ActiveStatusCode.FINAL);
    }
    
    @Test
    public void testNoStartDate() throws ParseException {
        doTest(true, null, "1/10/12", ActiveStatusCode.FINAL);
    }
    
    private static void doTest(boolean bFinal, String startDateStr, 
            String endDateStr, ActiveStatusCode expected) 
            throws ParseException {
        DateFormat df = DateFormat.getDateInstance(DateFormat.SHORT);
        Date startDate = startDateStr != null ? df.parse(startDateStr) : null;
        Date endDate = endDateStr != null ? df.parse(endDateStr) : null;
        
        ActiveStatusCode actual = 
                ActiveStatusCode.getInstance(bFinal, startDate, endDate);
        Assert.assertEquals(expected, actual);
    }
}
