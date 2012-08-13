package edu.emory.cci.aiw.i2b2etl.table;

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
