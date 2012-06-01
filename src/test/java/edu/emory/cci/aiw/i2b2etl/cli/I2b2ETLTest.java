package edu.emory.cci.aiw.i2b2etl.cli;

import edu.emory.cci.aiw.i2b2etl.I2B2QueryResultsHandler;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.arp.javautil.io.IOUtil;
import org.junit.*;
import org.protempa.FinderException;
import org.protempa.Protempa;
import org.protempa.ProtempaStartupException;
import org.protempa.query.DefaultQueryBuilder;
import org.protempa.query.Query;
import org.protempa.query.QueryBuildException;
import org.protempa.query.handler.QueryResultsHandler;
import org.protempa.query.handler.test.DataProviderException;
import org.protempa.query.handler.test.DatabasePopulator;

/**
 * Integration tests for the i2b2 ETL. This assumes that there is an i2b2
 * instance somewhere to use for testing. Specify where it is in your
 * settings.xml file (future). 
 * 
 * @author Andrew Post
 */
public class I2b2ETLTest {

    private static final String[] PROP_IDS = {
        "Patient",
        "PatientAll",
        "Encounter",
        "AttendingPhysician",
        "LAB:LabTest",
        "VitalSign",
        "ICD9:Procedures",
        "ICD9:Diagnoses",
        "MED:medications",
        "LAB:LabTest",
        "CPTCode"
    };

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
    public static void setUp() throws ProtempaStartupException, IOException, 
            QueryBuildException, FinderException, DataProviderException, 
            SQLException, URISyntaxException {
        new DatabasePopulator().doPopulate();
        
        File configPath = 
                IOUtil.resourceToFile("/protempa-config/i2b2etltest", 
                "i2b2etltest", null);
        System.setProperty("protempa.inicommonsconfigurations.pathname",
                configPath.getParent());
        // force the use of the H2 driver so we don't bother trying to load
        // others
        System.setProperty("protempa.dsb.relationaldatabase.sqlgenerator",
                "org.protempa.bp.commons.dsb.relationaldb.H2SQLGenerator");
        Protempa protempa = Protempa.newInstance(configPath.getName());
        try {
            File confXML = IOUtil.resourceToFile("/conf.xml", "conf", null);
            DefaultQueryBuilder q = new DefaultQueryBuilder();
            q.setPropIds(PROP_IDS);
            Query query = protempa.buildQuery(q);
            QueryResultsHandler tdqrh = 
                    new I2B2QueryResultsHandler(confXML.getAbsolutePath());
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
