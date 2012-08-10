package edu.emory.cci.aiw.i2b2etl.cli;

import edu.emory.cci.aiw.i2b2etl.I2B2QueryResultsHandler;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import org.junit.*;
import org.protempa.FinderException;
import org.protempa.Protempa;
import org.protempa.ProtempaStartupException;
import org.protempa.backend.BackendProviderSpecLoaderException;
import org.protempa.backend.ConfigurationsLoadException;
import org.protempa.backend.InvalidConfigurationException;
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
            SQLException, URISyntaxException, 
            BackendProviderSpecLoaderException, ConfigurationsLoadException, 
            InvalidConfigurationException {
        new DatabasePopulator().doPopulate();
        Protempa protempa = new ProtempaFactory().newInstance();
        try {
            File confXML = new I2b2ETLConfAsFile().getFile();
            DefaultQueryBuilder q = new DefaultQueryBuilder();
            Query query = protempa.buildQuery(q);
            QueryResultsHandler tdqrh = new I2B2QueryResultsHandler(confXML);
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
