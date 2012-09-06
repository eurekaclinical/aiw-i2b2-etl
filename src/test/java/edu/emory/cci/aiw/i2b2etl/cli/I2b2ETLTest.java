package edu.emory.cci.aiw.i2b2etl.cli;

import edu.emory.cci.aiw.i2b2etl.ProtempaFactory;
import edu.emory.cci.aiw.i2b2etl.I2B2QueryResultsHandler;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import org.junit.*;
import org.protempa.EventDefinition;
import org.protempa.FinderException;
import org.protempa.HighLevelAbstractionDefinition;
import org.protempa.Offsets;
import org.protempa.PrimitiveParameterDefinition;
import org.protempa.PropositionDefinition;
import org.protempa.Protempa;
import org.protempa.ProtempaStartupException;
import org.protempa.TemporalExtendedPropositionDefinition;
import org.protempa.backend.BackendProviderSpecLoaderException;
import org.protempa.backend.ConfigurationsLoadException;
import org.protempa.backend.InvalidConfigurationException;
import org.protempa.proposition.interval.Relation;
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
            
            EventDefinition ed = new EventDefinition("MyDiagnosis");
            ed.setDisplayName("My Diagnosis");
            ed.setInverseIsA("ICD9:907.1");
            
            PrimitiveParameterDefinition pd = 
                    new PrimitiveParameterDefinition("MyLabTest");
            pd.setDisplayName("My Lab Test");
            pd.setInverseIsA("LAB:8007694");
            
            HighLevelAbstractionDefinition hd =
                    new HighLevelAbstractionDefinition("MyTemporalPattern");
            hd.setDisplayName("My Temporal Pattern");
            TemporalExtendedPropositionDefinition td1 =
                    new TemporalExtendedPropositionDefinition(ed.getId());
            TemporalExtendedPropositionDefinition td2 =
                    new TemporalExtendedPropositionDefinition(pd.getId());
            hd.add(td1);
            hd.add(td2);
            Relation rel = new Relation();
            hd.setRelation(td1, td2, rel);
            hd.setTemporalOffset(new Offsets());
            
            q.setPropositionDefinitions(
                    new PropositionDefinition[]{ed, pd, hd});
            q.setId("i2b2 ETL Test Query");
            
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
