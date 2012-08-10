package edu.emory.cci.aiw.i2b2etl.cli;

import edu.emory.cci.aiw.i2b2etl.I2B2QueryResultsHandler;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang.ArrayUtils;
import org.arp.javautil.arrays.Arrays;
import org.arp.javautil.io.WithBufferedReaderByLine;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.protempa.KnowledgeSource;
import org.protempa.KnowledgeSourceReadException;
import org.protempa.Protempa;
import org.protempa.ProtempaStartupException;
import org.protempa.backend.BackendProviderSpecLoaderException;
import org.protempa.backend.ConfigurationsLoadException;
import org.protempa.backend.InvalidConfigurationException;
import org.protempa.query.handler.QueryResultsHandlerInitException;

/**
 *
 * @author Andrew Post
 */
public class GetPropositionIdsNeededTest {

    private static Protempa protempa;
    private static File confXML;
    private static Set<String> expectedPropIds;

    @BeforeClass
    public static void setUp() throws IOException,
            BackendProviderSpecLoaderException, ConfigurationsLoadException,
            InvalidConfigurationException, ProtempaStartupException {
        protempa = new ProtempaFactory().newInstance();
        confXML = new I2b2ETLConfAsFile().getFile();
        expectedPropIds = expectedPropIds();
    }

    @Test
    public void testPropositionIds() throws
            QueryResultsHandlerInitException, KnowledgeSourceReadException {
        KnowledgeSource knowledgeSource = protempa.getKnowledgeSource();
        I2B2QueryResultsHandler qrh = new I2B2QueryResultsHandler(confXML);
        qrh.init(knowledgeSource);
        String[] actualPropIds = qrh.getPropositionIdsNeeded();
        Assert.assertEquals(expectedPropIds, Arrays.asSet(actualPropIds));
    }

    @AfterClass
    public static void tearDown() {
        if (protempa != null) {
            protempa.close();
        }
    }
    
    private static Set<String> expectedPropIds() throws IOException {
        final Set<String> result = new HashSet<String>();
        InputStream is = 
                GetPropositionIdsNeededTest.class.getResourceAsStream(
                "/get-proposition-ids-needed-test-file");
        new WithBufferedReaderByLine(is) {

            @Override
            public void readLine(String line) {
                line = line.trim();
                if (line.length() != 0) {
                    result.add(line);
                }
            }  
        }.execute();
        
        return result;
    }
}
