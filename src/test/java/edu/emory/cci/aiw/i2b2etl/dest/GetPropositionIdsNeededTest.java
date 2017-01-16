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

import static edu.emory.cci.aiw.i2b2etl.AbstractDataTest.getConfigFactory;
import edu.emory.cci.aiw.i2b2etl.I2b2DestinationFactory;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;
import org.protempa.Protempa;
import org.protempa.SourceFactory;
import org.protempa.dest.Destination;
import org.protempa.dest.QueryResultsHandler;
import org.protempa.query.DefaultQueryBuilder;
import org.protempa.query.Query;
import org.protempa.query.QueryBuilder;
import static org.junit.Assert.assertArrayEquals;

/**
 *
 * @author Andrew Post
 */
public class GetPropositionIdsNeededTest extends AbstractI2b2DestTest {

    @Test
    public void testPropositionIds() throws Exception {
        try (Protempa protempa = getProtempaFactory().newInstance()) {
            String[] supportedPropositionIds = protempa.getSupportedPropositionIds(new I2b2DestinationFactory("/conf.xml").getInstance(true));
            java.io.File createTempFile = java.io.File.createTempFile("foo", "bar");
            try (java.io.PrintWriter w = new java.io.PrintWriter(new java.io.FileWriter(createTempFile))) {
                for (String propId : supportedPropositionIds) {
                    w.println(propId);
                }
            }
            System.err.println("Written expected to " + createTempFile.getAbsolutePath());
            assertEqualsStrings("/truth/get-proposition-ids-needed-test-file", supportedPropositionIds);
        }
    }
    
    @Test
    public void testPropositionIdsDestControlsPropIdsRetained() throws Exception {
        SourceFactory sourceFactory = new SourceFactory(getConfigFactory().getProtempaConfiguration());
        try (Protempa protempa = Protempa.newInstance(sourceFactory)) {
            Destination dest = new I2b2DestinationFactory("/conf.xml").getInstance();
            QueryBuilder queryBuilder = new DefaultQueryBuilder();
            Query query = protempa.buildQuery(queryBuilder);
            try (QueryResultsHandler qrh = dest.getQueryResultsHandler(query, protempa.getDataSource(), protempa.getKnowledgeSource())) {
                assertArrayEquals(ArrayUtils.EMPTY_STRING_ARRAY, protempa.getSupportedPropositionIds(dest));
            }
        }

    }

}
