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

import static edu.emory.cci.aiw.i2b2etl.AbstractTest.getConfigFactory;
import edu.emory.cci.aiw.i2b2etl.I2b2DestinationFactory;
import org.apache.commons.lang3.ArrayUtils;
import static org.junit.Assert.assertArrayEquals;
import org.junit.Test;
import org.protempa.Protempa;
import org.protempa.SourceFactory;
import org.protempa.dest.Destination;
import org.protempa.dest.QueryResultsHandler;
import org.protempa.query.DefaultQueryBuilder;
import org.protempa.query.Query;
import org.protempa.query.QueryBuilder;

/**
 *
 * @author Andrew Post
 */
public class GetPropositionIdsNeededTest extends AbstractI2b2DestTest {

    @Test
    public void testPropositionIds() throws Exception {
        try (Protempa protempa = getProtempaFactory().newInstance()) {
            assertEqualsStrings("/truth/get-proposition-ids-needed-test-file", protempa.getSupportedPropositionIds(new I2b2DestinationFactory().getInstance(true)));
        }
    }
    
    @Test
    public void testPropositionIdsDestControlsPropIdsRetained() throws Exception {
        SourceFactory sourceFactory = new SourceFactory(getConfigFactory().getProtempaConfiguration());
        try (Protempa protempa = Protempa.newInstance(sourceFactory)) {
            Destination dest = new I2b2DestinationFactory().getInstance();
            QueryBuilder queryBuilder = new DefaultQueryBuilder();
            Query query = protempa.buildQuery(queryBuilder);
            try (QueryResultsHandler qrh = dest.getQueryResultsHandler(query, protempa.getDataSource(), protempa.getKnowledgeSource())) {
                assertArrayEquals(ArrayUtils.EMPTY_STRING_ARRAY, protempa.getSupportedPropositionIds(dest));
            }
        }

    }

}
