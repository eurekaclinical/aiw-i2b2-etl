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


import org.junit.Test;
import org.protempa.Protempa;
import org.protempa.query.DefaultQueryBuilder;
import org.protempa.query.Query;
import org.protempa.query.QueryBuilder;
import org.protempa.dest.QueryResultsHandler;

/**
 *
 * @author Andrew Post
 */
public class GetPropositionIdsNeededTest extends AbstractI2b2DestTest {

    @Test
    public void testPropositionIds() throws Exception {
        QueryBuilder queryBuilder = new DefaultQueryBuilder();
        try (Protempa protempa = getProtempaFactory().newInstance()) {
            Query query = protempa.buildQuery(queryBuilder);
            I2b2Destination destination = getI2b2DestFactory().getInstance();
            try (QueryResultsHandler qrh = 
                    destination.getQueryResultsHandler(query, protempa.getDataSource(), protempa.getKnowledgeSource())) {
                assertEqualsStrings("/truth/get-proposition-ids-needed-test-file", qrh.getPropositionIdsNeeded());
            }
        }
    }
}
