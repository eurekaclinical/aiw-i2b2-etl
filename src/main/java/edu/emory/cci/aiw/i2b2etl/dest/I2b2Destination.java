package edu.emory.cci.aiw.i2b2etl.dest;

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
import edu.emory.cci.aiw.i2b2etl.dest.config.Concepts;
import edu.emory.cci.aiw.i2b2etl.dest.config.Configuration;
import edu.emory.cci.aiw.i2b2etl.dest.config.Data;
import edu.emory.cci.aiw.i2b2etl.dest.config.DataSpec;
import edu.emory.cci.aiw.i2b2etl.dest.config.FolderSpec;
import edu.emory.cci.aiw.i2b2etl.dest.config.Settings;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.ArrayUtils;
import org.protempa.DataSource;
import org.protempa.KnowledgeSource;
import org.protempa.KnowledgeSourceReadException;
import org.protempa.PropositionDefinition;
import org.protempa.ReferenceDefinition;
import org.protempa.dest.AbstractDestination;
import org.protempa.dest.GetSupportedPropositionIdsException;
import org.protempa.dest.QueryResultsHandler;
import org.protempa.dest.QueryResultsHandlerInitException;
import org.protempa.dest.Statistics;
import org.protempa.dest.StatisticsException;
import org.protempa.query.Query;

/**
 *
 * @author Andrew Post
 */
public final class I2b2Destination extends AbstractDestination {

    private final Configuration config;
    private final boolean insertSupportedPropositionIds;

    /**
     * Creates a new query results handler that will use the provided
     * configuration file. It is the same as calling the two-argument
     * constructor with <code>inferPropositionIdsNeeded</code> set to
     * <code>false</code>.
     *
     * @param confXML an i2b2 query results handler configuration file. Cannot
     * be <code>null</code>.
     */
    public I2b2Destination(Configuration config) {
        this(config, false);
    }

    /**
     * Creates a new query results handler that will use the provided
     * configuration file. This constructor, through the
     * <code>inferPropositionIdsNeeded</code> parameter, lets you control
     * whether proposition ids to be returned from the Protempa processing run
     * should be inferred from the i2b2 configuration file.
     *
     * @param confXML an i2b2 query results handler configuration file. Cannot
     * be <code>null</code>.
     * @param inferPropositionIdsNeeded <code>true</code> if proposition ids to
     * be returned from the Protempa processing run should include all of those
     * specified in the i2b2 configuration file, <code>false</code> if the
     * proposition ids returned should be only those specified in the Protempa
     * {@link Query}.
     */
    public I2b2Destination(Configuration config, boolean inferSupportedPropositionIds) {
        if (config == null) {
            throw new IllegalArgumentException("config cannot be null");
        }
        this.config = config;
        this.insertSupportedPropositionIds = inferSupportedPropositionIds;
    }

    @Override
    public QueryResultsHandler getQueryResultsHandler(Query query, DataSource dataSource, KnowledgeSource knowledgeSource) throws QueryResultsHandlerInitException {
        return new I2b2QueryResultsHandler(query, dataSource, knowledgeSource, this.config);
    }

    @Override
    public boolean isGetStatisticsSupported() {
        return true;
    }

    @Override
    public Statistics getStatistics() throws StatisticsException {
        return new I2b2Statistics(this.config);
    }

    @Override
    public String[] getSupportedPropositionIds(DataSource dataSource, KnowledgeSource knowledgeSource) throws GetSupportedPropositionIdsException {
        try {
            return readPropIdsFromKnowledgeSource(dataSource, knowledgeSource);
        } catch (KnowledgeSourceReadException ex) {
            throw new GetSupportedPropositionIdsException(ex);
        }
    }

    private String[] readPropIdsFromKnowledgeSource(DataSource dataSource, KnowledgeSource knowledgeSource) throws KnowledgeSourceReadException {
        if (this.insertSupportedPropositionIds) {
            Set<String> result = new HashSet<>();
            Settings settings = this.config.getSettings();
            Data data = this.config.getData();
            Concepts concepts = this.config.getConcepts();
            String visitPropId = settings.getVisitDimension();
            if (visitPropId != null) {
                result.add(visitPropId);
                PropositionDefinition visitProp = knowledgeSource.readPropositionDefinition(visitPropId);
                if (visitProp == null) {
                    throw new KnowledgeSourceReadException("Invalid visit proposition id: " + visitPropId);
                }
                for (DataSpec dataSpec : data.getAll()) {
                    if (dataSpec.getReferenceName() != null) {
                        ReferenceDefinition refDef = visitProp.referenceDefinition(dataSpec.getReferenceName());
                        if (refDef == null) {
                            throw new KnowledgeSourceReadException("missing reference " + dataSpec.getReferenceName() + " for proposition definition " + visitPropId);
                        }
                        org.arp.javautil.arrays.Arrays.addAll(result, refDef.getPropositionIds());
                    }
                }
                for (FolderSpec folderSpec : concepts.getFolderSpecs()) {
                    for (String proposition : folderSpec.getPropositions()) {
                        result.add(proposition);
                    }
                }
            }
            return result.toArray(new String[result.size()]);
        } else {
            return ArrayUtils.EMPTY_STRING_ARRAY;
        }
    }
    
}
