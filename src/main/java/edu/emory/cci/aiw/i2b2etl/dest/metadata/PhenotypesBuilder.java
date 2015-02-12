package edu.emory.cci.aiw.i2b2etl.dest.metadata;

/*
 * #%L
 * AIW i2b2 ETL
 * %%
 * Copyright (C) 2012 - 2015 Emory University
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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.protempa.KnowledgeSource;
import org.protempa.KnowledgeSourceReadException;
import org.protempa.PropositionDefinition;

/**
 *
 * @author Andrew Post
 */
class PhenotypesBuilder extends PropositionConceptTreeBuilder {

    private final String sourceSystemCode;

    PhenotypesBuilder(Map<String, PropositionDefinition> cache, KnowledgeSource knowledgeSource, Metadata metadata) throws KnowledgeSourceReadException, UnknownPropositionDefinitionException {
        super(cache, knowledgeSource, phenotypePropIds(metadata), null, null, null, false, metadata);
        this.sourceSystemCode = metadata.getSourceSystemCode();
    }

    @Override
    public void build(Concept parent) throws OntologyBuildException {
        if (getMetadata().getPhenotypeDefinitions().length > 0) {
            Metadata metadata = getMetadata();
            ConceptId conceptId
                    = SimpleConceptId.getInstance("Phenotypes", metadata);
            Concept concept = metadata.getFromIdCache(conceptId);
            if (concept == null) {
                try {
                    concept
                            = new Concept(conceptId, null, metadata);
                } catch (InvalidConceptCodeException ex) {
                    throw new OntologyBuildException(ex);
                }
                concept.setSourceSystemCode(this.sourceSystemCode);
                concept.setDisplayName("Phenotypes");
                concept.setDataType(DataType.TEXT);
                concept.setAlreadyLoaded(false);
                metadata.addToIdCache(concept);
                parent.add(concept);
            }
            super.build(concept);
        }
    }
    
    private static String[] phenotypePropIds(Metadata metadata) {
        Set<String> inPropIds = new HashSet<>();
        for (PropositionDefinition phenotypeDef : metadata.getPhenotypeDefinitions()) {
            inPropIds.add(phenotypeDef.getId());
        }
        return inPropIds.toArray(new String[inPropIds.size()]);
    }

}
