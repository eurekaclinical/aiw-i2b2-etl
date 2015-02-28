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
package edu.emory.cci.aiw.i2b2etl.dest.metadata;

import java.util.Map;
import org.protempa.KnowledgeSource;
import org.protempa.KnowledgeSourceReadException;
import org.protempa.PropertyDefinition;
import org.protempa.PropositionDefinition;
import org.protempa.ProtempaUtil;
import org.protempa.ValueSet;
import org.protempa.ValueSet.ValueSetElement;

class ValueSetConceptTreeBuilder implements OntologyBuilder {

    private final KnowledgeSource knowledgeSource;
    private final String propertyName;
    private final String conceptCodePrefix;
    private final PropositionDefinition[] rootPropositionDefinitions;
    private final Metadata metadata;

    ValueSetConceptTreeBuilder(KnowledgeSource knowledgeSource, Map<String, PropositionDefinition> cache, String[] propIds, String property,
            String conceptCodePrefix, Metadata metadata) throws KnowledgeSourceReadException, UnknownPropositionDefinitionException {
        assert knowledgeSource != null : "knowledgeSource cannot be null";
        ProtempaUtil.checkArray(propIds, "propIds");
        assert metadata != null : "metadata cannot be null";
        this.knowledgeSource = knowledgeSource;
        this.propertyName = property;
        this.conceptCodePrefix = conceptCodePrefix;
        this.rootPropositionDefinitions
                = new PropositionDefinition[propIds.length];
        for (int i = 0; i < propIds.length; i++) {
            this.rootPropositionDefinitions[i]
                    = cache.get(propIds[i]);
            if (this.rootPropositionDefinitions[i] == null) {
                throw new UnknownPropositionDefinitionException(propIds[i]);
            }
        }
        this.metadata = metadata;
    }
    
    @Override
    public void build(Concept concept) throws OntologyBuildException {
        try {
            for (PropositionDefinition propDefinition : this.rootPropositionDefinitions) {
                ConceptId rootConceptId = PropDefConceptId.getInstance(propDefinition.getId(), this.propertyName, this.metadata);
                Concept root = new Concept(rootConceptId, this.conceptCodePrefix, this.metadata);
                root.setSourceSystemCode(propDefinition.getSourceId().getStringRepresentation());
                root.setDataType(DataType.TEXT);
                root.setDisplayName(this.propertyName);
                if (concept != null) {
                    root.setAlreadyLoaded(concept.isAlreadyLoaded());
                }
                this.metadata.addToIdCache(root);
                PropertyDefinition propertyDef
                        = propDefinition.propertyDefinition(propertyName);
                ValueSet valueSet
                        = knowledgeSource.readValueSet(propertyDef.getValueSetId());
                ValueSetElement[] vse = valueSet.getValueSetElements();
                for (ValueSetElement e : vse) {
                    Concept vsEltConcept = new Concept(PropDefConceptId.getInstance(
                            propDefinition.getId(), this.propertyName, e.getValue(), this.metadata), this.conceptCodePrefix, this.metadata);
                    vsEltConcept.setSourceSystemCode(valueSet.getSourceId().getStringRepresentation());
                    vsEltConcept.setInDataSource(true);
                    vsEltConcept.setDisplayName(e.getDisplayName());
                    vsEltConcept.setDataType(DataType.TEXT);
                    this.metadata.addToIdCache(vsEltConcept);
                    vsEltConcept.setAlreadyLoaded(root.isAlreadyLoaded());
                    root.add(vsEltConcept);
                }
                if (concept != null) {
                    concept.add(root);
                }
            }
        } catch (KnowledgeSourceReadException | InvalidConceptCodeException ex) {
            throw new OntologyBuildException("Could not build value set concept tree", ex);
        }
    }
}
