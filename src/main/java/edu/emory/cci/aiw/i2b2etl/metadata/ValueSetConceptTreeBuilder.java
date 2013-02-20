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
package edu.emory.cci.aiw.i2b2etl.metadata;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.protempa.KnowledgeSource;
import org.protempa.PropertyDefinition;
import org.protempa.PropositionDefinition;
import org.protempa.ValueSet;
import org.protempa.ValueSet.ValueSetElement;

import org.protempa.*;

class ValueSetConceptTreeBuilder {

    private KnowledgeSource knowledgeSource;
    private String propertyName;
    private String conceptCodePrefix;
    private final PropositionDefinition[] rootPropositionDefinitions;
    private final Metadata metadata;

    ValueSetConceptTreeBuilder(KnowledgeSource knowledgeSource, String[] propIds, String property,
            String conceptCodePrefix, Metadata metadata) throws KnowledgeSourceReadException, UnknownPropositionDefinitionException {
        assert knowledgeSource != null : "knowledgeSource cannot be null";
        ProtempaUtil.checkArray(propIds, "propIds");
        assert metadata != null : "metadata cannot be null";
        this.knowledgeSource = knowledgeSource;
        this.propertyName = property;
        this.conceptCodePrefix = conceptCodePrefix;
        this.rootPropositionDefinitions =
                new PropositionDefinition[propIds.length];
        for (int i = 0; i < propIds.length; i++) {
            this.rootPropositionDefinitions[i] =
                    knowledgeSource.readPropositionDefinition(propIds[i]);
            if (this.rootPropositionDefinitions[i] == null) {
                throw new UnknownPropositionDefinitionException(propIds[i]);
            }
        }
        this.metadata = metadata;
    }

    Concept[] build() throws OntologyBuildException {
        try {
            Concept[] result =
                    new Concept[this.rootPropositionDefinitions.length];
            for (int i = 0; i < this.rootPropositionDefinitions.length; i++) {
                PropositionDefinition propDefinition = this.rootPropositionDefinitions[i];
                Concept root = new Concept(ConceptId.getInstance(propDefinition.getId(), this.propertyName, this.metadata), this.conceptCodePrefix, this.metadata);
                root.setSourceSystemCode(MetadataUtil.toSourceSystemCode(I2B2QueryResultsHandlerSourceId.getInstance().getStringRepresentation()));
                root.setDataType(DataType.TEXT);
                PropertyDefinition propertyDef =
                        propDefinition.propertyDefinition(propertyName);
                ValueSet valueSet =
                        knowledgeSource.readValueSet(propertyDef.getValueSetId());
                ValueSetElement[] vse = valueSet.getValueSetElements();
                for (ValueSetElement e : vse) {
                    Concept concept = new Concept(ConceptId.getInstance(
                            propDefinition.getId(), this.propertyName, e.getValue(), this.metadata), this.conceptCodePrefix, this.metadata);
                    concept.setSourceSystemCode(MetadataUtil.toSourceSystemCode(valueSet.getSourceId().getStringRepresentation()));
                    concept.setInDataSource(true);
                    concept.setDisplayName(e.getDisplayName());
                    concept.setDataType(DataType.TEXT);
                    this.metadata.addToIdCache(concept);
                    root.add(concept);
                }
                result[i] = root;
            }
            return result;
        } catch (KnowledgeSourceReadException ex) {
            throw new OntologyBuildException("Could not build value set concept tree", ex);
        } catch (InvalidConceptCodeException ex) {
            throw new OntologyBuildException("Could not build value set concept tree", ex);
        }
    }
}
