package edu.emory.cci.aiw.i2b2etl.metadata;

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

import edu.emory.cci.aiw.i2b2etl.configuration.Data;
import edu.emory.cci.aiw.i2b2etl.configuration.DataSpec;
import edu.emory.cci.aiw.i2b2etl.configuration.Settings;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.protempa.KnowledgeSource;
import org.protempa.KnowledgeSourceReadException;
import org.protempa.PropertyDefinition;
import org.protempa.PropositionDefinition;
import org.protempa.ReferenceDefinition;
import org.protempa.ValueSet;
import org.protempa.proposition.value.Value;

/**
 *
 * @author arpost
 */
class DimensionValueSetFolderBuilder {

    private final PropositionDefinition propDef;
    private final Concept root;
    private final KnowledgeSource knowledgeSource;
    private final Data dataSection;
    private final Metadata metadata;
    private final String sourceSystemCode;
    private final String tableName;
    private final String factTableColumn;
    private final Map<String, PropositionDefinition> cache;

    DimensionValueSetFolderBuilder(Concept root, KnowledgeSource knowledgeSource, Map<String, PropositionDefinition> cache, Settings dictSection, Data dataSection, Metadata metadata, String sourceSystemCode, String factTableColumn, String tableName) throws OntologyBuildException {
        assert cache != null : "cache cannot be null";
        this.cache = cache;
        this.root = root;
        this.knowledgeSource = knowledgeSource;
        this.dataSection = dataSection;
        this.metadata = metadata;
        this.sourceSystemCode = sourceSystemCode;
        this.factTableColumn = factTableColumn;
        this.tableName = tableName;
        String propId = dictSection.getVisitDimension();
        try {
            this.propDef = cache.get(propId);
            if (this.propDef == null) {
                throw new UnknownPropositionDefinitionException(propId);
            }
        } catch (UnknownPropositionDefinitionException ex) {
            throw new OntologyBuildException("Could not build descendants", ex);
        }

    }

    void build(String childName, String dictVal, String columnName) throws OntologyBuildException {
        DataSpec dataSpec = getDataSection(dictVal);
        if (dataSpec != null) {
            //ConceptId conceptId = ConceptId.getInstance(null, dataSpec.propertyName, null, metadata);
            ConceptId conceptId = ConceptId.getInstance(childName, metadata);
            Concept concept = newQueryableConcept(conceptId, dataSpec.getConceptCodePrefix());
            concept.setColumnName(columnName);
            concept.setOperator(ConceptOperator.IN);
            concept.setDisplayName(childName);
            List<String> inClause = new ArrayList<>();
            if (dataSpec.getReferenceName() != null) {
                try {
                    addChildrenFromValueSets(this.propDef, dataSpec, concept, columnName);
                    Enumeration children = concept.children();
                    while (children.hasMoreElements()) {
                        Concept child = (Concept) children.nextElement();
                        inClause.add(child.getDimCode());
                    }
                } catch (UnknownPropositionDefinitionException | KnowledgeSourceReadException | InvalidConceptCodeException ex) {
                    throw new OntologyBuildException("Could not build descendants", ex);
                }
            }
            concept.setDimCode("'" + StringUtils.join(inClause, "','") + "'");
            this.root.add(concept);
        }
    }

    private DataSpec getDataSection(String dictVal) {
        if (dictVal != null) {
            return this.dataSection.get(dictVal);
        } else {
            return null;
        }
    }

    private void addChildrenFromValueSets(PropositionDefinition propDef,
            DataSpec dataSpec, Concept concept, String columnName) throws OntologyBuildException,
            UnsupportedOperationException, KnowledgeSourceReadException,
            InvalidConceptCodeException, UnknownPropositionDefinitionException {
        ReferenceDefinition refDef = propDef.referenceDefinition(dataSpec.getReferenceName());
        String[] propIds = refDef.getPropositionIds();
        for (String propId : propIds) {
            PropositionDefinition genderPropositionDef = this.cache.get(propId);
            if (genderPropositionDef == null) {
                throw new UnknownPropositionDefinitionException(propId);
            }
            PropertyDefinition genderPropertyDef = genderPropositionDef.propertyDefinition(dataSpec.getPropertyName());
            if (genderPropertyDef != null) {
                String valueSetId = genderPropertyDef.getValueSetId();
                if (valueSetId == null) {
                    throw new UnsupportedOperationException("We don't support non-enumerated property values for demographics yet!");
                }
                ValueSet valueSet
                        = this.knowledgeSource.readValueSet(valueSetId);
                ValueSet.ValueSetElement[] valueSetElements = valueSet.getValueSetElements();
                for (ValueSet.ValueSetElement valueSetElement : valueSetElements) {
                    Value valueSetElementVal = valueSetElement.getValue();
                    ConceptId conceptId = ConceptId.getInstance(propId, dataSpec.getPropertyName(), valueSetElementVal, metadata);
                    Concept childConcept = newQueryableConcept(conceptId, dataSpec.getConceptCodePrefix());
                    childConcept.setDisplayName(valueSetElement.getDisplayName());
                    childConcept.setColumnName(columnName);
                    childConcept.setDimCode(valueSetElementVal != null ? valueSetElementVal.getFormatted() : "");
                    childConcept.setOperator(ConceptOperator.EQUAL);
                    concept.add(childConcept);
                }
            }
        }
    }
    
    private Concept newQueryableConcept(ConceptId conceptId, String conceptCodePrefix) throws OntologyBuildException {
        Concept concept = this.metadata.newConcept(conceptId, conceptCodePrefix, this.sourceSystemCode);
        concept.setFactTableColumn(this.factTableColumn);
        concept.setTableName(this.tableName);
        return concept;
    }
    
}
