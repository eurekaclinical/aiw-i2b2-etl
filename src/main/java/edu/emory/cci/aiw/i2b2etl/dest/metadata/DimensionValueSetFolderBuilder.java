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
import edu.emory.cci.aiw.i2b2etl.dest.metadata.conceptid.InvalidConceptCodeException;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.conceptid.ConceptId;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.conceptid.PropDefConceptId;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.conceptid.SimpleConceptId;
import edu.emory.cci.aiw.i2b2etl.dest.config.Data;
import edu.emory.cci.aiw.i2b2etl.dest.config.DataSpec;
import edu.emory.cci.aiw.i2b2etl.dest.config.Settings;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.protempa.KnowledgeSource;
import org.protempa.KnowledgeSourceCache;
import org.protempa.KnowledgeSourceReadException;
import org.protempa.PropertyDefinition;
import org.protempa.PropositionDefinition;
import org.protempa.ReferenceDefinition;
import org.protempa.valueset.ValueSet;
import org.protempa.proposition.value.Value;
import org.protempa.valueset.ValueSetElement;

/**
 *
 * @author Andrew Post
 */
abstract class DimensionValueSetFolderBuilder implements OntologyBuilder {

    private final PropositionDefinition propDef;
    private final Data dataSection;
    private final Metadata metadata;
    private final String sourceSystemCode;
    private final String tableName;
    private final String factTableColumn;
    private final KnowledgeSourceCache cache;
    private final String childName;
    private final String dictVal;
    private final String columnName;
    private final Settings settings;

    DimensionValueSetFolderBuilder(KnowledgeSourceCache cache, Metadata metadata, String childName, String dictVal, String columnName) throws OntologyBuildException {
        assert cache != null : "cache cannot be null";
        assert metadata != null : "metadata cannot be null";
        assert childName != null : "childName cannot be null";
        assert columnName != null : "columnName cannot be null";

        this.cache = cache;
        this.dataSection = metadata.getDataSection();
        this.metadata = metadata;
        this.sourceSystemCode = metadata.getSourceSystemCode();
        this.factTableColumn = "patient_num";
        this.tableName = "patient_dimension";
        this.settings = metadata.getSettings();
        String propId = settings.getVisitDimension();
        if (propId != null) {
            try {
                this.propDef = cache.get(propId);
                if (this.propDef == null) {
                    throw new UnknownPropositionDefinitionException(propId);
                }
            } catch (UnknownPropositionDefinitionException ex) {
                throw new OntologyBuildException("Could not build descendants", ex);
            }
        } else {
            this.propDef = null;
        }
        this.childName = childName;
        this.dictVal = dictVal;
        this.columnName = columnName;
    }

    Settings getSettings() {
        return settings;
    }

    @Override
    public void build(Concept parent) throws OntologyBuildException {
        DataSpec dataSpec = getDataSection(dictVal);
        if (dataSpec != null && this.propDef != null) {
            ConceptId conceptId = SimpleConceptId.getInstance(childName, metadata);
            Concept concept = newQueryableConcept(conceptId, dataSpec.getConceptCodePrefix());
            concept.setColumnName(columnName);
            concept.setOperator(ConceptOperator.NOT_EQUAL);
            concept.setDisplayName(childName);
            concept.setAlreadyLoaded(parent.isAlreadyLoaded());
            if (dataSpec.getReferenceName() != null) {
                try {
                    addChildrenFromValueSets(this.propDef, dataSpec, concept, columnName);
                } catch (UnknownPropositionDefinitionException | KnowledgeSourceReadException | InvalidConceptCodeException ex) {
                    throw new OntologyBuildException("Could not build descendants", ex);
                }
            }
            concept.setDimCode("''");
            parent.add(concept);
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
            PropositionDefinition propositionDef = this.cache.get(propId);
            if (propositionDef == null) {
                throw new UnknownPropositionDefinitionException(propId);
            }
            PropertyDefinition propertyDef = propositionDef.propertyDefinition(dataSpec.getPropertyName());
            if (propertyDef != null) {
                String valueSetId = propertyDef.getValueSetId();
                if (valueSetId != null) {
                    ValueSet valueSet
                            = this.cache.getValueSet(valueSetId);
                    ValueSetElement[] valueSetElements = valueSet.getValueSetElements();
                    for (ValueSetElement valueSetElement : valueSetElements) {
                        Value valueSetElementVal = valueSetElement.getValue();
                        PropDefConceptId conceptId = PropDefConceptId.getInstance(propId, dataSpec.getPropertyName(), valueSetElementVal, metadata);
                        Concept childConcept = newQueryableConcept(conceptId, dataSpec.getConceptCodePrefix());
                        childConcept.setDisplayName(valueSetElement.getDisplayName());
                        childConcept.setColumnName(columnName);
                        childConcept.setDimCode(valueSetElementVal != null ? valueSetElementVal.getFormatted() : "");
                        childConcept.setOperator(ConceptOperator.EQUAL);
                        childConcept.setAlreadyLoaded(concept.isAlreadyLoaded());
                        concept.add(childConcept);
                    }
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
