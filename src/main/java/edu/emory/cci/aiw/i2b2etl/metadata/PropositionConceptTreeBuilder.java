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

import edu.emory.cci.aiw.i2b2etl.configuration.ModifierSpec;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.protempa.KnowledgeSource;
import org.protempa.KnowledgeSourceReadException;
import org.protempa.ParameterDefinition;
import org.protempa.PropertyDefinition;
import org.protempa.PropositionDefinition;
import org.protempa.ProtempaUtil;
import org.protempa.ValueSet;
import org.protempa.ValueSet.ValueSetElement;
import org.protempa.proposition.value.ValueType;

final class PropositionConceptTreeBuilder {
    
    private static final ModifierSpec[] EMPTY_MODIFIER_SPEC_ARRAY = new ModifierSpec[0];

    private final SimpleDateFormat valueMetadataCreateDateTimeFormat;
    private final KnowledgeSource knowledgeSource;
    private final PropositionDefinition[] rootPropositionDefinitions;
    private final String conceptCode;
    private final Metadata metadata;
    private final ValueTypeCode valueTypeCode;
    private final String createDate;
    private final ModifierSpec[] modifiers;

    PropositionConceptTreeBuilder(KnowledgeSource knowledgeSource,
            String[] propIds, String conceptCode, ValueTypeCode valueTypeCode,
            ModifierSpec[] modifiers, Metadata metadata)
            throws KnowledgeSourceReadException,
            UnknownPropositionDefinitionException {
        assert knowledgeSource != null : "knowledgeSource cannot be null";
        ProtempaUtil.checkArray(propIds, "propIds");
        assert metadata != null : "metadata cannot be null";

        this.knowledgeSource = knowledgeSource;
        this.rootPropositionDefinitions =
                new PropositionDefinition[propIds.length];
        for (int i = 0; i < propIds.length; i++) {
            this.rootPropositionDefinitions[i] =
                    readPropositionDefinition(propIds[i]);
        }
        this.conceptCode = conceptCode;
        this.metadata = metadata;
        this.valueTypeCode = valueTypeCode;
        this.valueMetadataCreateDateTimeFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        this.createDate = this.valueMetadataCreateDateTimeFormat.format(new Date());
        if (modifiers != null) {
            this.modifiers = modifiers.clone();
        } else {
            this.modifiers = EMPTY_MODIFIER_SPEC_ARRAY;
        }
    }

    void build(Concept concept) throws OntologyBuildException {
        try {
            for (int i = 0; i < this.rootPropositionDefinitions.length; i++) {
                PropositionDefinition rootPropositionDefinition =
                        this.rootPropositionDefinitions[i];
                Concept rootConcept =
                        addNode(rootPropositionDefinition);
                concept.add(rootConcept);
                addModifierConcepts(rootPropositionDefinition, rootConcept);
                buildHelper(rootPropositionDefinition.getInverseIsA(), rootConcept);
            }
        } catch (UnknownPropositionDefinitionException | InvalidConceptCodeException | KnowledgeSourceReadException ex) {
            throw new OntologyBuildException(
                    "Could not build proposition concept tree", ex);
        }
    }

    private void buildHelper(String[] childPropIds, Concept parent)
            throws UnknownPropositionDefinitionException,
            KnowledgeSourceReadException, InvalidConceptCodeException, OntologyBuildException {
        for (String childPropId : childPropIds) {
            PropositionDefinition childPropDef =
                    readPropositionDefinition(childPropId);
            Concept child = addNode(childPropDef);
            if (parent != null) {
                parent.add(child);
            }
            addModifierConcepts(childPropDef, child);
            buildHelper(childPropDef.getInverseIsA(), child);
        }
    }

    private Concept addNode(PropositionDefinition propDef)
            throws InvalidConceptCodeException, KnowledgeSourceReadException, OntologyBuildException {
        ConceptId conceptId =
                ConceptId.getInstance(propDef.getId(), this.metadata);
        Concept newChild = new Concept(conceptId, this.conceptCode, this.metadata);
        newChild.setDownloaded(propDef.getAccessed());
        String[] children = propDef.getChildren();
        String[] inverseIsAs = propDef.getInverseIsA();
        newChild.setInDataSource(children.length == 0 //is a leaf
                || inverseIsAs.length == 0 /* is abstracted */);
        newChild.setDerived(children.length > inverseIsAs.length);
        newChild.setDisplayName(propDef.getDisplayName());
        newChild.setSourceSystemCode(
                MetadataUtil.toSourceSystemCode(
                propDef.getSourceId().getStringRepresentation()));
        newChild.setValueTypeCode(this.valueTypeCode);
        newChild.setComment(propDef.getDescription());
        if (this.valueTypeCode == ValueTypeCode.LABORATORY_TESTS) {
            ValueType valueType =
                    ((ParameterDefinition) propDef).getValueType();
            newChild.setDataType(DataType.dataTypeFor(valueType));
            if (children.length < 1) {
                newChild.setMetadataXml("<?xml version=\"1.0\"?><ValueMetadata><Version>3.02</Version><CreationDateTime>" + this.createDate + 
                    "</CreationDateTime><TestID>" + newChild.getConceptCode() + 
                    "</TestID><TestName>" + newChild.getDisplayName() + 
                    "</TestName><DataType>" + (newChild.getDataType() == DataType.NUMERIC ? "Float" : "String") + "</DataType><Flagstouse></Flagstouse><Oktousevalues>Y</Oktousevalues><UnitValues><NormalUnits> </NormalUnits></UnitValues></ValueMetadata>");
            }
        } else {
            newChild.setDataType(DataType.TEXT);
        }
        if (this.metadata.getFromIdCache(conceptId) == null) {
            this.metadata.addToIdCache(newChild);
        }
        return newChild;
    }

    private void addModifierConcepts(PropositionDefinition propDef, Concept appliedConcept) throws KnowledgeSourceReadException, InvalidConceptCodeException {
        if (this.modifiers.length > 0) {
            ConceptId modParentId = ConceptId.getInstance(propDef.getId(), this.metadata);
            Concept modParent = new Concept(modParentId, null, this.metadata);
            if (this.metadata.getFromIdCache(modParentId) == null) {
                this.metadata.addToIdCache(modParent);
            }
            PathSupport pathSupport = new PathSupport();
            for (ModifierSpec modifier : this.modifiers) {
                PropertyDefinition propertyDef = propDef.propertyDefinition(modifier.getProperty());
                if (propertyDef != null) {
                    ConceptId modId = ConceptId.getInstance(null, propertyDef.getName(), this.metadata);
                    Concept mod = new Concept(modId, modifier.getCodePrefix(), this.metadata);
                    pathSupport.setConcept(mod);
                    mod.setDisplayName(modifier.getDisplayName());
                    mod.setDownloaded(propDef.getAccessed());
                    mod.setSourceSystemCode(
                            MetadataUtil.toSourceSystemCode(
                                    propDef.getSourceId().getStringRepresentation()));
                    mod.setValueTypeCode(this.valueTypeCode);
                    mod.setAppliedPath(appliedConcept.getFullName() + "%");
                    mod.setDataType(DataType.dataTypeFor(propertyDef.getValueType()));
                    mod.setFactTableColumn("MODIFIER_CD");
                    mod.setTableName("MODIFIER_DIMENSION");
                    mod.setColumnName("MODIFIER_PATH");
                    StringBuilder mXml = new StringBuilder();
                    mXml.append("<?xml version=\"1.0\"?><ValueMetadata><Version>3.02</Version><CreationDateTime>");
                    mXml.append(this.createDate);
                    mXml.append("</CreationDateTime><TestID>");
                    mXml.append(mod.getConceptCode());
                    mXml.append("</TestID><TestName>");
                    mXml.append(mod.getDisplayName());
                    mXml.append("</TestName>");
                    String valueSetId = propertyDef.getValueSetId();
                    if (valueSetId != null) {
                        mXml.append("<DataType>Enum</DataType><EnumValues>");
                        ValueSet valueSet = this.knowledgeSource.readValueSet(valueSetId);
                        if (valueSet != null) {
                            for (ValueSetElement vse : valueSet.getValueSetElements()) {
                                mXml.append("<Val description=\"");
                                mXml.append(vse.getDisplayName());
                                mXml.append("\">");
                                mXml.append(vse.getValue().getFormatted());
                                mXml.append("</Val>");
                            }
                        }
                        mXml.append("</EnumValues>");
                    } else {
                        mXml.append("<DataType>");
                        mXml.append(mod.getDataType() == DataType.NUMERIC ? "Float" : "String");
                        mXml.append("</DataType>");
                    }
                    mXml.append("<Flagstouse></Flagstouse><Oktousevalues>Y</Oktousevalues><UnitValues><NormalUnits> </NormalUnits></UnitValues></ValueMetadata>");
                    mod.setMetadataXml(mXml.toString());
                    if (this.metadata.getFromIdCache(modId) == null) {
                        this.metadata.addToIdCache(mod);
                    }
                    modParent.add(mod);
                    mod.setFullName(pathSupport.getFullName());
                    mod.setCPath(pathSupport.getCPath());
                    mod.setToolTip(pathSupport.getToolTip());
                    mod.setLevel(pathSupport.getLevel());
                    appliedConcept.add(mod);
                }
            }
        }
    }

    private PropositionDefinition readPropositionDefinition(String propId)
            throws UnknownPropositionDefinitionException,
            KnowledgeSourceReadException {
        PropositionDefinition result =
                this.knowledgeSource.readPropositionDefinition(propId);
        if (result != null) {
            return result;
        } else {
            throw new UnknownPropositionDefinitionException(propId);
        }
    }
}
