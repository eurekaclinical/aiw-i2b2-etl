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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.arp.javautil.collections.Collections;
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

    Concept[] build() throws OntologyBuildException {
        try {
            List<Concept> result = new ArrayList<>();
            for (int i = 0; i < this.rootPropositionDefinitions.length; i++) {
                PropositionDefinition rootPropositionDefinition =
                        this.rootPropositionDefinitions[i];
                Concept rootConcept =
                        addNode(rootPropositionDefinition);
                buildHelper(rootPropositionDefinition.getInverseIsA(), rootConcept);
                result.add(rootConcept);
            }
            return result.toArray(new Concept[result.size()]);
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
        
        if (this.modifiers.length > 0) {
            for (ModifierSpec modifier : this.modifiers) {
                PropertyDefinition propertyDef = propDef.propertyDefinition(modifier.getProperty());
                if (propertyDef != null) {
                    String valueSetId = propertyDef.getValueSetId();
                    if (valueSetId != null) {
                        ConceptId modId = ConceptId.getInstance(null, propertyDef.getName(), this.metadata);
                        if (this.metadata.getFromIdCache(conceptId) == null) {
                            Concept mod = this.metadata.newConcept(modId, modifier.getCodePrefix(), newChild.getSourceSystemCode());
                            mod.setDisplayName(propertyDef.getName());
                            mod.setDownloaded(newChild.getDownloaded());
                            mod.setSourceSystemCode(newChild.getSourceSystemCode());
                            mod.setValueTypeCode(newChild.getValueTypeCode());
                            mod.setAppliedPath(newChild.getFullName() + "%");
                            StringBuilder mXml = new StringBuilder();
                            mXml.append("<?xml version=\"1.0\"?><ValueMetadata><Version>3.02</Version><CreationDateTime>");
                            mXml.append(this.createDate);
                            mXml.append("</CreationDateTime><TestID>");
                            mXml.append(mod.getConceptCode());
                            mXml.append("</TestID><TestName>");
                            mXml.append(mod.getDisplayName());
                            mXml.append("</TestName><DataType>Enum</DataType><EnumValues>");
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
                            mXml.append("</EnumValues><Flagstouse></Flagstouse><Oktousevalues>Y</Oktousevalues><UnitValues><NormalUnits> </NormalUnits></UnitValues></ValueMetadata>");
                            mod.setMetadataXml(mXml.toString());
                            newChild.add(mod);
                        }
                    }
                }
            }
        }
        
        return newChild;
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
