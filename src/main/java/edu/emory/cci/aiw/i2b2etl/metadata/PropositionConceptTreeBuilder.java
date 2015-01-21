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

import java.text.SimpleDateFormat;
import java.util.Date;
import org.protempa.KnowledgeSource;
import org.protempa.KnowledgeSourceReadException;
import org.protempa.ParameterDefinition;
import org.protempa.PropositionDefinition;
import org.protempa.ProtempaUtil;
import org.protempa.proposition.value.ValueType;

final class PropositionConceptTreeBuilder {

    private final SimpleDateFormat valueMetadataCreateDateTimeFormat;
    private final KnowledgeSource knowledgeSource;
    private final PropositionDefinition[] rootPropositionDefinitions;
    private final String conceptCode;
    private final Metadata metadata;
    private final ValueTypeCode valueTypeCode;
    private final String createDate;

    PropositionConceptTreeBuilder(KnowledgeSource knowledgeSource,
            String[] propIds, String conceptCode, ValueTypeCode valueTypeCode,
                                  Metadata metadata)
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
    }

    Concept[] build() throws OntologyBuildException {
        try {
            Concept[] result =
                    new Concept[this.rootPropositionDefinitions.length];
            for (int i = 0; i < this.rootPropositionDefinitions.length; i++) {
                PropositionDefinition rootPropositionDefinition =
                        this.rootPropositionDefinitions[i];
                Concept rootConcept =
                        addNode(rootPropositionDefinition);
                buildHelper(rootPropositionDefinition.getInverseIsA(), rootConcept);
                result[i] = rootConcept;
            }
            return result;
        } catch (UnknownPropositionDefinitionException | InvalidConceptCodeException | KnowledgeSourceReadException ex) {
            throw new OntologyBuildException(
                    "Could not build proposition concept tree", ex);
        }
    }

    private void buildHelper(String[] childPropIds, Concept parent)
            throws UnknownPropositionDefinitionException,
            KnowledgeSourceReadException, InvalidConceptCodeException {
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
            throws InvalidConceptCodeException {
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
