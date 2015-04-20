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

import edu.emory.cci.aiw.i2b2etl.dest.metadata.conceptid.InvalidConceptCodeException;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.conceptid.ModifierParentConceptId;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.conceptid.ConceptId;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.conceptid.ModifierConceptId;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.conceptid.PropDefConceptId;
import edu.emory.cci.aiw.i2b2etl.dest.config.ModifierSpec;
import edu.emory.cci.aiw.i2b2etl.util.Util;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringEscapeUtils;
import org.protempa.Attribute;
import org.protempa.KnowledgeSource;
import org.protempa.KnowledgeSourceReadException;
import org.protempa.ParameterDefinition;
import org.protempa.PropertyDefinition;
import org.protempa.PropositionDefinition;
import org.protempa.proposition.value.Value;
import org.protempa.valueset.ValueSet;
import org.protempa.valueset.ValueSetElement;
import org.protempa.proposition.value.ValueType;

class PropositionConceptTreeBuilder implements OntologyBuilder, SubtreeBuilder {

    private static final ModifierSpec[] EMPTY_MODIFIER_SPEC_ARRAY = new ModifierSpec[0];

    private final SimpleDateFormat valueMetadataCreateDateTimeFormat;
    private final KnowledgeSource knowledgeSource;
    private final String conceptCode;
    private final Metadata metadata;
    private final ValueTypeCode valueTypeCode;
    private final String createDate;
    private final ModifierSpec[] modifiers;
    private final String[] propIds;
    private final Map<String, PropositionDefinition> cache;
    private final boolean alreadyLoaded;
    private List<Concept> roots;

    PropositionConceptTreeBuilder(Map<String, PropositionDefinition> cache,
            KnowledgeSource knowledgeSource,
            String[] propIds, String conceptCode, ValueTypeCode valueTypeCode,
            ModifierSpec[] modifiers, boolean alreadyLoaded, Metadata metadata)
            throws KnowledgeSourceReadException,
            UnknownPropositionDefinitionException {
        assert knowledgeSource != null : "knowledgeSource cannot be null";
        assert cache != null : "cache cannot be null";
        assert metadata != null : "metadata cannot be null";
        this.propIds = propIds;
        this.knowledgeSource = knowledgeSource;
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
        this.cache = cache;
        this.alreadyLoaded = alreadyLoaded;
        this.roots = new ArrayList<>();
    }

    Metadata getMetadata() {
        return metadata;
    }

    @Override
    public void build(Concept concept) throws OntologyBuildException {
        try {
            for (String childPropId : this.propIds) {
                PropositionDefinition childPropDef
                        = readPropositionDefinition(childPropId);
                Concept child = addNode(childPropDef);
                if (concept != null) {
                    concept.add(child);
                }
                this.roots.add(child);
                addModifierConcepts(childPropDef, child);
                buildHelper(childPropDef.getInverseIsA(), child);
            }
        } catch (UnknownPropositionDefinitionException | InvalidConceptCodeException | KnowledgeSourceReadException ex) {
            throw new OntologyBuildException(
                    "Could not build proposition concept tree", ex);
        }
    }

    @Override
    public Concept[] getRoots() {
        return this.roots.toArray(new Concept[this.roots.size()]);
    }

    private void buildHelper(String[] childPropIds, Concept parent)
            throws UnknownPropositionDefinitionException,
            KnowledgeSourceReadException, InvalidConceptCodeException, OntologyBuildException {
        for (String childPropId : childPropIds) {
            PropositionDefinition childPropDef
                    = readPropositionDefinition(childPropId);
            Concept child = addNode(childPropDef);
            parent.add(child);
            addModifierConcepts(childPropDef, child);
            buildHelper(childPropDef.getInverseIsA(), child);
        }
    }

    private Concept addNode(PropositionDefinition propDef)
            throws InvalidConceptCodeException, KnowledgeSourceReadException, OntologyBuildException {
        ConceptId conceptId
                = PropDefConceptId.getInstance(propDef.getId(), null, null, this.metadata);
        Concept newChild = new Concept(conceptId, this.conceptCode, this.metadata);
        newChild.setDownloaded(propDef.getDownloaded());
        Date updated = propDef.getUpdated();
        newChild.setUpdated(updated != null ? propDef.getUpdated() : propDef.getCreated());
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
        newChild.setAlreadyLoaded(this.alreadyLoaded);
        Attribute attribute = propDef.attribute(Util.C_FULLNAME_ATTRIBUTE_NAME);
        if (attribute != null) {
            Value value = attribute.getValue();
            if (value != null) {
                newChild.setFullName(value.getFormatted());
            }
        }
        if (this.valueTypeCode == ValueTypeCode.LABORATORY_TESTS) {
            ValueType valueType
                    = ((ParameterDefinition) propDef).getValueType();
            newChild.setDataType(DataType.dataTypeFor(valueType));
            if (children.length < 1) {
                newChild.setMetadataXml("<?xml version=\"1.0\"?><ValueMetadata><Version>3.02</Version><CreationDateTime>" + this.createDate
                        + "</CreationDateTime><TestID>" + StringEscapeUtils.escapeXml10(newChild.getConceptCode())
                        + "</TestID><TestName>" + StringEscapeUtils.escapeXml10(newChild.getDisplayName())
                        + "</TestName><DataType>" + (newChild.getDataType() == DataType.NUMERIC ? "Float" : "String") + "</DataType><Flagstouse></Flagstouse><Oktousevalues>Y</Oktousevalues><UnitValues><NormalUnits> </NormalUnits></UnitValues></ValueMetadata>");
            }
        } else {
            newChild.setDataType(DataType.TEXT);
        }
        if (this.metadata.getFromIdCache(conceptId) == null) {
            this.metadata.addToIdCache(newChild);
        } else {
            newChild.setSynonymCode(SynonymCode.SYNONYM);
        }
        return newChild;
    }

    private void addModifierConcepts(PropositionDefinition propDef, Concept appliedConcept) throws KnowledgeSourceReadException, InvalidConceptCodeException {
        if (this.modifiers.length > 0) {
            ConceptId modParentId = ModifierParentConceptId.getInstance(propDef.getId(), this.metadata);
            Concept modParent = new Concept(modParentId, null, this.metadata);
            modParent.setAlreadyLoaded(this.alreadyLoaded);
            this.metadata.addModifierRoot(modParent);
            if (this.metadata.getFromIdCache(modParentId) == null) {
                this.metadata.addToIdCache(modParent);
            } else {
                modParent.setSynonymCode(SynonymCode.SYNONYM);
            }
            for (ModifierSpec modifier : this.modifiers) {
                PropertyDefinition propertyDef = propDef.propertyDefinition(modifier.getProperty());
                if (propertyDef != null && !propertyDef.isInherited()) {
                    String valStr = modifier.getValue();
                    Value val = valStr != null ? propertyDef.getValueType().parse(valStr) : null;
                    ConceptId modId = ModifierConceptId.getInstance(
                            propDef.getId(),
                            propertyDef.getId(),
                            val,
                            this.metadata);
                    Concept mod = new Concept(modId, modifier.getCodePrefix(), this.metadata);
                    mod.setDisplayName(modifier.getDisplayName());
                    Date updated = propDef.getUpdated();
                    mod.setUpdated(updated != null ? propDef.getUpdated() : propDef.getCreated());
                    mod.setDownloaded(propDef.getDownloaded());
                    mod.setSourceSystemCode(
                            MetadataUtil.toSourceSystemCode(
                                    propDef.getSourceId().getStringRepresentation()));
                    mod.setValueTypeCode(this.valueTypeCode);
                    mod.setAppliedPath(appliedConcept.getFullName() + "%");
                    mod.setDataType(DataType.dataTypeFor(propertyDef.getValueType()));
                    mod.setFactTableColumn("MODIFIER_CD");
                    mod.setTableName("MODIFIER_DIMENSION");
                    mod.setColumnName("MODIFIER_PATH");
                    mod.setAlreadyLoaded(this.alreadyLoaded);
                    Attribute attribute = propertyDef.attribute(Util.C_FULLNAME_ATTRIBUTE_NAME);
                    if (attribute != null) {
                        Value value = attribute.getValue();
                        if (value != null) {
                            mod.setFullName(value.getFormatted());
                        }
                    }
                    if (propertyDef.getValueType() != ValueType.BOOLEANVALUE) {
                        StringBuilder mXml = new StringBuilder();
                        mXml.append("<?xml version=\"1.0\"?><ValueMetadata><Version>3.02</Version><CreationDateTime>");
                        mXml.append(this.createDate);
                        mXml.append("</CreationDateTime><TestID>");
                        mXml.append(StringEscapeUtils.escapeXml10(mod.getConceptCode()));
                        mXml.append("</TestID><TestName>");
                        mXml.append(StringEscapeUtils.escapeXml10(mod.getDisplayName()));
                        mXml.append("</TestName>");
                        String valueSetId = propertyDef.getValueSetId();
                        if (valueSetId != null) {
                            mXml.append("<DataType>Enum</DataType><EnumValues>");
                            ValueSet valueSet = this.knowledgeSource.readValueSet(valueSetId);
                            if (valueSet != null) {
                                for (ValueSetElement vse : valueSet.getValueSetElements()) {
                                    mXml.append("<Val description=\"");
                                    mXml.append(StringEscapeUtils.escapeXml10(vse.getDisplayName()));
                                    mXml.append("\">");
                                    mXml.append(StringEscapeUtils.escapeXml10(vse.getValue().getFormatted()));
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
                    }
                    if (this.metadata.getFromIdCache(modId) == null) {
                        this.metadata.addToIdCache(mod);
                    } else {
                        mod.setSynonymCode(SynonymCode.SYNONYM);
                    }
                    if (this.metadata.getFromIdCache(modId) == null) {
                        this.metadata.addToIdCache(mod);
                    } else {
                        mod.setSynonymCode(SynonymCode.SYNONYM);
                    }
                    modParent.add(mod);
                }
            }
        }
    }

    private PropositionDefinition readPropositionDefinition(String propId)
            throws UnknownPropositionDefinitionException,
            KnowledgeSourceReadException {
        PropositionDefinition result = cache.get(propId);
        if (result != null) {
            return result;
        } else {
            throw new UnknownPropositionDefinitionException(propId);
        }
    }
}
