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

import edu.emory.cci.aiw.i2b2etl.configuration.DataSection;
import edu.emory.cci.aiw.i2b2etl.configuration.DataSection.DataSpec;
import edu.emory.cci.aiw.i2b2etl.configuration.DictionarySection;
import org.protempa.*;
import org.protempa.ValueSet.ValueSetElement;
import org.protempa.proposition.value.NumberValue;

/**
 *
 * @author Andrew Post
 */
class DemographicsConceptTreeBuilder {

    private static final int[][] ageCategories = {
        ageGroup(0, 9),
        ageGroup(10, 17),
        ageGroup(18, 34),
        ageGroup(35, 44),
        ageGroup(45, 54),
        ageGroup(55, 64),
        ageGroup(65, 74),
        ageGroup(75, 84),
        ageGroup(85, 94),
        ageGroup(95, 104),
        ageGroup(105, 120)
    };
    private final KnowledgeSource knowledgeSource;
    private final DictionarySection dictionarySection;
    private final DataSection dataSection;
    private final Metadata metadata;
    private final String visitDimensionPropId;

    DemographicsConceptTreeBuilder(KnowledgeSource knowledgeSource, DictionarySection dictSection, DataSection dataSection, Metadata metadata) {
        assert knowledgeSource != null : "knowledgeSource cannot be null";
        assert dataSection != null : "dataSection cannot be null";
        assert metadata != null : "metadata cannot be null";
        this.knowledgeSource = knowledgeSource;
        this.dictionarySection = dictSection;
        this.dataSection = dataSection;
        this.metadata = metadata;
        this.visitDimensionPropId = dictSection.get("visitDimension");
    }

    Concept build() throws OntologyBuildException {
        Concept root = newConcept("Demographics");

        root.add(buildAge("Age"));

        DescendantBuilder descendantBuilder = new DescendantBuilder(root);
        descendantBuilder.build("Gender", "patientDimensionGender");
        descendantBuilder.build("Language", "patientDimensionLanguage");
        descendantBuilder.build("Marital Status", "patientDimensionMaritalStatus");
        descendantBuilder.build("Race", "patientDimensionRace");
        descendantBuilder.build("Religion", "patientDimensionReligion");
        descendantBuilder.build("Vital Status", "patientDimensionVital");

        return root;
    }

    private Concept buildAge(String displayName) throws OntologyBuildException {
        Concept age = newConcept(displayName);
        String ageConceptCodePrefix =
                this.dictionarySection.get("ageConceptCodePrefix");
        for (int i = 0; i < ageCategories.length; i++) {
            int[] ages = ageCategories[i];
            String ageRangeDisplayName = String.valueOf(ages[0]) + '-'
                    + String.valueOf(ages[ages.length - 1]) + " years old";
            Concept ageCategory = newConcept(ageRangeDisplayName);
            age.add(ageCategory);
            for (int j = 0; j < ages.length; j++) {
                try {
                    ConceptId conceptId = ConceptId.getInstance(
                            null, null,
                            NumberValue.getInstance(ages[j]),
                            this.metadata);
                    Concept ageConcept = this.metadata.getFromIdCache(conceptId);
                    if (ageConcept == null) {
                        ageConcept =
                                new Concept(conceptId,
                                ageConceptCodePrefix, this.metadata);
                        if (ages[j] == 1) {
                            ageConcept.setDisplayName(ages[j] + " year old");
                        } else {
                            ageConcept.setDisplayName(ages[j] + " years old");
                        }
                        ageConcept.setSourceSystemCode(
                                MetadataUtil.toSourceSystemCode(
                                I2B2QueryResultsHandlerSourceId.getInstance().getStringRepresentation()));
                        ageConcept.setDataType(DataType.TEXT);
                        this.metadata.addToIdCache(ageConcept);
                    } else {
                        throw new OntologyBuildException("Duplicate age concept: " + ageConcept.getConceptCode());
                    }
                    ageCategory.add(ageConcept);
                } catch (InvalidConceptCodeException ex) {
                    throw new OntologyBuildException("Could not build age concept", ex);
                }
            }
        }

        return age;
    }

    private final class DescendantBuilder {

        private final PropositionDefinition propDef;
        private final Concept root;

        DescendantBuilder(Concept root) throws OntologyBuildException {
            this.root = root;

            try {
                this.propDef = DemographicsConceptTreeBuilder.this.knowledgeSource.readPropositionDefinition(visitDimensionPropId);
                if (this.propDef == null) {
                    throw new UnknownPropositionDefinitionException(visitDimensionPropId);
                }
            } catch (KnowledgeSourceReadException ex) {
                throw new OntologyBuildException("Could not build descendants", ex);
            } catch (UnknownPropositionDefinitionException ex) {
                throw new OntologyBuildException("Could not build descendants", ex);
            }

        }

        void build(String childName, String childSpec) throws OntologyBuildException {
            DataSpec dataSpec = getDataSection(childSpec);
            Concept concept = newConcept(childName);
            if (dataSpec != null && dataSpec.referenceName != null) {
                try {
                    addChildrenFromValueSets(this.propDef, dataSpec, concept);
                } catch (KnowledgeSourceReadException ex) {
                    throw new OntologyBuildException("Could not build descendants", ex);
                } catch (InvalidConceptCodeException ex) {
                    throw new OntologyBuildException("Could not build descendants", ex);
                }
            }
            if (dataSpec != null) {
                this.root.add(concept);
            }
        }

        private DataSpec getDataSection(String dictSectionKey) {
            String dictVal = DemographicsConceptTreeBuilder.this.dictionarySection.get(dictSectionKey);
            if (dictVal != null) {
                return DemographicsConceptTreeBuilder.this.dataSection.get(dictVal);
            } else {
                return null;
            }
        }

        private void addChildrenFromValueSets(PropositionDefinition propDef,
                DataSpec dataSpec, Concept concept) throws OntologyBuildException,
                UnsupportedOperationException, KnowledgeSourceReadException,
                InvalidConceptCodeException {
            ReferenceDefinition refDef = propDef.referenceDefinition(dataSpec.referenceName);
            String[] propIds = refDef.getPropositionIds();
            for (String propId : propIds) {
                PropositionDefinition genderPropositionDef =
                        DemographicsConceptTreeBuilder.this.knowledgeSource.readPropositionDefinition(propId);
                assert genderPropositionDef != null : "genderPropositionDef cannot be null";
                PropertyDefinition genderPropertyDef = genderPropositionDef.propertyDefinition(dataSpec.propertyName);
                if (genderPropertyDef != null) {
                    String valueSetId = genderPropertyDef.getValueSetId();
                    if (valueSetId == null) {
                        throw new UnsupportedOperationException("We don't support non-enumerated property values for demographics yet!");
                    }
                    ValueSet valueSet =
                            DemographicsConceptTreeBuilder.this.knowledgeSource.readValueSet(valueSetId);
                    ValueSetElement[] valueSetElements = valueSet.getValueSetElements();
                    for (ValueSetElement valueSetElement : valueSetElements) {
                        ConceptId conceptId = ConceptId.getInstance(propId, dataSpec.propertyName, valueSetElement.getValue(), DemographicsConceptTreeBuilder.this.metadata);
                        Concept childConcept = DemographicsConceptTreeBuilder.this.metadata.getFromIdCache(conceptId);
                        if (childConcept == null) {
                            childConcept = new Concept(conceptId, dataSpec.conceptCodePrefix, DemographicsConceptTreeBuilder.this.metadata);
                            childConcept.setDisplayName(valueSetElement.getDisplayName());
                            childConcept.setSourceSystemCode(MetadataUtil.toSourceSystemCode(I2B2QueryResultsHandlerSourceId.getInstance().getStringRepresentation()));
                            childConcept.setDataType(DataType.TEXT);
                            DemographicsConceptTreeBuilder.this.metadata.addToIdCache(childConcept);
                        } else {
                            throw new OntologyBuildException("Duplicate demographics concept: " + childConcept.getConceptCode());
                        }
                        concept.add(childConcept);
                    }
                }
            }
        }
    }

    private Concept newConcept(String displayName) throws OntologyBuildException {
        String conceptIdPrefix = MetadataUtil.DEFAULT_CONCEPT_ID_PREFIX_INTERNAL + "|Demographics|";
        ConceptId conceptId = ConceptId.getInstance(displayName, metadata);
        Concept folder = this.metadata.getFromIdCache(conceptId);
        if (folder == null) {
            try {
                folder = new Concept(conceptId, conceptIdPrefix + displayName, this.metadata);
            } catch (InvalidConceptCodeException ex) {
                throw new OntologyBuildException("Error building ontology", ex);
            }
            folder.setSourceSystemCode(MetadataUtil.toSourceSystemCode(I2B2QueryResultsHandlerSourceId.getInstance().getStringRepresentation()));
            folder.setDisplayName(displayName);
            folder.setDataType(DataType.TEXT);
            this.metadata.addToIdCache(folder);
        } else {
            throw new OntologyBuildException(
                    "Duplicate demographics concept: " + folder.getConceptCode());
        }
        return folder;
    }

    private static int[] ageGroup(int minAge, int maxAge) {
        int[] result = new int[maxAge - minAge + 1];
        for (int i = 0; i < result.length; i++) {
            result[i] = minAge + i;
        }
        return result;
    }
}
