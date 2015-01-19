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

import edu.emory.cci.aiw.i2b2etl.configuration.DictionarySection;
import org.protempa.KnowledgeSource;
import org.protempa.proposition.value.NominalValue;
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
    private final DictionarySection dictionarySection;
    private final Metadata metadata;
    private final String qrhId;

    DemographicsConceptTreeBuilder(String qrhId, KnowledgeSource knowledgeSource, DictionarySection dictSection, Metadata metadata) {
        assert knowledgeSource != null : "knowledgeSource cannot be null";
        assert metadata != null : "metadata cannot be null";
        assert qrhId != null : "qrhId cannot be null";
        this.dictionarySection = dictSection;
        this.metadata = metadata;
        this.qrhId = qrhId;
    }

    Concept build() throws OntologyBuildException {
        Concept root = newContainerConcept("Demographics", MetadataUtil.DEFAULT_CONCEPT_ID_PREFIX_INTERNAL + "|Demographics");
        root.add(buildAge("Age"));

        DimensionValueSetFolderBuilder descendantBuilder = this.metadata.newDimensionValueSetFolderBuilder(root, "patient_num", "patient_dimension");
        descendantBuilder.build("Gender", "patientDimensionGender", "sex_cd");
        descendantBuilder.build("Language", "patientDimensionLanguage", "language_cd");
        descendantBuilder.build("Marital Status", "patientDimensionMaritalStatus", "marital_status_cd");
        descendantBuilder.build("Race", "patientDimensionRace", "race_cd");
        descendantBuilder.build("Religion", "patientDimensionReligion", "religion_cd");
        descendantBuilder.build("Vital Status", "patientDimensionVital", "vital_status_cd");

        return root;
    }

    private Concept buildAge(String displayName) throws OntologyBuildException {
        Concept age = newContainerConcept(displayName, MetadataUtil.DEFAULT_CONCEPT_ID_PREFIX_INTERNAL + "|Demographics|Age");
        String ageConceptCodePrefix =
                this.dictionarySection.get("ageConceptCodePrefix");
        for (int i = 0; i < ageCategories.length; i++) {
            int[] ages = ageCategories[i];
            String ageRangeDisplayName = String.valueOf(ages[0]) + '-'
                    + String.valueOf(ages[ages.length - 1]) + " years old";
            ConceptId ageRangeConceptId = ConceptId.getInstance(null, null, NominalValue.getInstance(ageRangeDisplayName), metadata);
            Concept ageCategory = newQueryableConcept(ageRangeConceptId, ageConceptCodePrefix);
            ageCategory.setColumnName("birth_date");
            ageCategory.setDataType(DataType.NUMERIC);
            ageCategory.setDisplayName(ageRangeDisplayName);
            if (i == 0) {
                ageCategory.setOperator(ConceptOperator.GREATER_THAN);
                ageCategory.setDimCode("sysdate - (365.25 * " + (ages[ages.length - 1] + 1) + ")");
            } else {
                ageCategory.setOperator(ConceptOperator.BETWEEN);
                /*
                 * This dimcode is what is recommended in i2b2's documentation at
                 * https://community.i2b2.org/wiki/display/DevForum/Query+Building+from+Ontology.
                 * There seems to be a problem with it, though. BETWEEN is inclusive on both
                 * sides of the range. Thus, if sysdate happens to be exactly midnight, the
                 * patient will end up in two adjacent age buckets.
                 */
                ageCategory.setDimCode("sysdate - (365.25 * " + (ages[ages.length - 1] + 1) + ") AND sysdate - (365.25 * " + ages[0] + ")");
            }
            age.add(ageCategory);
            for (int j = 0; j < ages.length; j++) {
                ConceptId conceptId = ConceptId.getInstance(
                        null, null,
                        NumberValue.getInstance(ages[j]),
                        this.metadata);
                Concept ageConcept = newQueryableConcept(conceptId, ageConceptCodePrefix);
                if (ages[j] == 1) {
                    ageConcept.setDisplayName(ages[j] + " year old");
                } else {
                    ageConcept.setDisplayName(ages[j] + " years old");
                }
                ageConcept.setDataType(DataType.NUMERIC);
                ageConcept.setColumnName("birth_date");
                ageConcept.setOperator(ConceptOperator.BETWEEN);
                /*
                 * This dimcode is what is recommended in i2b2's
                 * documentation at
                 * https://community.i2b2.org/wiki/display/DevForum/Query+Building+from+Ontology.
                 * There seems to be a problem with it, though. BETWEEN
                 * is inclusive on both sides of the range. Thus, if
                 * sysdate happens to be exactly midnight, the patient
                 * will end up in two adjacent age buckets.
                 */
                ageConcept.setDimCode("sysdate - (365.25 * " + (ages[j] + 1) + ") AND sysdate - (365.25 * " + ages[j] + ")");
                ageCategory.add(ageConcept);
            }
        }

        return age;
    }
    
    private Concept newContainerConcept(String displayName, String conceptCode) throws OntologyBuildException {
        ConceptId conceptId = ConceptId.getInstance(displayName, metadata);
        Concept concept = this.metadata.newConcept(conceptId, conceptCode, this.qrhId);
        concept.setCVisualAttributes("CAE");
        concept.setDisplayName(displayName);
        return concept;
    }
    
    private Concept newQueryableConcept(ConceptId conceptId, String conceptCodePrefix) throws OntologyBuildException {
        Concept concept = this.metadata.newConcept(conceptId, conceptCodePrefix, this.qrhId);
        concept.setFactTableColumn("patient_num");
        concept.setTableName("patient_dimension");
        return concept;
    }

    private static int[] ageGroup(int minAge, int maxAge) {
        int[] result = new int[maxAge - minAge + 1];
        for (int i = 0; i < result.length; i++) {
            result[i] = minAge + i;
        }
        return result;
    }
}
