package edu.emory.cci.aiw.i2b2etl.dsb;

/*
 * #%L
 * AIW i2b2 ETL
 * %%
 * Copyright (C) 2012 - 2014 Emory University
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

import java.io.IOException;
import java.util.logging.Logger;
import org.protempa.backend.BackendInitializationException;
import org.protempa.backend.BackendInstanceSpec;
import org.protempa.backend.annotations.BackendInfo;
import org.protempa.backend.annotations.BackendProperty;
import org.protempa.backend.dsb.relationaldb.ColumnSpec;
import org.protempa.backend.dsb.relationaldb.EntitySpec;
import org.protempa.backend.dsb.relationaldb.JDBCDateTimeTimestampDateValueFormat;
import org.protempa.backend.dsb.relationaldb.JDBCDateTimeTimestampPositionParser;
import org.protempa.backend.dsb.relationaldb.JDBCPositionFormat;
import org.protempa.backend.dsb.relationaldb.JoinSpec;
import org.protempa.backend.dsb.relationaldb.PropertySpec;
import org.protempa.backend.dsb.relationaldb.ReferenceSpec;
import org.protempa.backend.dsb.relationaldb.RelationalDbDataSourceBackend;
import org.protempa.backend.dsb.relationaldb.StagingSpec;
import org.protempa.proposition.value.AbsoluteTimeGranularity;
import org.protempa.proposition.value.AbsoluteTimeGranularityFactory;
import org.protempa.proposition.value.AbsoluteTimeUnit;
import org.protempa.proposition.value.AbsoluteTimeUnitFactory;
import org.protempa.proposition.value.GranularityFactory;
import org.protempa.proposition.value.UnitFactory;
import org.protempa.proposition.value.ValueType;

/**
 *
 * @author Andrew Post
 */
@BackendInfo(displayName = "I2B2 Data Source Backend")
public final class I2B2DataSourceBackend extends RelationalDbDataSourceBackend {
    
    private final static AbsoluteTimeUnitFactory ABS_TIME_UNIT_FACTORY =
            new AbsoluteTimeUnitFactory();
    private final static AbsoluteTimeGranularityFactory ABS_TIME_GRANULARITY_FACTORY =
            new AbsoluteTimeGranularityFactory();
    private static final JDBCPositionFormat POSITION_PARSER =
            new JDBCDateTimeTimestampPositionParser();
    
    private final static String PATIENT_DIMENSION = "patient_dimension";
    private final static String VISIT_DIMENSION = "visit_dimension";
    private final static String OBSERVATION_FACT = "observation_fact";
    private final static String PROVIDER_DIMENSION = "provider_dimension";
    
    private final static Logger LOGGER = 
                Logger.getLogger(I2B2DataSourceBackend.class.getPackage().getName());
    private Mapper mapper;

    public I2B2DataSourceBackend() {
        setDefaultKeyIdTable(PATIENT_DIMENSION);
        /*
         * Per the i2b2 1.7 CRC design docs and stored procedure 
         * implementation, 
         * to_char(patient_num) = patient_ide when patient_ide_source = 'HIVE'.
         */
        setDefaultKeyIdColumn("patient_num"); 
        setDefaultKeyIdJoinKey("patient_num");
        setMapper(null);
    }

    public Mapper getMapper() {
        return mapper;
    }

    public void setMapper(Mapper mapper) {
        if (mapper == null) {
            this.mapper = new ResourceMapper("/etc/i2b2dsb/", getClass());
        } else {
            this.mapper = mapper;
        }
    }
    
    @BackendProperty(propertyName = "mapper")
    public void parseMapper(String pathname) {
        this.mapper = new FileMapper(pathname);
    }
    
    @Override
    public void initialize(BackendInstanceSpec config) throws BackendInitializationException {
        super.initialize(config);
    }
    
    @Override
    protected EntitySpec[] constantSpecs(String keyIdSchema, String keyIdTable, String keyIdColumn, String keyIdJoinKey) throws IOException {
        String schemaName = getSchemaName();
        return new EntitySpec[] {
            new EntitySpec("Patients", 
                    null, 
                    new String[]{"PatientAll"}, 
                    true, 
                    new ColumnSpec(keyIdSchema, keyIdTable, keyIdColumn), 
                    new ColumnSpec[]{
                        new ColumnSpec(keyIdSchema, keyIdTable, keyIdColumn)
                    }, 
                    null, 
                    null, 
                    new PropertySpec[]{
                        /*
                         * This should be patient_ide where patient_ide_source = 'HIVE'.
                         */
                        new PropertySpec("patientId", null, new ColumnSpec(keyIdSchema, keyIdTable, keyIdColumn), ValueType.NOMINALVALUE)
                    }, 
                    new ReferenceSpec[]{
                        new ReferenceSpec("encounters", "Encounters", new ColumnSpec[]{new ColumnSpec(keyIdSchema, keyIdTable, new JoinSpec(keyIdJoinKey, "patient_num", new ColumnSpec(schemaName, VISIT_DIMENSION, "encounter_num")))}, ReferenceSpec.Type.MANY), 
                        new ReferenceSpec("patientDetails", "Patient Details", new ColumnSpec[]{new ColumnSpec(keyIdSchema, keyIdTable, keyIdColumn)}, ReferenceSpec.Type.MANY)}, 
                    null, null, null, null, null, null, null, null),
            new EntitySpec("Patient Details", 
                    null, 
                    new String[]{"Patient"}, 
                    true, 
                    new ColumnSpec(keyIdSchema, keyIdTable, keyIdColumn, new JoinSpec(keyIdJoinKey, "patient_num", new ColumnSpec(schemaName, PATIENT_DIMENSION))), 
                    new ColumnSpec[]{
                        new ColumnSpec(schemaName, keyIdTable, "patient_num")
                    }, 
                    null, 
                    null, 
                    new PropertySpec[]{
                        new PropertySpec("patientId", null, new ColumnSpec(schemaName, PATIENT_DIMENSION, "patient_num"), ValueType.NOMINALVALUE), 
                        new PropertySpec("gender", null, new ColumnSpec(schemaName, PATIENT_DIMENSION, "SEX_CD", ColumnSpec.Constraint.EQUAL_TO, this.mapper.propertyNameOrPropIdToSqlCodeArray("gender.txt"), true), ValueType.NOMINALVALUE), 
                        new PropertySpec("race", null, new ColumnSpec(schemaName, PATIENT_DIMENSION, "RACE_CD", ColumnSpec.Constraint.EQUAL_TO, this.mapper.propertyNameOrPropIdToSqlCodeArray("race.txt"), true), ValueType.NOMINALVALUE), 
                        new PropertySpec("ethnicity", null, new ColumnSpec(schemaName, PATIENT_DIMENSION, "RACE_CD", ColumnSpec.Constraint.EQUAL_TO, this.mapper.propertyNameOrPropIdToSqlCodeArray("ethnicity.txt"), true), ValueType.NOMINALVALUE), 
                        new PropertySpec("dateOfBirth", null, new ColumnSpec(schemaName, PATIENT_DIMENSION, "BIRTH_DATE"), ValueType.DATEVALUE, new JDBCDateTimeTimestampDateValueFormat()),
                        new PropertySpec("language", null, new ColumnSpec(schemaName, PATIENT_DIMENSION, "LANGUAGE_CD", ColumnSpec.Constraint.EQUAL_TO, this.mapper.propertyNameOrPropIdToSqlCodeArray("language.txt"), true), ValueType.NOMINALVALUE),
                        new PropertySpec("maritalStatus", null, new ColumnSpec(schemaName, PATIENT_DIMENSION, "MARITAL_STATUS_CD", ColumnSpec.Constraint.EQUAL_TO, this.mapper.propertyNameOrPropIdToSqlCodeArray("marital_status.txt"), true), ValueType.NOMINALVALUE), 
                    },
                    new ReferenceSpec[]{
                        new ReferenceSpec("encounters", "Encounters", 
                                new ColumnSpec[]{
                                    new ColumnSpec(schemaName, PATIENT_DIMENSION, new JoinSpec("patient_num", "patient_num", new ColumnSpec(schemaName, VISIT_DIMENSION, "encounter_num")))
                                }, ReferenceSpec.Type.MANY), 
                        new ReferenceSpec("patient", "Patients", 
                                new ColumnSpec[]{new ColumnSpec(schemaName, PATIENT_DIMENSION, "patient_num")
                                }, ReferenceSpec.Type.ONE)
                    }, 
                    null, null, null, null, null, null, null, null),
            new EntitySpec("Providers", null, 
                    new String[]{"AttendingPhysician"}, 
                    false, 
                    new ColumnSpec(keyIdSchema, keyIdTable, keyIdColumn, new JoinSpec(keyIdJoinKey, "patient_num", new ColumnSpec(schemaName, PATIENT_DIMENSION, new JoinSpec("patient_num", "patient_num", new ColumnSpec(schemaName, VISIT_DIMENSION, new JoinSpec("encounter_num", "encounter_num", new ColumnSpec(schemaName, OBSERVATION_FACT, new JoinSpec("provider_id", "provider_id", new ColumnSpec(schemaName, PROVIDER_DIMENSION))))))))), 
                    new ColumnSpec[]{
                        new ColumnSpec(schemaName, PROVIDER_DIMENSION, "provider_id")
                    }, 
                    null, 
                    null, 
                    new PropertySpec[]{
                        new PropertySpec("fullName", null, new ColumnSpec(schemaName, PROVIDER_DIMENSION, "NAME_CHAR"), ValueType.NOMINALVALUE)
                    }, null, null, null, null, null, null, null, null, null),
        };
    }

    @Override
    protected EntitySpec[] eventSpecs(String keyIdSchema, String keyIdTable, String keyIdColumn, String keyIdJoinKey) throws IOException {
        String schemaName = getSchemaName();
        EntitySpec[] eventSpecs = {
            new EntitySpec("Encounters", 
                    null, 
                    new String[]{"Encounter"}, 
                    true, 
                    new ColumnSpec(keyIdSchema, keyIdTable, keyIdColumn, new JoinSpec(keyIdJoinKey, "patient_num", new ColumnSpec(schemaName, PATIENT_DIMENSION, new JoinSpec("patient_num", "patient_num", new ColumnSpec(schemaName, VISIT_DIMENSION))))), 
                    new ColumnSpec[]{
                        /*
                         * This should be encounter_ide where encounter_ide_source = 'HIVE'.
                         * Like with patient_num, encounter_num = encounter_ide
                         * when encounter_ide_source = 'HIVE'.
                         */
                        new ColumnSpec(schemaName, VISIT_DIMENSION, "encounter_num")
                    }, 
                    new ColumnSpec(schemaName, VISIT_DIMENSION, "START_DATE"), 
                    new ColumnSpec(schemaName, VISIT_DIMENSION, "END_DATE"), 
                    new PropertySpec[]{
                        new PropertySpec("encounterId", null, new ColumnSpec(schemaName, VISIT_DIMENSION, "encounter_num"), ValueType.NOMINALVALUE), 
                        /*new PropertySpec("type", null, new ColumnSpec(schemaName, "ENCOUNTER", "PATIENTTYPE", ColumnSpec.Constraint.EQUAL_TO, this.mapper.propertyNameOrPropIdToSqlCodeArray("type_encounter_07182011.txt"), true), ValueType.NOMINALVALUE), 
                        new PropertySpec("healthcareEntity", null, new ColumnSpec(schemaName, "ENCOUNTER", "UNIVCODE", ColumnSpec.Constraint.EQUAL_TO, this.mapper.propertyNameOrPropIdToSqlCodeArray("entity_healthcare_07182011.txt"), true), ValueType.NOMINALVALUE), 
                        new PropertySpec("dischargeDisposition", null, new ColumnSpec(schemaName, "ENCOUNTER", "DISCHARGESTATUSCODE", ColumnSpec.Constraint.EQUAL_TO, this.mapper.propertyNameOrPropIdToSqlCodeArray("disposition_discharge_07182011.txt"), true), ValueType.NOMINALVALUE), 
                        new PropertySpec("dischargeDispositionCat", null, new ColumnSpec(schemaName, "ENCOUNTER", "DISCHARGESTATUSCODE", ColumnSpec.Constraint.EQUAL_TO, this.mapper.propertyNameOrPropIdToSqlCodeArray("disposition_discharge_category_10072011.txt"), true), ValueType.NOMINALVALUE), 
                        new PropertySpec("aprdrgRiskMortalityValue", null, new ColumnSpec(schemaName, "ENCOUNTER", "APRRISKOFMORTALITY"), ValueType.NUMERICALVALUE), new PropertySpec("aprdrgSeverityValue", null, new ColumnSpec(schemaName, "ENCOUNTER", "APRSEVERITYOFILLNESS"), ValueType.NOMINALVALUE), 
                        new PropertySpec("insuranceType", null, new ColumnSpec(schemaName, "ENCOUNTER", "HOSPITALPRIMARYPAYER", ColumnSpec.Constraint.EQUAL_TO, this.mapper.propertyNameOrPropIdToSqlCodeArray("insurance_types_07182011.txt")), ValueType.NOMINALVALUE)*/
                        },
                    new ReferenceSpec[]{
                        new ReferenceSpec("patient", "Patients", 
                                new ColumnSpec[]{
                                    new ColumnSpec(schemaName, VISIT_DIMENSION, "patient_num")
                                }, ReferenceSpec.Type.ONE),
                        new ReferenceSpec("patientDetails", "Patient Details",
                                new ColumnSpec[] { 
                                    new ColumnSpec(schemaName, VISIT_DIMENSION, "patient_num") 
                                },
                                ReferenceSpec.Type.ONE),
                        new ReferenceSpec("diagnosisCodes", "Diagnosis Codes", 
                                new ColumnSpec[]{
                                    new ColumnSpec(schemaName, VISIT_DIMENSION, new JoinSpec("encounter_num", "encounter_num", new ColumnSpec(schemaName, OBSERVATION_FACT, "encounter_num"))),
                                    new ColumnSpec(schemaName, VISIT_DIMENSION, new JoinSpec("encounter_num", "encounter_num", new ColumnSpec(schemaName, OBSERVATION_FACT, "concept_cd"))),
                                    new ColumnSpec(schemaName, VISIT_DIMENSION, new JoinSpec("encounter_num", "encounter_num", new ColumnSpec(schemaName, OBSERVATION_FACT, "provider_id"))),
                                    new ColumnSpec(schemaName, VISIT_DIMENSION, new JoinSpec("encounter_num", "encounter_num", new ColumnSpec(schemaName, OBSERVATION_FACT, "start_date")))
                                }, ReferenceSpec.Type.MANY), 
                        new ReferenceSpec("procedures", "ICD9 Procedure Codes", 
                                new ColumnSpec[]{
                                    new ColumnSpec(schemaName, VISIT_DIMENSION, new JoinSpec("encounter_num", "encounter_num", new ColumnSpec(schemaName, OBSERVATION_FACT, "encounter_num"))),
                                    new ColumnSpec(schemaName, VISIT_DIMENSION, new JoinSpec("encounter_num", "encounter_num", new ColumnSpec(schemaName, OBSERVATION_FACT, "concept_cd"))),
                                    new ColumnSpec(schemaName, VISIT_DIMENSION, new JoinSpec("encounter_num", "encounter_num", new ColumnSpec(schemaName, OBSERVATION_FACT, "provider_id"))),
                                    new ColumnSpec(schemaName, VISIT_DIMENSION, new JoinSpec("encounter_num", "encounter_num", new ColumnSpec(schemaName, OBSERVATION_FACT, "start_date")))
                                }, ReferenceSpec.Type.MANY), 
                        new ReferenceSpec("msdrg", "MSDRG Codes", 
                                new ColumnSpec[]{
                                    new ColumnSpec(schemaName, VISIT_DIMENSION, new JoinSpec("encounter_num", "encounter_num", new ColumnSpec(schemaName, OBSERVATION_FACT, "encounter_num"))),
                                    new ColumnSpec(schemaName, VISIT_DIMENSION, new JoinSpec("encounter_num", "encounter_num", new ColumnSpec(schemaName, OBSERVATION_FACT, "concept_cd"))),
                                    new ColumnSpec(schemaName, VISIT_DIMENSION, new JoinSpec("encounter_num", "encounter_num", new ColumnSpec(schemaName, OBSERVATION_FACT, "provider_id"))),
                                    new ColumnSpec(schemaName, VISIT_DIMENSION, new JoinSpec("encounter_num", "encounter_num", new ColumnSpec(schemaName, OBSERVATION_FACT, "start_date")))
                                }, ReferenceSpec.Type.ONE), 
                        new ReferenceSpec("aprdrg", "APR DRG Codes", 
                                new ColumnSpec[]{
                                    new ColumnSpec(schemaName, VISIT_DIMENSION, new JoinSpec("encounter_num", "encounter_num", new ColumnSpec(schemaName, OBSERVATION_FACT, "encounter_num"))),
                                    new ColumnSpec(schemaName, VISIT_DIMENSION, new JoinSpec("encounter_num", "encounter_num", new ColumnSpec(schemaName, OBSERVATION_FACT, "concept_cd"))),
                                    new ColumnSpec(schemaName, VISIT_DIMENSION, new JoinSpec("encounter_num", "encounter_num", new ColumnSpec(schemaName, OBSERVATION_FACT, "provider_id"))),
                                    new ColumnSpec(schemaName, VISIT_DIMENSION, new JoinSpec("encounter_num", "encounter_num", new ColumnSpec(schemaName, OBSERVATION_FACT, "start_date")))
                                }, ReferenceSpec.Type.ONE), 
                        /*
                         * Assumes every fact for an encounter has the same provider. PCORI CDM requires this.
                         * This is not quite right. It will cause providers to get duplicated x the number of encounters in which each appears. With the i2b2 destination, this appears
                         * harmless because the aiw-i2b2-etl code ends up resolving the duplicates anyway.
                         */
                        new ReferenceSpec("provider", "Providers", 
                                new ColumnSpec[]{
                                    new ColumnSpec(schemaName, VISIT_DIMENSION, new JoinSpec("encounter_num", "encounter_num", new ColumnSpec(schemaName, OBSERVATION_FACT, "provider_id")))
                                }, ReferenceSpec.Type.ONE)
                    /*new ReferenceSpec("chargeAmount", "Hospital Charge Amount", new ColumnSpec[]{new ColumnSpec(schemaName, "ENCOUNTER", "RECORD_ID")}, ReferenceSpec.Type.ONE)*/
                    }, 
                    null, null, null, null, null, AbsoluteTimeGranularity.DAY, POSITION_PARSER, null),
            new EntitySpec("Diagnosis Codes", 
                    null, 
                    this.mapper.readCodes("icd9_diagnosis.txt", 0), 
                    true, 
                    new ColumnSpec(keyIdSchema, keyIdTable, keyIdColumn, new JoinSpec(keyIdJoinKey, "patient_num", new ColumnSpec(schemaName, PATIENT_DIMENSION, new JoinSpec("patient_num", "patient_num", new ColumnSpec(schemaName, VISIT_DIMENSION, new JoinSpec("encounter_num", "encounter_num", new ColumnSpec(schemaName, OBSERVATION_FACT))))))), 
                    new ColumnSpec[]{
                        new ColumnSpec(schemaName, OBSERVATION_FACT, "encounter_num"),
                        new ColumnSpec(schemaName, OBSERVATION_FACT, "concept_cd"),
                        new ColumnSpec(schemaName, OBSERVATION_FACT, "provider_id"),
                        new ColumnSpec(schemaName, OBSERVATION_FACT, "start_date")
                    }, 
                    new ColumnSpec(schemaName, OBSERVATION_FACT, "START_DATE"), 
                    null, 
                    new PropertySpec[]{
                        new PropertySpec("code", null, new ColumnSpec(schemaName, OBSERVATION_FACT, "concept_cd"), ValueType.NOMINALVALUE), 
                        //new PropertySpec("position", null, new ColumnSpec(schemaName, "ENCOUNTER", new JoinSpec("RECORD_ID", "RECORD_ID", new ColumnSpec(schemaName, "DIAGNOSIS", "SEQ_NBR", ColumnSpec.Constraint.EQUAL_TO, this.mapper.propertyNameOrPropIdToSqlCodeArray("icd9_diagnosis_position_07182011.txt")))), ValueType.NOMINALVALUE)
                    }, 
                    null, 
                    null, 
                    new ColumnSpec(schemaName, OBSERVATION_FACT, "concept_cd", ColumnSpec.Constraint.EQUAL_TO, this.mapper.propertyNameOrPropIdToSqlCodeArray("icd9_diagnosis.txt"), false), 
                    null, null, null, 
                    AbsoluteTimeGranularity.DAY, 
                    POSITION_PARSER, 
                    AbsoluteTimeUnit.YEAR),
            new EntitySpec("ICD9 Procedure Codes", 
                    null, 
                    this.mapper.readCodes("icd9_procedure.txt", 0), 
                    true, 
                    new ColumnSpec(keyIdSchema, keyIdTable, keyIdColumn, new JoinSpec(keyIdJoinKey, "patient_num", new ColumnSpec(schemaName, PATIENT_DIMENSION, new JoinSpec("patient_num", "patient_num", new ColumnSpec(schemaName, VISIT_DIMENSION, new JoinSpec("encounter_num", "encounter_num", new ColumnSpec(schemaName, OBSERVATION_FACT))))))), 
                    new ColumnSpec[]{
                        new ColumnSpec(schemaName, OBSERVATION_FACT, "encounter_num"),
                        new ColumnSpec(schemaName, OBSERVATION_FACT, "concept_cd"),
                        new ColumnSpec(schemaName, OBSERVATION_FACT, "provider_id"),
                        new ColumnSpec(schemaName, OBSERVATION_FACT, "start_date")
                    }, 
                    new ColumnSpec(schemaName, OBSERVATION_FACT, "START_DATE"), 
                    null, 
                    new PropertySpec[]{
                        new PropertySpec("code", null, new ColumnSpec(schemaName, OBSERVATION_FACT, "concept_cd"), ValueType.NOMINALVALUE), 
                    }, null, null, 
                    new ColumnSpec(schemaName, OBSERVATION_FACT, "concept_cd", ColumnSpec.Constraint.EQUAL_TO, this.mapper.propertyNameOrPropIdToSqlCodeArray("icd9_procedure.txt"), false), 
                    null, null, null, 
                    AbsoluteTimeGranularity.DAY, 
                    POSITION_PARSER, 
                    null),
        };
        return eventSpecs;
    }

    @Override
    protected EntitySpec[] primitiveParameterSpecs(String keyIdSchema, String keyIdTable, String keyIdColumn, String keyIdJoinKey) throws IOException {
        return new EntitySpec[0];
    }

    @Override
    protected StagingSpec[] stagedSpecs(String keyIdSchema, String keyIdTable, String keyIdColumn, String keyIdJoinKey) throws IOException {
        return new StagingSpec[0];
    }

    @Override
    public GranularityFactory getGranularityFactory() {
        return ABS_TIME_GRANULARITY_FACTORY;
    }

    @Override
    public UnitFactory getUnitFactory() {
        return ABS_TIME_UNIT_FACTORY;
    }

    @Override
    public String getKeyType() {
        return "Patient";
    }
    
}
