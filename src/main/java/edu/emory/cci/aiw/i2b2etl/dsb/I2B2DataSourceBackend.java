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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;
import org.arp.javautil.sql.InvalidConnectionSpecArguments;
import org.protempa.DataSourceReadException;
import org.protempa.KeySetSpec;
import org.protempa.backend.BackendInitializationException;
import org.protempa.backend.BackendInstanceSpec;
import org.protempa.backend.annotations.BackendInfo;
import org.protempa.backend.annotations.BackendProperty;
import org.protempa.backend.dsb.relationaldb.ColumnSpec;
import org.protempa.backend.dsb.relationaldb.Operator;
import org.protempa.backend.dsb.relationaldb.EntitySpec;
import org.protempa.backend.dsb.relationaldb.JDBCDateTimeTimestampDateValueFormat;
import org.protempa.backend.dsb.relationaldb.JDBCDateTimeTimestampPositionParser;
import org.protempa.backend.dsb.relationaldb.JDBCPositionFormat;
import org.protempa.backend.dsb.relationaldb.JoinSpec;
import org.protempa.backend.dsb.relationaldb.PropertySpec;
import org.protempa.backend.dsb.relationaldb.ReferenceSpec;
import org.protempa.backend.dsb.relationaldb.RelationalDbDataSourceBackend;
import org.protempa.backend.dsb.relationaldb.mappings.DefaultMappings;
import org.protempa.backend.dsb.relationaldb.mappings.Mappings;
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

    private final static AbsoluteTimeUnitFactory ABS_TIME_UNIT_FACTORY
            = new AbsoluteTimeUnitFactory();
    private final static AbsoluteTimeGranularityFactory ABS_TIME_GRANULARITY_FACTORY
            = new AbsoluteTimeGranularityFactory();
    private static final JDBCPositionFormat POSITION_PARSER
            = new JDBCDateTimeTimestampPositionParser();

    private final static String PATIENT_DIMENSION = "patient_dimension";
    private final static String VISIT_DIMENSION = "visit_dimension";
    private final static String OBSERVATION_FACT = "observation_fact";
    private final static String PROVIDER_DIMENSION = "provider_dimension";

    private final static Logger LOGGER
            = Logger.getLogger(I2B2DataSourceBackend.class.getPackage().getName());

    private Long resultInstanceId;

    public I2B2DataSourceBackend() {
        setDefaultKeyIdTable(PATIENT_DIMENSION);
        /*
         * Per the i2b2 1.7 CRC design docs and stored procedure 
         * implementation, 
         * to_char(patient_num) = patient_ide when patient_ide_source = 'HIVE'.
         */
        setDefaultKeyIdColumn("patient_num");
        setDefaultKeyIdJoinKey("patient_num");
    }

    @Override
    public void initialize(BackendInstanceSpec config) throws BackendInitializationException {
        super.initialize(config);
    }

    @Override
    protected EntitySpec[] constantSpecs(String keyIdSchema, String keyIdTable, String keyIdColumn, String keyIdJoinKey) throws IOException {
        Mappings riId = new DefaultMappings(new HashMap<Object, String>() {
            {
                put(resultInstanceId, "" + resultInstanceId);
            }
        });
        Mappings hive = new DefaultMappings(new HashMap<Object, String>() {
            {
                put("HIVE", "HIVE");
            }
        });
        String schemaName = getSchemaName();
        return new EntitySpec[]{
            new EntitySpec("Patients",
            null,
            new String[]{"Patient"},
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
                new ReferenceSpec("patientDetails", "Patient Details", new ColumnSpec[]{new ColumnSpec(keyIdSchema, keyIdTable, keyIdColumn)}, ReferenceSpec.Type.MANY),
                new ReferenceSpec("patientAliases", "Patient Aliases", new ColumnSpec[]{new ColumnSpec(keyIdSchema, keyIdTable, new JoinSpec(keyIdJoinKey, "patient_num", new ColumnSpec(schemaName, "PATIENT_MAPPING", "PATIENT_IDE")))}, ReferenceSpec.Type.MANY)
            },
            null, null,
            isInKeySetMode()
            ? new ColumnSpec[]{
                new ColumnSpec(keyIdSchema, keyIdTable, "RESULT_INSTANCE_ID", Operator.EQUAL_TO, riId)
            }
            : null,
            null, null, null, null, null),
            new EntitySpec("Patient Aliases",
            null,
            new String[]{"PatientAlias"},
            true,
            new ColumnSpec(keyIdSchema, keyIdTable, keyIdColumn, new JoinSpec(keyIdJoinKey, "patient_num", new ColumnSpec(schemaName, "PATIENT_MAPPING"))),
            new ColumnSpec[]{
                new ColumnSpec(schemaName, "PATIENT_MAPPING", "PATIENT_IDE"),
                new ColumnSpec(schemaName, "PATIENT_MAPPING", "PATIENT_IDE_SOURCE")
            },
            null,
            null,
            new PropertySpec[]{
                new PropertySpec("patientId", null, new ColumnSpec(schemaName, "PATIENT_MAPPING", "PATIENT_IDE"), ValueType.NOMINALVALUE),
                new PropertySpec("fieldName", null, new ColumnSpec(schemaName, "PATIENT_MAPPING", "PATIENT_IDE_SOURCE"), ValueType.NOMINALVALUE)
            },
            null,
            null, null,
            isInKeySetMode()
            ? new ColumnSpec[]{
                new ColumnSpec(schemaName, "PATIENT_MAPPING", new JoinSpec("patient_num", keyIdJoinKey, new ColumnSpec(keyIdSchema, keyIdTable, "RESULT_INSTANCE_ID", Operator.EQUAL_TO, riId))),
                new ColumnSpec(schemaName, "PATIENT_MAPPING", "PATIENT_IDE_SOURCE", Operator.NOT_EQUAL_TO, hive)
            }
            : new ColumnSpec[]{new ColumnSpec(schemaName, "PATIENT_MAPPING", "PATIENT_IDE_SOURCE", Operator.NOT_EQUAL_TO, hive)},
            null, null, null, null, null),
            new EntitySpec("Patient Details",
            null,
            new String[]{"PatientDetails"},
            true,
            new ColumnSpec(keyIdSchema, keyIdTable, keyIdColumn, new JoinSpec(keyIdJoinKey, "patient_num", new ColumnSpec(schemaName, PATIENT_DIMENSION))),
            new ColumnSpec[]{
                new ColumnSpec(schemaName, PATIENT_DIMENSION, "patient_num")
            },
            null,
            null,
            new PropertySpec[]{
                new PropertySpec("patientId", null, new ColumnSpec(schemaName, PATIENT_DIMENSION, "patient_num"), ValueType.NOMINALVALUE),
                new PropertySpec("gender", null, new ColumnSpec(schemaName, PATIENT_DIMENSION, "SEX_CD", Operator.EQUAL_TO, getMappingsFactory().getInstance("gender.txt"), true), ValueType.NOMINALVALUE),
                new PropertySpec("race", null, new ColumnSpec(schemaName, PATIENT_DIMENSION, "RACE_CD", Operator.EQUAL_TO, getMappingsFactory().getInstance("race.txt"), true), ValueType.NOMINALVALUE),
                new PropertySpec("ethnicity", null, new ColumnSpec(schemaName, PATIENT_DIMENSION, "RACE_CD", Operator.EQUAL_TO, getMappingsFactory().getInstance("ethnicity.txt"), true), ValueType.NOMINALVALUE),
                new PropertySpec("dateOfBirth", null, new ColumnSpec(schemaName, PATIENT_DIMENSION, "BIRTH_DATE"), ValueType.DATEVALUE, new JDBCDateTimeTimestampDateValueFormat()),
                new PropertySpec("language", null, new ColumnSpec(schemaName, PATIENT_DIMENSION, "LANGUAGE_CD", Operator.EQUAL_TO, getMappingsFactory().getInstance("language.txt"), true), ValueType.NOMINALVALUE),
                new PropertySpec("maritalStatus", null, new ColumnSpec(schemaName, PATIENT_DIMENSION, "MARITAL_STATUS_CD", Operator.EQUAL_TO, getMappingsFactory().getInstance("marital_status.txt"), true), ValueType.NOMINALVALUE),},
            new ReferenceSpec[]{
                new ReferenceSpec("encounters", "Encounters",
                new ColumnSpec[]{
                    new ColumnSpec(schemaName, PATIENT_DIMENSION, new JoinSpec("patient_num", "patient_num", new ColumnSpec(schemaName, VISIT_DIMENSION, "encounter_num")))
                }, ReferenceSpec.Type.MANY),
                new ReferenceSpec("patient", "Patients",
                new ColumnSpec[]{new ColumnSpec(schemaName, PATIENT_DIMENSION, "patient_num")
                }, ReferenceSpec.Type.ONE)
            },
            null, null,
            isInKeySetMode()
            ? new ColumnSpec[]{
                new ColumnSpec(schemaName, PATIENT_DIMENSION, new JoinSpec("patient_num", keyIdJoinKey, new ColumnSpec(keyIdSchema, keyIdTable, "RESULT_INSTANCE_ID", Operator.EQUAL_TO, riId)))}
            : null,
            null, null, null, null, null),
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
            }, null, null, null, null, null, null, null, null, null),};
    }

    @Override
    protected EntitySpec[] eventSpecs(String keyIdSchema, String keyIdTable, String keyIdColumn, String keyIdJoinKey) throws IOException {
        Mappings riId = new DefaultMappings(new HashMap<Object, String>() {
            {
                put(resultInstanceId, "" + resultInstanceId);
            }
        });
        String schemaName = getSchemaName();
        Mappings icd9DxMappings = getMappingsFactory().getInstance("icd9_diagnosis.txt");
        Mappings icd9PxMappings = getMappingsFactory().getInstance("icd9_procedure.txt");
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
                new PropertySpec("encounterId", null, new ColumnSpec(schemaName, VISIT_DIMENSION, "encounter_num"), ValueType.NOMINALVALUE), /*new PropertySpec("type", null, new ColumnSpec(schemaName, "ENCOUNTER", "PATIENTTYPE", ColumnSpec.Operator.EQUAL_TO, getMappingsFactory().propertyNameOrPropIdToSqlCodeArray("type_encounter_07182011.txt"), true), ValueType.NOMINALVALUE), 
             new PropertySpec("healthcareEntity", null, new ColumnSpec(schemaName, "ENCOUNTER", "UNIVCODE", ColumnSpec.Operator.EQUAL_TO, getMappingsFactory().propertyNameOrPropIdToSqlCodeArray("entity_healthcare_07182011.txt"), true), ValueType.NOMINALVALUE), 
             new PropertySpec("dischargeDisposition", null, new ColumnSpec(schemaName, "ENCOUNTER", "DISCHARGESTATUSCODE", ColumnSpec.Operator.EQUAL_TO, getMappingsFactory().propertyNameOrPropIdToSqlCodeArray("disposition_discharge_07182011.txt"), true), ValueType.NOMINALVALUE), 
             new PropertySpec("dischargeDispositionCat", null, new ColumnSpec(schemaName, "ENCOUNTER", "DISCHARGESTATUSCODE", ColumnSpec.Operator.EQUAL_TO, getMappingsFactory().propertyNameOrPropIdToSqlCodeArray("disposition_discharge_category_10072011.txt"), true), ValueType.NOMINALVALUE), 
             new PropertySpec("aprdrgRiskMortalityValue", null, new ColumnSpec(schemaName, "ENCOUNTER", "APRRISKOFMORTALITY"), ValueType.NUMERICALVALUE), new PropertySpec("aprdrgSeverityValue", null, new ColumnSpec(schemaName, "ENCOUNTER", "APRSEVERITYOFILLNESS"), ValueType.NOMINALVALUE), 
             new PropertySpec("insuranceType", null, new ColumnSpec(schemaName, "ENCOUNTER", "HOSPITALPRIMARYPAYER", ColumnSpec.Operator.EQUAL_TO, getMappingsFactory().propertyNameOrPropIdToSqlCodeArray("insurance_types_07182011.txt")), ValueType.NOMINALVALUE)*/},
            new ReferenceSpec[]{
                new ReferenceSpec("patient", "Patients",
                new ColumnSpec[]{
                    new ColumnSpec(schemaName, VISIT_DIMENSION, "patient_num")
                }, ReferenceSpec.Type.ONE),
                new ReferenceSpec("patientDetails", "Patient Details",
                new ColumnSpec[]{
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
            null, null, isInKeySetMode()
            ? new ColumnSpec[]{
                new ColumnSpec(schemaName, VISIT_DIMENSION, new JoinSpec("patient_num", keyIdJoinKey, new ColumnSpec(keyIdSchema, keyIdTable, "RESULT_INSTANCE_ID", Operator.EQUAL_TO, riId)))}
            : null, null, null, AbsoluteTimeGranularity.DAY, POSITION_PARSER, null),
            new EntitySpec("Diagnosis Codes",
            null,
            icd9DxMappings.readTargets(),
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
                new PropertySpec("code", null, new ColumnSpec(schemaName, OBSERVATION_FACT, "concept_cd"), ValueType.NOMINALVALUE), //new PropertySpec("position", null, new ColumnSpec(schemaName, "ENCOUNTER", new JoinSpec("RECORD_ID", "RECORD_ID", new ColumnSpec(schemaName, "DIAGNOSIS", "SEQ_NBR", ColumnSpec.Operator.EQUAL_TO, getMappingsFactory().propertyNameOrPropIdToSqlCodeArray("icd9_diagnosis_position_07182011.txt")))), ValueType.NOMINALVALUE)
            },
            null,
            null,
            new ColumnSpec(schemaName, OBSERVATION_FACT, "concept_cd", Operator.EQUAL_TO, icd9DxMappings, false),
            isInKeySetMode()
            ? new ColumnSpec[]{
                new ColumnSpec(schemaName, OBSERVATION_FACT, new JoinSpec("encounter_num", "encounter_num", new ColumnSpec(schemaName, VISIT_DIMENSION, new JoinSpec("patient_num", keyIdJoinKey, new ColumnSpec(keyIdSchema, keyIdTable, "RESULT_INSTANCE_ID", Operator.EQUAL_TO, riId)))))}
            : null, null, null,
            AbsoluteTimeGranularity.DAY,
            POSITION_PARSER,
            AbsoluteTimeUnit.YEAR),
            new EntitySpec("ICD9 Procedure Codes",
            null,
            icd9PxMappings.readTargets(),
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
                new PropertySpec("code", null, new ColumnSpec(schemaName, OBSERVATION_FACT, "concept_cd"), ValueType.NOMINALVALUE),}, null, null,
            new ColumnSpec(schemaName, OBSERVATION_FACT, "concept_cd", Operator.EQUAL_TO, icd9PxMappings, false),
            isInKeySetMode()
            ? new ColumnSpec[]{
                new ColumnSpec(schemaName, OBSERVATION_FACT, new JoinSpec("encounter_num", "encounter_num", new ColumnSpec(schemaName, VISIT_DIMENSION, new JoinSpec("patient_num", keyIdJoinKey, new ColumnSpec(keyIdSchema, keyIdTable, "RESULT_INSTANCE_ID", Operator.EQUAL_TO, riId)))))}
            : null, null, null,
            AbsoluteTimeGranularity.DAY,
            POSITION_PARSER,
            null),};
        return eventSpecs;
    }

    @Override
    protected EntitySpec[] primitiveParameterSpecs(String keyIdSchema, String keyIdTable, String keyIdColumn, String keyIdJoinKey) throws IOException {
        return new EntitySpec[0];
    }

    @Override
    public GranularityFactory getGranularityFactory() {
        return ABS_TIME_GRANULARITY_FACTORY;
    }

    @Override
    public UnitFactory getUnitFactory() {
        return ABS_TIME_UNIT_FACTORY;
    }

    @BackendProperty(displayName = "Query Master ID")
    public void setResultInstanceId(Long resultInstanceId) {
        this.resultInstanceId = resultInstanceId;
        setKeyLoaderKeyIdSchema(getSchemaName());
        setKeyLoaderKeyIdTable("QT_PATIENT_SET_COLLECTION");
        setKeyLoaderKeyIdColumn("PATIENT_NUM");
        setKeyLoaderKeyIdJoinKey("PATIENT_NUM");
    }

    public Long getResultInstanceId() {
        return this.resultInstanceId;
    }

    @Override
    public KeySetSpec[] getSelectedKeySetSpecs() throws DataSourceReadException {
        List<KeySetSpec> result = new ArrayList<>();
        if (this.resultInstanceId != null) {
            try (Connection con = this.getConnectionSpecInstance().getOrCreate();
                    PreparedStatement stmt = con.prepareStatement("SELECT A1.NAME, A1.USER_ID, A3.DESCRIPTION FROM QT_QUERY_MASTER A1 JOIN QT_QUERY_INSTANCE A2 ON (A1.QUERY_MASTER_ID=A2.QUERY_MASTER_ID) JOIN QT_QUERY_RESULT_INSTANCE A3 ON (A2.QUERY_INSTANCE_ID=A3.QUERY_INSTANCE_ID) WHERE A3.RESULT_INSTANCE_ID = ? AND A1.DELETE_FLAG = 'N' AND A2.DELETE_FLAG = 'N' AND A3.RESULT_TYPE_ID = 1")) {
                stmt.setLong(1, this.resultInstanceId);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        result.add(new KeySetSpec(getSourceSystem(), "" + this.resultInstanceId, rs.getString(1) + " (" + rs.getString(2) + ")", rs.getString(3)));
                    }
                }
            } catch (SQLException | InvalidConnectionSpecArguments ex) {
                throw new DataSourceReadException(ex);
            }
        }
        return result.toArray(new KeySetSpec[result.size()]);
    }

    @Override
    public String getKeyType() {
        return "Patient";
    }

}
