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
package edu.emory.cci.aiw.i2b2etl.table;

import edu.emory.cci.aiw.i2b2etl.metadata.MetadataUtil;
import java.sql.*;
import java.util.Collection;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Represents records in the patient dimension.
 *
 * The concept dimension has the following DDL:
 * <pre>
 * CREATE TABLE  "PATIENT_DIMENSION"
 *    	(
 *    	"PATIENT_NUM" 		NUMBER(38,0) NOT NULL ENABLE,
 *    	"VITAL_STATUS_CD" 	VARCHAR2(50),
 *    	"BIRTH_DATE" 		DATE,
 *    	"DEATH_DATE" 		DATE,
 *    	"SEX_CD" 			VARCHAR2(50),
 *    	"AGE_IN_YEARS_NUM" 	NUMBER(38,0),
 *    	"LANGUAGE_CD" 		VARCHAR2(50),
 *    	"RACE_CD" 			VARCHAR2(50),
 *    	"MARITAL_STATUS_CD" VARCHAR2(50),
 *    	"RELIGION_CD" 		VARCHAR2(50),
 *    	"ZIP_CD" 			VARCHAR2(10),
 *    	"STATECITYZIP_PATH" VARCHAR2(700),
 *    	"PATIENT_BLOB" 		CLOB,
 *    	"UPDATE_DATE" 		DATE,
 *    	"DOWNLOAD_DATE" 	DATE,
 *    	"IMPORT_DATE" 		DATE,
 *    	"SOURCESYSTEM_CD" 	VARCHAR2(50),
 *    	"UPLOAD_ID" 		NUMBER(38,0),
 *    	CONSTRAINT "PATIENT_DIMENSION_PK" PRIMARY KEY ("PATIENT_NUM")
 *    	)
 * </pre>
 *
 * @author Andrew Post
 */
public class PatientDimension {

    //	This is a related table that we should leverage.
    //
    //  CREATE TABLE PATIENT_MAPPING
    //  (
    //  "PATIENT_IDE"          VARCHAR2(200 BYTE) NOT NULL ENABLE,
    //  "PATIENT_IDE_SOURCE"   VARCHAR2(50 BYTE)  NOT NULL ENABLE,
    //  "PATIENT_NUM"          NUMBER(38,0)       NOT NULL ENABLE,
    //  "PATIENT_IDE_STATUS"   VARCHAR2(50 BYTE),
    //  "UPLOAD_DATE"          DATE,
    //  "UPDATE_DATE"          DATE,
    //  "DOWNLOAD_DATE"        DATE,
    //  "IMPORT_DATE"          DATE,
    //  "SOURCESYSTEM_CD"      VARCHAR2(50 BYTE),
    //  "UPLOAD_ID"            NUMBER(38,0),
    //  CONSTRAINT "PATIENT_MAPPING_PK" PRIMARY KEY ("PATIENT_IDE", "PATIENT_IDE_SOURCE")
    //  )
    private final long patientNum;
    private final String encryptedPatientId;
    private Long ageInYears;
    private final String zip;
    private final String race;
    private final String gender;
    private final String language;
    private final String maritalStatus;
    private final String religion;
    private final VitalStatusCode vital;
    private final Date birthDate;
    private final Date deathDate;
    private final String sourceSystem;
    private static final NumFactory NUM_FACTORY = new IncrNumFactory();
    private static final Logger logger = Logger.getLogger(PatientDimension.class.getName());

    public static final String TEMP_PATIENT_TABLE = "temp_patient";

    public PatientDimension(String encryptedPatientId, String zipCode,
            Long ageInYears,
            String gender, String language, String religion,
            java.util.Date birthDate, java.util.Date deathDate,
            String maritalStatus, String race, String sourceSystem) {
        //Required attributes
        this.patientNum = NUM_FACTORY.getInstance();
        this.zip = zipCode;
        this.birthDate = TableUtil.setDateAttribute(birthDate);
        this.deathDate = TableUtil.setDateAttribute(deathDate);
        this.vital = VitalStatusCode.getInstance(deathDate);

        //Optional attributes
        this.ageInYears = ageInYears;
        this.maritalStatus = maritalStatus;
        this.race = race;
        this.gender = gender;
        this.language = language;
        this.religion = religion;
        this.sourceSystem = sourceSystem;
        this.encryptedPatientId = encryptedPatientId;
    }

    public long getPatientNum() {
        return this.patientNum;
    }

    public String getEncryptedPatientId() {
        return this.encryptedPatientId;
    }

    public String getEncryptedPatientIdSourceSystem() {
        return NUM_FACTORY.getSourceSystem();
    }

    public Long getAgeInYears() {
        return this.ageInYears;
    }

    public static void insertAges(Collection<PatientDimension> patients, Connection cn, String ageConceptCodePrefix, Map<Long, VisitDimension> patientLevelFakeVisits) throws SQLException {
        int batchSize = 1000;
        int counter = 0;
        int commitCounter = 0;
        int commitSize = 10000;
        PreparedStatement ps = null;
        try {
            Timestamp importTimestamp =
                    new Timestamp(System.currentTimeMillis());
            ps = cn.prepareStatement("INSERT INTO OBSERVATION_FACT (ENCOUNTER_NUM, PATIENT_NUM, CONCEPT_CD, PROVIDER_ID, START_DATE, END_DATE, MODIFIER_CD, IMPORT_DATE) VALUES (?, ?, CONCAT('" + ageConceptCodePrefix + ":', ?), '@', ?, ?, 0, ?)");
            for (PatientDimension patient : patients) {
                Long ageInYrs = patient.getAgeInYears();
                if (ageInYrs != null) {
                    try {
                        ps.setLong(1, patientLevelFakeVisits.get(patient.getPatientNum()).getEncounterNum());
                        ps.setLong(2, patient.getPatientNum());
                        ps.setString(3, ageInYrs.toString());
                        ps.setTimestamp(4, importTimestamp);
                        ps.setTimestamp(5, importTimestamp);
                        ps.setTimestamp(6, importTimestamp);
                        ps.addBatch();
                        ps.clearParameters();
                        counter++;
                        commitCounter++;
                        if (counter >= batchSize) {
                            importTimestamp =
                                    new Timestamp(System.currentTimeMillis());
                            ps.executeBatch();
                            ps.clearBatch();
                            counter = 0;
                        }
                        if (commitCounter >= commitSize) {
                            cn.commit();
                            commitCounter = 0;
                        }
                    } catch (SQLException e) {
                        logger.log(Level.SEVERE, "DB_PD_INSERT_FAIL {0}", patient);
                        throw e;
                    }
                }
            }
            if (counter > 0) {
                ps.executeBatch();
                ps.clearBatch();
            }
            if (commitCounter > 0) {
                cn.commit();
            }
            ps.close();
            ps = null;
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                }
            }
        }
    }

    public static void insertAll(Collection<PatientDimension> patients, Connection cn, String projectName) throws SQLException {
        int batchSize = 500;
        int counter = 0;
        int commitSize = 5000;
        int commitCounter = 0;
        PreparedStatement ps = null;
        PreparedStatement ps2 = null;
        try {
            Timestamp importTimestamp =
                    new Timestamp(System.currentTimeMillis());
            ps = cn.prepareStatement("insert into " + TEMP_PATIENT_TABLE + "(patient_num,vital_status_cd,birth_date,death_date,sex_cd," +
                    "age_in_years_num,language_cd,race_cd,marital_status_cd,religion_cd,zip_cd,statecityzip_path,patient_blob,update_date," +
                    "download_date,import_date,sourcesystem_cd) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            ps2 = cn.prepareStatement("insert into PATIENT_MAPPING(patient_ide,patient_ide_source,patient_num,patient_ide_status,project_id,upload_date," +
                    "update_date,download_date,import_date,sourcesystem_cd,upload_id) values (?,?,?,?,?,?,?,?,?,?,?)");
            for (PatientDimension patient : patients) {
                try {
                    ps.setLong(1, patient.patientNum);
                    ps.setString(2, patient.vital.getCode());
                    ps.setDate(3, patient.birthDate);
                    ps.setDate(4, patient.deathDate);
                    ps.setString(5, patient.gender);
                    ps.setObject(6, patient.ageInYears);
                    ps.setString(7, patient.language);
                    ps.setString(8, patient.race);
                    ps.setString(9, patient.maritalStatus);
                    ps.setString(10, patient.religion);
                    ps.setString(11, patient.zip);
                    ps.setString(12, null);
                    ps.setObject(13, null);
                    ps.setTimestamp(14, null);
                    ps.setTimestamp(15, null);
                    ps.setTimestamp(16, importTimestamp);
                    ps.setString(17, MetadataUtil.toSourceSystemCode(patient.sourceSystem));
                    ps.addBatch();
                    ps.clearParameters();

                    ps2.setString(1, patient.encryptedPatientId);
                    ps2.setString(2, MetadataUtil.toSourceSystemCode(NUM_FACTORY.getSourceSystem()));
                    ps2.setLong(3, patient.patientNum);
                    ps2.setString(4, PatientIdeStatusCode.ACTIVE.getCode());
                    ps2.setString(5, projectName);
                    ps2.setDate(6, null);
                    ps2.setDate(7, null);
                    ps2.setDate(8, null);
                    ps2.setTimestamp(9, importTimestamp);
                    ps2.setString(10, MetadataUtil.toSourceSystemCode(patient.sourceSystem));
                    ps2.setNull(11, Types.NUMERIC);
                    ps2.addBatch();
                    ps2.clearParameters();

                    ps2.setLong(1, patient.patientNum);
                    ps2.setString(2, "HIVE");
                    ps2.setLong(3, patient.patientNum);
                    ps2.setString(4, PatientIdeStatusCode.ACTIVE.getCode());
                    ps2.setString(5, projectName);
                    ps2.setDate(6, null);
                    ps2.setDate(7, null);
                    ps2.setDate(8, null);
                    ps2.setTimestamp(9, importTimestamp);
                    ps2.setString(10, null);
                    ps2.setNull(11, Types.NUMERIC);
                    ps2.addBatch();
                    ps2.clearParameters();

                    counter++;
                    commitCounter++;

                    if (counter >= batchSize) {
                        importTimestamp =
                                new Timestamp(System.currentTimeMillis());
                        ps.executeBatch();
                        ps.clearBatch();
                        ps2.executeBatch();
                        ps2.clearBatch();
                        counter = 0;
                    }
                    if (commitCounter >= commitSize) {
                        cn.commit();
                        commitSize = 0;
                    }

                    logger.log(Level.FINEST, "DB_PD_INSERT {0}", patient);
                } catch (SQLException e) {
                    logger.log(Level.SEVERE, "DB_PD_INSERT_FAIL {0}", patient);
                    throw e;
                }
            }
            if (counter > 0) {
                ps.executeBatch();
                ps.clearBatch();
                ps2.executeBatch();
                ps2.clearBatch();
                cn.commit();
            }
            ps.close();
            ps = null;
            ps2.close();
            ps2 = null;
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                }
            }
            if (ps2 != null) {
                try {
                    ps2.close();
                } catch (SQLException e) {
                }
            }
        }
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}