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
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

public class VisitDimension {

    //	there should be one instance for each visit.
    //	cached on encounter_num.
    //    	CREATE TABLE  "VISIT_DIMENSION" 
    // 	    (
    // 		"ENCOUNTER_NUM"		NUMBER(38,0) NOT NULL ENABLE, 
    // 		"PATIENT_NUM"		NUMBER(38,0) NOT NULL ENABLE, 
    // 		"ACTIVE_STATUS_CD"	VARCHAR2(50), 
    // 		"START_DATE"		DATE, 
    // 		"END_DATE"			DATE, 
    // 		"INOUT_CD"			VARCHAR2(50), 
    // 		"LOCATION_CD"		VARCHAR2(50), 
    // 		"LOCATION_PATH"		VARCHAR2(900), 
    // 		"VISIT_BLOB"		CLOB, 
    // 		"UPDATE_DATE"		DATE, 
    // 		"DOWNLOAD_DATE"		DATE, 
    // 		"IMPORT_DATE"		DATE, 
    // 		"SOURCESYSTEM_CD"	VARCHAR2(50), 
    // 		"UPLOAD_ID"			NUMBER(38,0), 
    // 		 CONSTRAINT "VISIT_DIMENSION_PK" PRIMARY KEY ("ENCOUNTER_NUM", "PATIENT_NUM") ENABLE
    // 	    )
    private final Long encounterNum;
    private final String encryptedVisitId;
    private final long patientNum;
    private final Date startDate;
    private final Date endDate;
    private final String visitSourceSystem;
    private final String encryptedPatientIdSourceSystem;
    private final ActiveStatusCode activeStatus;
    private final String encryptedPatientId;
    private static final Logger logger = Logger.getLogger(VisitDimension.class.getName());
    private static final NumFactory NUM_FACTORY = new IncrNumFactory();
    private final Timestamp updateDate;
    private final Timestamp downloadDate;

    public static final String TEMP_VISIT_TABLE = "temp_visit";
    public static final String TEMP_ENC_MAPPING_TABLE = "temp_encounter_mapping";

    public VisitDimension(long patientNum, String encryptedPatientId,
            java.util.Date startDate, java.util.Date endDate,
            String encryptedVisitId, String visitSourceSystem,
            String encryptedPatientIdSourceSystem,
            java.util.Date downloadDate, java.util.Date updateDate) {
        this.encounterNum = NUM_FACTORY.getInstance();
        this.encryptedVisitId = TableUtil.setStringAttribute(encryptedVisitId);
        this.patientNum = patientNum;
        this.encryptedPatientId = TableUtil.setStringAttribute(encryptedPatientId);
        this.startDate = TableUtil.setDateAttribute(startDate);
        this.endDate = TableUtil.setDateAttribute(endDate);
        this.visitSourceSystem = visitSourceSystem;
        this.encryptedPatientIdSourceSystem = encryptedPatientIdSourceSystem;
        this.activeStatus = ActiveStatusCode.getInstance(true, startDate, endDate);
        this.downloadDate = TableUtil.setTimestampAttribute(downloadDate);
        this.updateDate = TableUtil.setTimestampAttribute(updateDate);
    }

    public long getEncounterNum() {
        return this.encounterNum;
    }

    public String getEncryptedVisitId() {
        return this.encryptedVisitId;
    }

    public String getEncryptedVisitIdSourceSystem() {
        return NUM_FACTORY.getSourceSystem();
    }

    public static void insertAll(Collection<VisitDimension> visits, Connection cn, String projectName) throws SQLException {
        int batchSize = 500;
        int commitSize = 5000;
        int batchCounter = 0;
        int commitCounter = 0;
        boolean ps2BatchAdded = false;
        PreparedStatement ps = null;
        PreparedStatement ps2 = null;
        try {
            Timestamp importTimestamp =
                    new Timestamp(System.currentTimeMillis());
            ps = cn.prepareStatement("insert into " + TEMP_VISIT_TABLE + "(encounter_id, encounter_id_source," +
                    "patient_id, patient_id_source, encounter_num, inout_cd, location_cd, location_path, start_date, end_date, " +
                    "visit_blob, update_date, download_date, import_date, sourcesystem_cd) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            ps2 = cn.prepareStatement("insert into " + TEMP_ENC_MAPPING_TABLE + "(encounter_id, encounter_id_source, encounter_map_id, encounter_map_id_source, " +
                    "encounter_map_id_status, encounter_num, patient_map_id, patient_map_id_source, update_date, download_date, import_date, sourcesystem_cd)" +
                    " values (?,?,?,?,?,?,?,?,?,?,?,?)");

            for (VisitDimension visit : visits) {
                try {
                    ps.setString(1, visit.encryptedVisitId);
                    ps.setString(2, MetadataUtil.toSourceSystemCode(NUM_FACTORY.getSourceSystem()));
                    ps.setString(3, String.valueOf(visit.encryptedPatientId));
                    ps.setString(4, MetadataUtil.toSourceSystemCode(visit.encryptedPatientIdSourceSystem));
                    ps.setLong(5, visit.encounterNum);
                    ps.setString(6, null);
                    ps.setString(7, null);
                    ps.setString(8, null);
                    ps.setDate(9, visit.startDate);
                    ps.setDate(10, visit.endDate);
                    ps.setObject(11, null);
                    ps.setDate(12, null);
                    ps.setDate(13, null);
                    ps.setDate(14, new java.sql.Date(importTimestamp.getTime()));
                    ps.setString(15, MetadataUtil.toSourceSystemCode(visit.visitSourceSystem));

                    ps.addBatch();
                    ps.clearParameters();

                    if (!visit.encryptedVisitId.equals("@")) {
                        ps2.setString(1, visit.encryptedVisitId);
                        ps2.setString(2, MetadataUtil.toSourceSystemCode(NUM_FACTORY.getSourceSystem()));
                        ps2.setString(3, visit.encryptedVisitId);
                        ps2.setString(4, MetadataUtil.toSourceSystemCode(NUM_FACTORY.getSourceSystem()));
                        ps2.setString(5, EncounterIdeStatusCode.ACTIVE.getCode());
                        ps2.setLong(6, visit.encounterNum);
                        ps2.setString(7, visit.encryptedPatientId);
                        ps2.setString(8, MetadataUtil.toSourceSystemCode(visit.encryptedPatientIdSourceSystem));
                        ps2.setDate(9, null);
                        ps2.setDate(10, null);
                        ps2.setTimestamp(11, importTimestamp);
                        ps2.setString(12, MetadataUtil.toSourceSystemCode(visit.visitSourceSystem));
                        ps2.addBatch();
                        ps2.clearParameters();

//                        ps2.setString(1, visit.encryptedVisitId);
//                        ps2.setString(2, "HIVE");
//                        ps2.setString(3, visit.encryptedVisitId);
//                        ps2.setString(4, "HIVE");
//                        ps2.setString(5, EncounterIdeStatusCode.ACTIVE.getCode());
//                        ps2.setLong(6, visit.encounterNum);
//                        ps2.setString(7, visit.encryptedPatientId);
//                        ps2.setString(8, "HIVE");
//                        ps2.setDate(9, null);
//                        ps2.setDate(10, null);
//                        ps2.setTimestamp(11, importTimestamp);
//                        ps2.setString(12, "HIVE");
//                        ps2.addBatch();
//                        ps2.clearParameters();

                        ps2BatchAdded = true;
                    }
                    batchCounter++;
                    commitCounter++;

                    if (batchCounter >= batchSize) {
                        importTimestamp =
                                new Timestamp(System.currentTimeMillis());
                        ps.executeBatch();
                        ps.clearBatch();
                        if (ps2BatchAdded) {
                            ps2.executeBatch();
                            ps2.clearBatch();
                            ps2BatchAdded = false;
                        }
                        batchCounter = 0;
                    }
                    if (commitCounter >= commitSize) {
                        cn.commit();
                        commitCounter = 0;
                    }

                    logger.log(Level.FINEST, "DB_VD_INSERT {0}", visit);
                } catch (SQLException e) {
                    logger.log(Level.SEVERE, "DB_VD_INSERT_FAIL {0}", visit);
                    throw e;
                }
            }
            if (batchCounter > 0) {
                ps.executeBatch();
                ps.clearBatch();
                if (ps2BatchAdded) {
                    ps2.executeBatch();
                    ps2.clearBatch();
                }
                cn.commit();
            }
            if (commitCounter > 0) {
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
