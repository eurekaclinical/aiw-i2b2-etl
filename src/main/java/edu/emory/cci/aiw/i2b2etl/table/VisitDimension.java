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
    private final Date updateDate;
    private final Date downloadDate;

    public VisitDimension(long patientNum, String encryptedPatientId,
            java.util.Date startDate, java.util.Date endDate,
            String encryptedVisitId, String visitSourceSystem,
            String encryptedPatientIdSourceSystem,
            Date downloadDate, Date updateDate) {
        this.encounterNum = NUM_FACTORY.getInstance();
        this.encryptedVisitId = TableUtil.setStringAttribute(encryptedVisitId);
        this.patientNum = patientNum;
        this.encryptedPatientId = TableUtil.setStringAttribute(encryptedPatientId);
        this.startDate = TableUtil.setDateAttribute(startDate);
        this.endDate = TableUtil.setDateAttribute(endDate);
        this.visitSourceSystem = visitSourceSystem;
        this.encryptedPatientIdSourceSystem = encryptedPatientIdSourceSystem;
        this.activeStatus = ActiveStatusCode.getInstance(true, startDate, endDate);
        this.downloadDate = downloadDate;
        this.updateDate = updateDate;
    }

    public long getEncounterNum() {
        return this.encounterNum;
    }

    public String getEncryptedVisitIdSourceSystem() {
        return NUM_FACTORY.getSourceSystem();
    }

    public Date getUpdateDate() {
        return updateDate;
    }

    public Date getDownloadDate() {
        return downloadDate;
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
            ps = cn.prepareStatement("insert into VISIT_DIMENSION(encounter_num," +
                    "patient_num,active_status_cd,start_date,end_date,inout_cd,location_cd,location_path,visit_blob," +
                    "update_date,download_date,import_date,sourcesystem_cd,upload_id,length_of_stay) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            ps2 = cn.prepareStatement("insert into ENCOUNTER_MAPPING(encounter_ide,encounter_ide_source,encounter_num,patient_ide," +
                    "patient_ide_source,encounter_ide_status,update_date,upload_date,download_date,import_date,sourcesystem_cd,upload_id,project_id)" +
                    " values (?,?,?,?,?,?,?,?,?,?,?,?,?)");

            for (VisitDimension visit : visits) {
                try {
                    ps.setLong(1, visit.encounterNum);
                    ps.setLong(2, visit.patientNum);
                    ps.setString(3, visit.activeStatus.getCode());
                    ps.setDate(4, visit.startDate);
                    ps.setDate(5, visit.endDate);
                    ps.setString(6, null);
                    ps.setString(7, null);
                    ps.setString(8, null);
                    ps.setObject(9, null);
                    ps.setTimestamp(10, TableUtil.setTimestampAttribute(visit.updateDate));
                    ps.setTimestamp(11, TableUtil.setTimestampAttribute(visit.downloadDate));
                    ps.setTimestamp(12, importTimestamp);
                    ps.setString(13, MetadataUtil.toSourceSystemCode(visit.visitSourceSystem));
                    ps.setObject(14, null);
                    ps.setObject(15, null);
                    ps.addBatch();
                    ps.clearParameters();

                    if (!visit.encryptedVisitId.equals("@")) {
                        ps2.setString(1, visit.encryptedVisitId);
                        ps2.setString(2, MetadataUtil.toSourceSystemCode(NUM_FACTORY.getSourceSystem()));
                        ps2.setLong(3, visit.encounterNum);
                        ps2.setString(4, visit.encryptedPatientId);
                        ps2.setString(5, MetadataUtil.toSourceSystemCode(visit.encryptedPatientIdSourceSystem));
                        ps2.setString(6, EncounterIdeStatusCode.ACTIVE.getCode());
                        ps2.setDate(7, null);
                        ps2.setDate(8, null);
                        ps2.setDate(9, null);
                        ps2.setTimestamp(10, importTimestamp);
                        ps2.setString(11, MetadataUtil.toSourceSystemCode(visit.visitSourceSystem));
                        ps2.setNull(12, Types.NUMERIC);
                        ps2.setString(13, projectName);
                        ps2.addBatch();
                        ps2.clearParameters();
                        
                        ps2.setLong(1, visit.encounterNum);
                        ps2.setString(2, "HIVE");
                        ps2.setLong(3, visit.encounterNum);
                        ps2.setLong(4, visit.patientNum);
                        ps2.setString(5, "HIVE");
                        ps2.setString(6, EncounterIdeStatusCode.ACTIVE.getCode());
                        ps2.setDate(7, null);
                        ps2.setDate(8, null);
                        ps2.setDate(9, null);
                        ps2.setTimestamp(10, importTimestamp);
                        ps2.setString(11, null);
                        ps2.setNull(12, Types.NUMERIC);
                        ps2.setString(13, projectName);
                        ps2.addBatch();
                        ps2.clearParameters();
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
