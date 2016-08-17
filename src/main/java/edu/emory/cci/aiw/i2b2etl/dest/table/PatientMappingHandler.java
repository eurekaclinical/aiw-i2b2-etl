package edu.emory.cci.aiw.i2b2etl.dest.table;

/*
 * #%L
 * AIW i2b2 ETL
 * %%
 * Copyright (C) 2012 - 2015 Emory University
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

import edu.emory.cci.aiw.i2b2etl.util.RecordHandler;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.arp.javautil.sql.ConnectionSpec;

/**
 *
 * @author arpost
 */
public class PatientMappingHandler extends RecordHandler<PatientDimension> {
    public static final String TEMP_PATIENT_MAPPING_TABLE = "ek_temp_patient_mapping";
    
    public PatientMappingHandler(ConnectionSpec connSpec) throws SQLException {
        super(connSpec,
                "insert into " + TEMP_PATIENT_MAPPING_TABLE + " (patient_id,patient_id_source,patient_map_id,patient_map_id_source,patient_map_id_status," +
                    "update_date,download_date,import_date,sourcesystem_cd,delete_date) values (?,?,?,?,?,?,?,?,?,?)");
    }

    @Override
    protected void setParameters(PreparedStatement ps2, PatientDimension patient) throws SQLException {
        ps2.setString(1, patient.getEncryptedPatientId());
        ps2.setString(2, patient.getEncryptedPatientIdSource());
        ps2.setString(3, patient.getEncryptedPatientId());
        ps2.setString(4, patient.getEncryptedPatientIdSource());
        ps2.setString(5, PatientIdeStatusCode.ACTIVE.getCode());
        ps2.setTimestamp(6, patient.getUpdated());
        ps2.setTimestamp(7, patient.getDownloaded());
        ps2.setTimestamp(8, importTimestamp());
        ps2.setString(9, patient.getSourceSystem());
        ps2.setTimestamp(10,patient.getDeletedDate());
    }
    
}
