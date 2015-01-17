package edu.emory.cci.aiw.i2b2etl.table;

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

import edu.emory.cci.aiw.i2b2etl.metadata.MetadataUtil;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.arp.javautil.sql.ConnectionSpec;

/**
 *
 * @author arpost
 */
public class PatientDimensionHandler extends RecordHandler<PatientDimension> {

    public static final String TEMP_PATIENT_TABLE = "temp_patient";

    public PatientDimensionHandler(ConnectionSpec connSpec) throws SQLException {
        super(connSpec,
                "insert into " + TEMP_PATIENT_TABLE + "(patient_id,patient_id_source,vital_status_cd,birth_date,death_date,sex_cd,"
                + "age_in_years_num,language_cd,race_cd,marital_status_cd,religion_cd,zip_cd,statecityzip_path,patient_blob,update_date,"
                + "download_date,import_date,sourcesystem_cd) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
    }

    @Override
    protected void setParameters(PreparedStatement ps, PatientDimension patient) throws SQLException {
        ps.setString(1, patient.getEncryptedPatientId());
        ps.setString(2, MetadataUtil.toSourceSystemCode(patient.getEncryptedPatientIdSourceSystem()));
        ps.setString(3, patient.getVital().getCode());
        ps.setDate(4, patient.getBirthDate());
        ps.setDate(5, patient.getDeathDate());
        ps.setString(6, patient.getGender());
        ps.setObject(7, patient.getAgeInYears());
        ps.setString(8, patient.getLanguage());
        ps.setString(9, patient.getRace());
        ps.setString(10, patient.getMaritalStatus());
        ps.setString(11, patient.getReligion());
        ps.setString(12, patient.getZip());
        ps.setString(13, null);
        ps.setObject(14, null);
        ps.setTimestamp(15, null);
        ps.setTimestamp(16, null);
        ps.setTimestamp(17, importTimestamp());
        ps.setString(18, MetadataUtil.toSourceSystemCode(patient.getSourceSystem()));
    }

}
