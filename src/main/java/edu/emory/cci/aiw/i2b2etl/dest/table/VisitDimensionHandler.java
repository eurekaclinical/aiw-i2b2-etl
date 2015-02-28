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

import edu.emory.cci.aiw.i2b2etl.util.ConnectionSpecRecordHandler;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.MetadataUtil;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.arp.javautil.sql.ConnectionSpec;

/**
 *
 * @author arpost
 */
public class VisitDimensionHandler extends ConnectionSpecRecordHandler<VisitDimension> {

    public static final String TEMP_VISIT_TABLE = "ek_temp_visit";

    public VisitDimensionHandler(ConnectionSpec connSpec) throws SQLException {
        super(connSpec, 
                "insert into " + TEMP_VISIT_TABLE + "(encounter_id, encounter_id_source,"
                + "patient_id, patient_id_source, inout_cd, location_cd, location_path, start_date, end_date, "
                + "visit_blob, update_date, download_date, import_date, sourcesystem_cd, active_status_cd) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
    }

    @Override
    protected void setParameters(PreparedStatement ps, VisitDimension visit) throws SQLException {
        ps.setString(1, visit.getVisitId());
        ps.setString(2, MetadataUtil.toSourceSystemCode(visit.getVisitIdSourceSystem()));
        ps.setString(3, visit.getEncryptedPatientId());
        ps.setString(4, MetadataUtil.toSourceSystemCode(visit.getEncryptedPatientIdSourceSystem()));
        ps.setString(5, visit.getInOut());
        ps.setString(6, null);
        ps.setString(7, null);
        ps.setDate(8, visit.getStartDate());
        ps.setDate(9, visit.getEndDate());
        ps.setObject(10, null);
        ps.setTimestamp(11, visit.getUpdated());
        ps.setTimestamp(12, visit.getDownloaded());
        ps.setTimestamp(13, importTimestamp());
        ps.setString(14, MetadataUtil.toSourceSystemCode(visit.getVisitSourceSystem()));
        ps.setString(15, visit.getActiveStatus());
    }

}
