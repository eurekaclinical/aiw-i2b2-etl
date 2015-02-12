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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.commons.lang3.StringUtils;
import org.arp.javautil.sql.ConnectionSpec;

/**
 *
 * @author Andrew Post
 */
public class RejectedFactHandler extends AbstractFactHandler {
    
    public static final String REJECTED_FACT_TABLE = "REJECTED_OBSERVATION_FACT";

    public RejectedFactHandler(ConnectionSpec connSpec, String table) throws SQLException {
        super(connSpec, "insert into " + table + "(encounter_id, encounter_id_source, concept_cd, " +
                            "patient_id, patient_id_source, provider_id, start_date, modifier_cd, instance_num, valtype_cd, tval_char, nval_num, valueflag_cd, quantity_num, " +
                            "confidence_num, observation_blob, units_cd, end_date, location_cd, update_date, download_date, import_date, sourcesystem_cd, upload_id, reason)" +
                            " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
    }

    @Override
    protected void setParameters(PreparedStatement ps, ObservationFact record) throws SQLException {
        super.setParameters(ps, record);
        String[] rejectionReasons = record.getRejectionReasons();
        if (rejectionReasons.length > 0) {
            ps.setString(25, StringUtils.join(rejectionReasons, ", "));
        } else {
            ps.setString(25, null);
        }
    }
    
    

}
