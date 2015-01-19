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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.arp.javautil.sql.ConnectionSpec;

/**
 *
 * @author arpost
 */
public class ConceptDimensionHandler extends RecordHandler<ConceptDimension> {

    public static final String TEMP_CONCEPT_TABLE = "temp_concept";

    public ConceptDimensionHandler(ConnectionSpec connSpec) throws SQLException {
        super(connSpec,
                "insert into " + TEMP_CONCEPT_TABLE + " (concept_cd,concept_path,name_char,concept_blob,"
                + "update_date,download_date,import_date,sourcesystem_cd) values (?,?,?,?,?,?,?,?)");
    }

    @Override
    protected void setParameters(PreparedStatement ps, ConceptDimension record) throws SQLException {
        ps.setString(1, record.getConceptCode());
        ps.setString(2, record.getPath());
        ps.setString(3, record.getDisplayName());
        ps.setObject(4, null);
        ps.setTimestamp(5, null);
        ps.setTimestamp(6, null);
        ps.setTimestamp(7, importTimestamp());
        ps.setString(8, record.getSourceSystemCode());
    }
}
