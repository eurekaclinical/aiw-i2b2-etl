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
import edu.emory.cci.aiw.i2b2etl.dest.metadata.Concept;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.MetadataUtil;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.arp.javautil.sql.ConnectionSpec;

/**
 *
 * @author arpost
 */
public class ProviderDimensionHandler extends ConnectionSpecRecordHandler<ProviderDimension> {

    public static final String TEMP_PROVIDER_TABLE = "ek_temp_provider";

    public ProviderDimensionHandler(ConnectionSpec connSpec) throws SQLException {
        super(connSpec,
                "insert into " + TEMP_PROVIDER_TABLE + " (provider_id,provider_path,name_char,"
                + "provider_blob,update_date,download_date,import_date,sourcesystem_cd,upload_id) values (?,?,?,?,?,?,?,?,?)");
    }

    @Override
    protected void setParameters(PreparedStatement ps, ProviderDimension provider) throws SQLException {
        Concept concept = provider.getConcept();
        ps.setString(1, TableUtil.setStringAttribute(concept.getConceptCode()));
        ps.setString(2, concept.getFullName());
        ps.setString(3, concept.getDisplayName());
        ps.setObject(4, null);
        ps.setTimestamp(5, null);
        ps.setTimestamp(6, null);
        ps.setTimestamp(7, new java.sql.Timestamp(System.currentTimeMillis()));
        ps.setString(8, MetadataUtil.toSourceSystemCode(provider.getSourceSystem()));
        ps.setObject(9, null);
    }

}
