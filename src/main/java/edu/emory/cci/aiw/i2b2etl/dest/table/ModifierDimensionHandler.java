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
import org.arp.javautil.sql.ConnectionSpec;

/**
 *
 * @author Andrew Post
 */
public class ModifierDimensionHandler extends RecordHandler<ModifierDimension> {

    public static final String TEMP_MODIFIER_TABLE = "temp_modifier";

    public ModifierDimensionHandler(ConnectionSpec connSpec) throws SQLException {
        super(connSpec,
                "insert into " + TEMP_MODIFIER_TABLE + " (modifier_cd,modifier_path,name_char,modifier_blob,"
                + "update_date,download_date,import_date,sourcesystem_cd) values (?,?,?,?,?,?,?,?)");
    }

    @Override
    protected void setParameters(PreparedStatement ps, ModifierDimension record) throws SQLException {
        ps.setString(1, record.getConceptCode());
        ps.setString(2, record.getPath());
        ps.setString(3, record.getDisplayName());
        ps.setObject(4, null);
        ps.setTimestamp(5, record.getUpdated());
        ps.setTimestamp(6, record.getDownloaded());
        ps.setTimestamp(7, importTimestamp());
        ps.setString(8, record.getSourceSystemCode());
    }
}
