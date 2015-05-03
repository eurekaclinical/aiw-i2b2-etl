package edu.emory.cci.aiw.i2b2etl.ksb;

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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.protempa.KnowledgeSourceReadException;

/**
 *
 * @author Andrew Post
 */
public class TableAccessReader {

    private String[] ontTables;
    private final String excludeTableName;
    private final String ontTablesSyncVariable = "ONT_TABLES_SYNC_VARIABLE";

    public TableAccessReader(String excludeTableName) {
        this.excludeTableName = excludeTableName;
    }

    public String[] read(Connection connection) throws KnowledgeSourceReadException {
        synchronized (this.ontTablesSyncVariable) {
            if (this.ontTables == null) {
                StringBuilder query = new StringBuilder();
                query.append("SELECT DISTINCT C_TABLE_NAME FROM TABLE_ACCESS");
                if (this.excludeTableName != null) {
                    query.append(" WHERE C_TABLE_NAME <> ?");
                }
                try (PreparedStatement stmt = connection.prepareStatement(query.toString())) {
                    if (this.excludeTableName != null) {
                        stmt.setString(1, this.excludeTableName);
                    }
                    try (ResultSet rs = stmt.executeQuery()) {
                        List<String> tables = new ArrayList<>();
                        while (rs.next()) {
                            tables.add(rs.getString(1));
                        }
                        this.ontTables = tables.toArray(new String[tables.size()]);
                    }
                } catch (SQLException ex) {
                    throw new KnowledgeSourceReadException(ex);
                }
            }
        }
        return this.ontTables.clone();
    }
}
