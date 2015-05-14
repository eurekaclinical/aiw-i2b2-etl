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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.protempa.KnowledgeSourceReadException;

/**
 * Queries an i2b2 metadata schema for all of the tables in TABLE_ACCESS that
 * have a EK_UNIQUE_ID column (note case!) and thus are an Eureka metadata
 * table.
 * 
 * @author Andrew Post
 */
public class TableAccessReader {

    private String[] ontTables;
    private final String excludeTableName;

    public TableAccessReader(String excludeTableName) {
        this.excludeTableName = excludeTableName;
    }

    public String[] read(Connection connection) throws KnowledgeSourceReadException {
        synchronized (this) {
            if (this.ontTables == null) {
                StringBuilder query = new StringBuilder();
                query.append("SELECT DISTINCT C_TABLE_NAME FROM TABLE_ACCESS");
                if (this.excludeTableName != null) {
                    query.append(" WHERE C_TABLE_NAME <> ?");
                }
                List<String> tables = new ArrayList<>();
                try (PreparedStatement stmt = connection.prepareStatement(query.toString())) {
                    if (this.excludeTableName != null) {
                        stmt.setString(1, this.excludeTableName);
                    }
                    try (ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            tables.add(rs.getString(1));
                        }
                    }
                } catch (SQLException ex) {
                    throw new KnowledgeSourceReadException(ex);
                }
                try {
                    for (Iterator<String> itr = tables.iterator(); itr.hasNext();) {
                        String tableName = itr.next();
                        try (Statement stmt = connection.createStatement();
                                ResultSet resultSet = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE 1 = 0")) {
                            ResultSetMetaData metaData = resultSet.getMetaData();
                            boolean found = false;
                            for (int i = 1, n = metaData.getColumnCount(); i <= n; i++) {
                                if ("EK_UNIQUE_ID".equals(metaData.getColumnLabel(i))) {
                                    found = true;
                                    break;
                                }
                            }
                            if (!found) {
                                itr.remove();
                            }
                        }
                    }
                } catch (SQLException ex) {
                    Logger.getLogger(TableAccessReader.class.getName()).log(Level.SEVERE, null, ex);
                }
                this.ontTables = tables.toArray(new String[tables.size()]);
            }
        }
        return this.ontTables.clone();
    }
}
