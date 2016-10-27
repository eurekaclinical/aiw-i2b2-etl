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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.arp.javautil.sql.DatabaseProduct;
import org.arp.javautil.sql.InvalidConnectionSpecArguments;
import org.protempa.KnowledgeSourceReadException;

/**
 * Queries an i2b2 metadata schema for all of the tables in TABLE_ACCESS that
 * have a EK_UNIQUE_ID column (note case!) and thus are an Eureka metadata
 * table. The contents of TABLE_ACCESS are cached for the duration of a Protempa
 * run. Thus, when structurally modifying TABLE_ACCESS it is critical to take
 * Eureka off-line.
 *
 * @author Andrew Post
 */
public final class TableAccessReader {

    private static final String[] EMPTY_STRING_ARRAY = new String[0];

    static final class TableAccessReaderBuilder {

        private String excludeTableName;
        private String[] ekUniqueIds;

        public TableAccessReaderBuilder() {
            this.ekUniqueIds = EMPTY_STRING_ARRAY;
        }

        public TableAccessReaderBuilder(TableAccessReaderBuilder builder) {
            this.ekUniqueIds = builder.ekUniqueIds.clone();
            this.excludeTableName = builder.excludeTableName;
        }

        public TableAccessReaderBuilder excludeTableName(String excludeTableName) {
            this.excludeTableName = excludeTableName;
            return this;
        }

        public TableAccessReaderBuilder restrictTablesBy(String... ekUniqueIds) {
            this.ekUniqueIds = ekUniqueIds;
            return this;
        }

        public TableAccessReader build() {
            return new TableAccessReader(this.excludeTableName, this.ekUniqueIds);
        }
    }

    private String[] ontTables;
    private final String excludeTableName;
    private final String[] ekUniqueIds;

    public TableAccessReader(String excludeTableName, String... ekUniqueIds) {
        this.excludeTableName = excludeTableName;
        this.ekUniqueIds = ekUniqueIds.clone();
    }

    public String[] read(Connection connection) throws KnowledgeSourceReadException {
        synchronized (this) {
            if (this.ontTables == null) {
                Set<String> tables = new HashSet<>();
                StringBuilder query = new StringBuilder();
                query.append("SELECT DISTINCT C_TABLE_NAME FROM TABLE_ACCESS");
                if (this.excludeTableName != null) {
                    query.append(" WHERE C_TABLE_NAME <> ?");
                }
                try (PreparedStatement stmt = connection.prepareStatement(query.toString())) {
                    stmt.setFetchSize(10);
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
                                if ("EK_UNIQUE_ID".equalsIgnoreCase(metaData.getColumnLabel(i))) {
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
                    throw new KnowledgeSourceReadException(ex);
                }

                if (this.ekUniqueIds.length > 0) {
                    try {
                        DatabaseProduct databaseProduct = DatabaseProduct.fromMetaData(connection.getMetaData());
                        DefaultUnionedMetadataQueryBuilder builder = new DefaultUnionedMetadataQueryBuilder();
                        String subQuery = builder.statement("SELECT 1 FROM {0} WHERE EK_UNIQUE_ID IN (''" + String.join("'',''", this.ekUniqueIds) + "'') AND C_FULLNAME LIKE TA.C_FULLNAME || ''%''" + (databaseProduct == DatabaseProduct.POSTGRESQL ? " ESCAPE ''''" : "")).ontTables(tables.toArray(new String[tables.size()])).build();
                        try (Statement stmt = connection.createStatement();
                                ResultSet resultSet = stmt.executeQuery("SELECT DISTINCT C_TABLE_NAME FROM TABLE_ACCESS TA WHERE EXISTS (" + subQuery + ")")) {
                            resultSet.setFetchSize(10);
                            List<String> l = new ArrayList<>();
                            while (resultSet.next()) {
                                l.add(resultSet.getString(1));
                            }
                            this.ontTables = l.toArray(new String[l.size()]);
                        }
                    } catch (SQLException ex) {
                        throw new KnowledgeSourceReadException(ex);
                    }
                } else {
                    this.ontTables = tables.toArray(new String[tables.size()]);
                }
            }
        }
        return this.ontTables.clone();
    }
}
