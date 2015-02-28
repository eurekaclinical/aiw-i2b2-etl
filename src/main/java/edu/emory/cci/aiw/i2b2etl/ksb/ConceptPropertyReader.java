package edu.emory.cci.aiw.i2b2etl.ksb;

/*
 * #%L
 * Protempa i2b2 Knowledge Source Backend
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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.protempa.KnowledgeSourceReadException;
import org.protempa.ValueSet;
import org.protempa.ValueSet.ValueSetElement;
import org.protempa.proposition.value.NominalValue;

/**
 *
 * @author Andrew Post
 */
class ConceptPropertyReader {

    private final QuerySupport querySupport;

    ConceptPropertyReader(QuerySupport querySupport) {
        this.querySupport = querySupport;
    }

    private final ResultSetReader<ValueSetElement[]> reader = new ResultSetReader<ValueSetElement[]>() {

        @Override
        public ValueSetElement[] read(ResultSet rs) throws KnowledgeSourceReadException {
            try {
                if (rs.next()) {
                    int c_hlevel = rs.getInt(1);
                    String fullName = rs.getString(2);
                    return readLevelFromDatabaseHelper(c_hlevel, fullName, 1);
                } else {
                    return null;
                }
            } catch (SQLException ex) {
                throw new KnowledgeSourceReadException(ex);
            }
        }

    };

    private final QueryConstructor READ_FROM_DB_QUERY_CONSTRUCTOR = new QueryConstructor() {

        @Override
        public void appendStatement(StringBuilder sql, String table) {
            sql.append("SELECT C_HLEVEL, C_FULLNAME FROM ");
            sql.append(table);
            sql.append(" WHERE ").append(querySupport.getEurekaIdColumn()).append(" = ? AND M_APPLIED_PATH='@'");
        }

    };

    ValueSet readFromDatabase(String id) throws KnowledgeSourceReadException {
        try (ConnectionSpecQueryExecutor queryExecutor = this.querySupport.getQueryExecutorInstance(READ_FROM_DB_QUERY_CONSTRUCTOR)) {
            return new ValueSet(
                    id,
                    queryExecutor.execute(
                            id,
                            reader
                    ), null);
        }
    }

    ValueSetElement[] readLevelFromDatabaseHelper(final int c_hlevel, final String fullName, final int offset) throws KnowledgeSourceReadException {
        try (ConnectionSpecQueryExecutor queryExecutor = this.querySupport.getQueryExecutorInstance(new QueryConstructor() {

            @Override
            public void appendStatement(StringBuilder sql, String table) {
                sql.append("SELECT ").append(querySupport.getEurekaIdColumn()).append(", C_NAME FROM ");
                sql.append(table);
                sql.append(" WHERE C_HLEVEL=? AND M_APPLIED_PATH='@' AND C_FULLNAME LIKE ? ESCAPE '\\'");
            }
        })) {
            return queryExecutor.execute(
                    new ParameterSetter() {
                        private static final String ONT_PATH_SEP = "\\";
                        private String newFullName = newFullName(fullName, offset);

                        @Override
                        public int set(PreparedStatement stmt, int j) throws SQLException {
                            stmt.setInt(j++, c_hlevel + offset);
                            stmt.setString(j++, newFullName + "%");
                            return j;
                        }

                        private String newFullName(String fullName, int offset) {
                            String fullName2 = fullName;
                            if (fullName2.length() == 0) {
                                return fullName2;
                            }
                            if (fullName2.endsWith(ONT_PATH_SEP)) {
                                fullName2 = fullName2.substring(0, fullName2.length() - 1);
                            }
                            if (offset == -1) {
                                int lastIndexOf = fullName2.lastIndexOf(ONT_PATH_SEP);
                                if (lastIndexOf == -1) {
                                    fullName2 = "";
                                } else {
                                    fullName2 = fullName2.substring(0, lastIndexOf);
                                }
                            }
                            fullName2 = I2B2Util.escapeLike(fullName2);
                            return fullName2;
                        }
                    },
                    new ResultSetReader<ValueSetElement[]>() {

                        @Override
                        public ValueSetElement[] read(ResultSet rs) throws KnowledgeSourceReadException {
                            List<ValueSetElement> result = new ArrayList<>();
                            try {
                                while (rs.next()) {
                                    result.add(new ValueSetElement(NominalValue.getInstance(rs.getString(1)), rs.getString(2), null));
                                }
                            } catch (SQLException ex) {
                                throw new KnowledgeSourceReadException(ex);
                            }
                            return result.toArray(new ValueSetElement[result.size()]);
                        }
                    });
        }
    }

}
