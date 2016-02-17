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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.protempa.KnowledgeSourceReadException;

import static org.arp.javautil.collections.Collections.putSet;
import static org.arp.javautil.collections.Collections.putSetAll;
import org.arp.javautil.sql.InvalidConnectionSpecArguments;

/**
 *
 * @author Andrew Post
 */
class LevelReader {

    private final QuerySupport querySupport;

    LevelReader(QuerySupport querySupport) {
        this.querySupport = querySupport;
    }

    Set<String> readChildrenFromDatabase(String fullName) throws KnowledgeSourceReadException {
        try (ConnectionSpecQueryExecutor queryExecutor = this.querySupport.getQueryExecutorInstance(READ_CHILDREN_FROM_DB_QUERY_CONSTRUCTOR)) {
            return queryExecutor.execute(
                    fullName,
                    RESULT_SET_READER
            );
        }
    }

    Map<String, Set<String>> readChildrenFromDatabase(final Collection<String> symbols) throws KnowledgeSourceReadException {
        Map<String, Set<String>> result = new HashMap<>();
        try (Connection connection = this.querySupport.getConnection()) {
            try {
                try (UniqueIdTempTableHandler childTempTableHandler = new UniqueIdTempTableHandler(this.querySupport.getDatabaseProduct(), connection, false)) {
                    for (String child : symbols) {
                        childTempTableHandler.insert(child);
                    }
                }

                try (QueryExecutor queryExecutor = this.querySupport.getQueryExecutorInstance(connection, new QueryConstructor() {

                    @Override
                    public void appendStatement(StringBuilder sql, String table) {
                        String ekIdCol = querySupport.getEurekaIdColumn();
                        sql.append("SELECT A2.").append(ekIdCol).append(", A1.").append(ekIdCol).append(" FROM ");
                        sql.append(table);
                        sql.append(" A1 JOIN ");
                        sql.append(table);
                        sql.append(" A2 ON (A1.C_PATH=A2.C_FULLNAME) JOIN EK_TEMP_UNIQUE_IDS A3 ON (A3.UNIQUE_ID=A2.").append(ekIdCol).append(") WHERE A2.M_APPLIED_PATH='@' and A1.C_SYNONYM_CD='N' and A2.C_SYNONYM_CD='N'");
                    }
                })) {
                    putSetAll(result,
                            queryExecutor.execute(MULT_RESULT_SET_READER));
                }
                connection.commit();
            } catch (SQLException ex) {
                try {
                    connection.rollback();
                } catch (SQLException ignore) {
                } finally {
                    throw ex;
                }
            }
        } catch (InvalidConnectionSpecArguments | SQLException ex) {
            throw new KnowledgeSourceReadException(ex);
        }

        return result;
    }

    Set<String> readParentsFromDatabase(String propId) throws KnowledgeSourceReadException {
        try (ConnectionSpecQueryExecutor queryExecutor = this.querySupport.getQueryExecutorInstance(READ_PARENTS_FROM_DB_QUERY_CONSTRUCTOR)) {
            return queryExecutor.execute(
                    propId,
                    RESULT_SET_READER
            );
        }
    }

    private final QueryConstructor READ_PARENTS_FROM_DB_QUERY_CONSTRUCTOR = new QueryConstructor() {
        @Override
        public void appendStatement(StringBuilder sql, String table) {
            sql.append("SELECT ").append(querySupport.getEurekaIdColumn()).append(" FROM ");
            sql.append(table);
            sql.append(" WHERE M_APPLIED_PATH='@' AND C_SYNONYM_CD='N' AND C_FULLNAME IN (SELECT C_PATH FROM ");
            sql.append(table);
            sql.append(" WHERE ").append(querySupport.getEurekaIdColumn()).append(" = ? AND M_APPLIED_PATH='@')");
        }

    };

    private static final ResultSetReader<Set<String>> RESULT_SET_READER = new ResultSetReader<Set<String>>() {

        @Override
        public Set<String> read(ResultSet rs) throws KnowledgeSourceReadException {
            try {
                Set<String> result = new HashSet<>();
                if (rs != null) {
                    while (rs.next()) {
                        result.add(rs.getString(1));
                    }
                }
                return result;
            } catch (SQLException ex) {
                throw new KnowledgeSourceReadException(ex);
            }
        }

    };

    private static final ResultSetReader<Map<String, Set<String>>> MULT_RESULT_SET_READER = new ResultSetReader<Map<String, Set<String>>>() {

        @Override
        public Map<String, Set<String>> read(ResultSet rs) throws KnowledgeSourceReadException {
            try {
                Map<String, Set<String>> result = new HashMap<>();
                if (rs != null) {
                    while (rs.next()) {
                        putSet(result, rs.getString(1), rs.getString(2));
                    }
                }
                return result;
            } catch (SQLException ex) {
                throw new KnowledgeSourceReadException(ex);
            }
        }

    };

    private final QueryConstructor READ_CHILDREN_FROM_DB_QUERY_CONSTRUCTOR = new QueryConstructor() {

        @Override
        public void appendStatement(StringBuilder sql, String table) {
            sql.append("SELECT ").append(querySupport.getEurekaIdColumn()).append(" FROM ");
            sql.append(table);
            sql.append(" WHERE M_APPLIED_PATH='@' AND C_PATH=?");
        }

    };

}
