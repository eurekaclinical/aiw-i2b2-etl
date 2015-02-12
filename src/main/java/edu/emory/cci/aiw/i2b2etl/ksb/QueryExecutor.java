package edu.emory.cci.aiw.i2b2etl.ksb;

/*
 * #%L
 * Eureka! i2b2 Knowledge Source Backend
 * %%
 * Copyright (C) 2015 Emory University
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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.arp.javautil.sql.ConnectionSpec;
import org.arp.javautil.sql.DatabaseAPI;
import org.arp.javautil.sql.InvalidConnectionSpecArguments;
import org.protempa.KnowledgeSourceReadException;

/**
 *
 * @author Andrew Post
 */
class QueryExecutor implements AutoCloseable {

    private static final ParameterSetter EMPTY_PARAM_SETTER = new ParameterSetter() {

        @Override
        public int set(PreparedStatement stmt, int j) throws SQLException {
            return j;
        }
    };

    private final DatabaseAPI databaseApi;
    private final String databaseId;
    private final String username;
    private final String password;
    private ConnectionSpec connectionSpecInstance;
    private Connection connection;
    private String sql;
    private PreparedStatement preparedStatement;
    private List<String> ontTables;
    private final QueryConstructor queryConstructor;
    private final String excludeTableName;

    QueryExecutor(DatabaseAPI databaseApi, String databaseId, String username, String password, ConnectionSpec connectionSpecInstance, QueryConstructor queryConstructor, String excludeTableName) {
        this.databaseApi = databaseApi;
        this.databaseId = databaseId;
        this.username = username;
        this.password = password;
        this.connectionSpecInstance = connectionSpecInstance;
        this.queryConstructor = queryConstructor;
        this.excludeTableName = excludeTableName;
    }

    <E extends Object> E execute(ResultSetReader<E> resultSetReader) throws KnowledgeSourceReadException {
        return execute(
                EMPTY_PARAM_SETTER,
                resultSetReader
        );
    }

    <E extends Object> E execute(final String bindArgument, ResultSetReader<E> resultSetReader) throws KnowledgeSourceReadException {
        return execute(
                new ParameterSetter() {

                    @Override
                    public int set(PreparedStatement stmt, int j) throws SQLException {
                        stmt.setString(j++, bindArgument);
                        return j;
                    }
                },
                resultSetReader
        );
    }

    <E extends Object> E execute(ParameterSetter paramSetter, ResultSetReader<E> resultSetReader) throws KnowledgeSourceReadException {
        try {
            openConnection();
            readOntologyTables();
            prepare();
            int j = 1;
            for (int i = 0, n = this.ontTables.size(); i < n; i++) {
                j = paramSetter.set(this.preparedStatement, j);
            }
            try (ResultSet rs = this.preparedStatement.executeQuery()) {
                return resultSetReader.read(rs);
            }
        } catch (InvalidConnectionSpecArguments | SQLException ex) {
            throw new KnowledgeSourceReadException(ex);
        }
    }

    @Override
    public void close() throws KnowledgeSourceReadException {
        try {
            if (this.preparedStatement != null) {
                this.preparedStatement.close();
                this.preparedStatement = null;
            }
            if (this.connection != null) {
                this.connection.close();
                this.connection = null;
            }
        } catch (SQLException ex) {
            throw new KnowledgeSourceReadException(ex);
        } finally {
            if (this.connection != null) {
                try {
                    this.connection.close();
                } catch (SQLException ignore) {
                }
            }
        }
    }

    private Connection openConnection() throws InvalidConnectionSpecArguments, SQLException {
        if (this.connectionSpecInstance == null) {
            this.connectionSpecInstance = this.databaseApi.newConnectionSpecInstance(this.databaseId, this.username, this.password);
        }
        if (this.connection == null) {
            this.connection = connectionSpecInstance.getOrCreate();
        }
        return this.connection;
    }

    void prepare() throws KnowledgeSourceReadException {
        if (this.preparedStatement == null) {
            try {
                openConnection();
                readOntologyTables();
                StringBuilder sql = new StringBuilder();
                if (this.ontTables.size() > 1) {
                    sql.append('(');
                }
                for (int i = 0, n = this.ontTables.size(); i < n; i++) {
                    String table = this.ontTables.get(i);
                    if (i > 0) {
                        sql.append(") UNION ALL (");
                    }
                    this.queryConstructor.appendStatement(sql, table);
                }
                if (this.ontTables.size() > 1) {
                    sql.append(')');
                }
                this.sql = sql.toString();
                this.preparedStatement = this.connection.prepareStatement(this.sql);
                this.preparedStatement.setFetchSize(1000);
            } catch (SQLException | InvalidConnectionSpecArguments ex) {
                throw new KnowledgeSourceReadException(ex);
            }
        }
    }

    private void readOntologyTables() throws KnowledgeSourceReadException {
        if (this.ontTables == null) {
            StringBuilder query = new StringBuilder();
            query.append("SELECT C_TABLE_NAME FROM TABLE_ACCESS");
            if (this.excludeTableName != null) {
                query.append(" WHERE C_TABLE_NAME <> ?");
            }
            try (PreparedStatement stmt = this.connection.prepareStatement(query.toString())) {
                if (this.excludeTableName != null) {
                    stmt.setString(1, this.excludeTableName);
                }
                try (ResultSet rs = stmt.executeQuery()) {
                    List<String> tables = new ArrayList<>();
                    while (rs.next()) {
                        tables.add(rs.getString(1));
                    }
                    if (tables.isEmpty()) {
                        throw new KnowledgeSourceReadException("No metadata tables found!");
                    }
                    this.ontTables = tables;
                }
            } catch (SQLException ex) {
                throw new KnowledgeSourceReadException(ex);
            }
        }
    }

}
