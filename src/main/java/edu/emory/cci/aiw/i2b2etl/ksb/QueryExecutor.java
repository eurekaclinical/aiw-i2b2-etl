/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.arp.javautil.sql.InvalidConnectionSpecArguments;
import org.protempa.KnowledgeSourceReadException;

/**
 *
 * @author Andrew Post
 */
public class QueryExecutor implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(QueryExecutor.class.getName());
    private static final ParameterSetter EMPTY_PARAM_SETTER = new ParameterSetter() {

        @Override
        public int set(PreparedStatement stmt, int j) throws SQLException {
            return j;
        }
    };

    private final Connection connection;
    private String sql;
    private PreparedStatement preparedStatement;
    private String[] ontTables;
    private final QueryConstructor queryConstructor;
    private final TableAccessReader ontTableReader;

    public QueryExecutor(Connection connection, QueryConstructor queryConstructor, TableAccessReader ontTableReader) {
        this.connection = connection;
        this.queryConstructor = queryConstructor;
        this.ontTableReader = ontTableReader;
    }

    public <E extends Object> E execute(ResultSetReader<E> resultSetReader) throws KnowledgeSourceReadException {
        return execute(
                EMPTY_PARAM_SETTER,
                resultSetReader
        );
    }

    public <E extends Object> E execute(final String bindArgument, ResultSetReader<E> resultSetReader) throws KnowledgeSourceReadException {
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

    public <E extends Object> E execute(ParameterSetter paramSetter, ResultSetReader<E> resultSetReader) throws KnowledgeSourceReadException {
        try {
            prepare();
            if (this.preparedStatement == null) {
                return resultSetReader.read(null);
            } else {
                int j = 1;
                for (int i = 0, n = this.ontTables.length; i < n; i++) {
                    j = paramSetter.set(this.preparedStatement, j);
                }
                try (ResultSet rs = this.preparedStatement.executeQuery()) {
                    return resultSetReader.read(rs);
                }
            }
        } catch (SQLException ex) {
            throw new KnowledgeSourceReadException(ex);
        }
    }

    public Connection getConnection() {
        return connection;
    }

    @Override
    public void close() throws KnowledgeSourceReadException {
        if (this.preparedStatement != null) {
            try {
                this.preparedStatement.close();
            } catch (SQLException ex) {
                throw new KnowledgeSourceReadException(ex);
            }
            this.preparedStatement = null;
        }
    }

    public void prepare() throws KnowledgeSourceReadException {
        if (this.preparedStatement == null) {
            try {
                openConnection();
                readOntologyTables();
                StringBuilder sql = new StringBuilder();
                if (this.ontTables.length > 0) {
                    if (this.ontTables.length > 1) {
                        sql.append('(');
                    }
                    for (int i = 0, n = this.ontTables.length; i < n; i++) {
                        String table = this.ontTables[i];
                        if (i > 0) {
                            sql.append(") UNION ALL (");
                        }
                        this.queryConstructor.appendStatement(sql, table);
                    }
                    if (this.ontTables.length > 1) {
                        sql.append(')');
                    }

                    this.sql = sql.toString();
                    LOGGER.log(Level.FINE, "Preparing query {0}", this.sql);
                    this.preparedStatement = this.connection.prepareStatement(this.sql);
                    this.preparedStatement.setFetchSize(1000);
                }
            } catch (SQLException | InvalidConnectionSpecArguments ex) {
                throw new KnowledgeSourceReadException(ex);
            }
        }
    }

    private Connection openConnection() throws InvalidConnectionSpecArguments, SQLException {
        return this.connection;
    }

    private void readOntologyTables() throws KnowledgeSourceReadException {
        this.ontTables = this.ontTableReader.read(this.connection);
    }
}
