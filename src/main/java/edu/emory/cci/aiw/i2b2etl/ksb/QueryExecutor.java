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
import org.apache.commons.lang3.ArrayUtils;
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
    private TableAccessReader ontTableReader;
    private String[] tables;

    public QueryExecutor(Connection connection, QueryConstructor queryConstructor, TableAccessReader ontTableReader) {
        this.connection = connection;
        this.queryConstructor = queryConstructor;
        this.ontTableReader = ontTableReader;
    }

    public QueryExecutor(Connection connection, QueryConstructor queryConstructor, String... tables) {
        this.connection = connection;
        this.queryConstructor = queryConstructor;
        this.tables = tables;
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
                long start = System.currentTimeMillis();
                try (ResultSet rs = this.preparedStatement.executeQuery()) {
                    E result = resultSetReader.read(rs);
                    double queryTime = (System.currentTimeMillis() - start) / 1000.0;
                    if (LOGGER.isLoggable(Level.FINE) && queryTime >= 1) {
                        LOGGER.log(Level.FINE, "Long running query ({0} seconds): {1}", new Object[]{queryTime, this.sql});
                    }
                    return result;
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
                readOntologyTables();
                if (this.ontTables.length > 0) {
                    QueryConstructorUnionedMetadataQueryBuilder builder
                            = new QueryConstructorUnionedMetadataQueryBuilder();
                    this.sql = builder
                            .queryConstructor(this.queryConstructor)
                            .ontTables(this.ontTables).build();
                    LOGGER.log(Level.FINE, "Preparing query {0}", this.sql);
                    this.preparedStatement = this.connection.prepareStatement(this.sql);
                    this.preparedStatement.setFetchSize(1000);
                }
            } catch (SQLException ex) {
                throw new KnowledgeSourceReadException(ex);
            }
        }
    }

    private void readOntologyTables() throws KnowledgeSourceReadException {
        if (this.ontTableReader != null) {
            this.ontTables = this.ontTableReader.read(this.connection);
        } else if (this.tables != null) {
            this.ontTables = tables;
        } else {
            this.ontTables = ArrayUtils.EMPTY_STRING_ARRAY;
        }
    }
}
