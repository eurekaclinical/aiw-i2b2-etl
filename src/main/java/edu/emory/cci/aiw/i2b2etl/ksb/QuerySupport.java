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
import edu.emory.cci.aiw.i2b2etl.ksb.TableAccessReader.TableAccessReaderBuilder;
import java.sql.Connection;
import java.sql.SQLException;
import org.arp.javautil.sql.ConnectionSpec;
import org.arp.javautil.sql.DatabaseAPI;
import org.arp.javautil.sql.DatabaseProduct;
import org.arp.javautil.sql.InvalidConnectionSpecArguments;
import org.protempa.KnowledgeSourceReadException;

/**
 *
 * @author Andrew Post
 */
class QuerySupport {

    private static final String DEFAULT_EUREKA_ID_COLUMN = "EK_UNIQUE_ID";

    static class QuerySupportBuilder {

        private DatabaseAPI databaseApi;
        private String databaseId;
        private String username;
        private String password;
        private String excludeTableName;
        private String eurekaIdColumn;

        QuerySupportBuilder() {
            this.databaseApi = DatabaseAPI.DRIVERMANAGER;
            this.eurekaIdColumn = DEFAULT_EUREKA_ID_COLUMN;
        }

        DatabaseAPI getDatabaseApi() {
            return databaseApi;
        }

        void setDatabaseApi(DatabaseAPI databaseApi) {
            this.databaseApi = databaseApi;
        }

        String getDatabaseId() {
            return databaseId;
        }

        void setDatabaseId(String databaseId) {
            this.databaseId = databaseId;
        }

        String getUsername() {
            return username;
        }

        void setUsername(String username) {
            this.username = username;
        }

        String getPassword() {
            return password;
        }

        void setPassword(String password) {
            this.password = password;
        }

        String getExcludeTableName() {
            return excludeTableName;
        }

        void setExcludeTableName(String excludeTableName) {
            this.excludeTableName = excludeTableName;
        }

        String getEurekaIdColumn() {
            return eurekaIdColumn;
        }

        void setEurekaIdColumn(String eurekaIdColumn) {
            this.eurekaIdColumn = eurekaIdColumn;
        }

        QuerySupport build() throws InvalidConnectionSpecArguments, SQLException {
            return new QuerySupport(databaseApi, databaseId, username, password, excludeTableName, eurekaIdColumn);
        }

    }
    
    private final DatabaseAPI databaseApi;
    private final String databaseId;
    private final String username;
    private final String password;
    private final String excludeTableName;
    private final TableAccessReaderBuilder tableAccessReaderBuilder;
    private final String eurekaIdColumn;
    private final DatabaseProduct databaseProduct;
    private final ConnectionSpec connectionSpec;
    private final LevelReader levelReader;

    QuerySupport(DatabaseAPI databaseApi, String databaseId, String username, String password, String excludeTableName, String eurekaIdColumn) throws InvalidConnectionSpecArguments, SQLException {
        this.databaseApi = databaseApi;
        this.eurekaIdColumn = eurekaIdColumn;
        this.databaseId = databaseId;
        this.username = username;
        this.password = password;
        this.excludeTableName = excludeTableName;
        this.connectionSpec = this.databaseApi.newConnectionSpecInstance(databaseId, username, password, false);
        this.tableAccessReaderBuilder = new TableAccessReaderBuilder(this.connectionSpec.getDatabaseProduct()).excludeTableName(excludeTableName);
        try (Connection cn = this.connectionSpec.getOrCreate()) {
            this.databaseProduct = DatabaseProduct.fromMetaData(cn.getMetaData());
        }
        this.levelReader = new LevelReader(this);
    }

    String getEurekaIdColumn() {
        return eurekaIdColumn;
    }

    DatabaseAPI getDatabaseApi() {
        return this.databaseApi;
    }

    String getDatabaseId() {
        return databaseId;
    }

    String getUsername() {
        return username;
    }

    String getPassword() {
        return password;
    }

    String getExcludeTableName() {
        return excludeTableName;
    }

    TableAccessReaderBuilder getTableAccessReaderBuilder() {
        return new TableAccessReaderBuilder(this.tableAccessReaderBuilder);
    }

    Connection getConnection() throws InvalidConnectionSpecArguments, SQLException {
        return this.connectionSpec.getOrCreate();
    }

    DatabaseProduct getDatabaseProduct() throws KnowledgeSourceReadException {
        return this.databaseProduct;
    }
    
    LevelReader getLevelReader() {
        return this.levelReader;
    }

    ConnectionSpecQueryExecutor getQueryExecutorInstanceRestrictByTables(QueryConstructor queryConstructor, String... tables) throws KnowledgeSourceReadException {
        try {
            return new ConnectionSpecQueryExecutor(this.databaseApi, this.databaseId, this.username, this.password, queryConstructor, tables);
        } catch (InvalidConnectionSpecArguments | SQLException ex) {
            throw new KnowledgeSourceReadException(ex);
        }
    }

    ConnectionSpecQueryExecutor getQueryExecutorInstance(QueryConstructor queryConstructor) throws KnowledgeSourceReadException {
        try {
            return new ConnectionSpecQueryExecutor(this.databaseApi, this.databaseId, this.username, this.password, queryConstructor, getTableAccessReaderBuilder().build());
        } catch (InvalidConnectionSpecArguments | SQLException ex) {
            throw new KnowledgeSourceReadException(ex);
        }
    }

    ConnectionSpecQueryExecutor getQueryExecutorInstance(QueryConstructor queryConstructor, TableAccessReader tableAccessReader) throws KnowledgeSourceReadException {
        try {
            return new ConnectionSpecQueryExecutor(this.databaseApi, this.databaseId, this.username, this.password, queryConstructor, tableAccessReader);
        } catch (InvalidConnectionSpecArguments | SQLException ex) {
            throw new KnowledgeSourceReadException(ex);
        }
    }

    ConnectionSpecQueryExecutor getQueryExecutorInstanceRestrictByEkUniqueIds(QueryConstructor queryConstructor, String... ekUniqueIds) throws KnowledgeSourceReadException {
        try {
            return new ConnectionSpecQueryExecutor(this.databaseApi, this.databaseId, this.username, this.password, queryConstructor, getTableAccessReaderBuilder().restrictTablesBy(ekUniqueIds).build());
        } catch (InvalidConnectionSpecArguments | SQLException ex) {
            throw new KnowledgeSourceReadException(ex);
        }
    }

    QueryExecutor getQueryExecutorInstanceRestrictByTables(Connection connection, QueryConstructor queryConstructor, String... tables) throws KnowledgeSourceReadException {
        if (connection != null) {
            return new QueryExecutor(connection, queryConstructor, tables);
        } else {
            return getQueryExecutorInstanceRestrictByTables(queryConstructor, tables);
        }
    }

    QueryExecutor getQueryExecutorInstance(Connection connection, QueryConstructor queryConstructor) throws KnowledgeSourceReadException {
        if (connection != null) {
            return new QueryExecutor(connection, queryConstructor, getTableAccessReaderBuilder().build());
        } else {
            return getQueryExecutorInstance(queryConstructor);
        }
    }

    QueryExecutor getQueryExecutorInstance(Connection connection, QueryConstructor queryConstructor, TableAccessReader tableAccessReader) throws KnowledgeSourceReadException {
        if (connection != null) {
            return new QueryExecutor(connection, queryConstructor, tableAccessReader);
        } else {
            return getQueryExecutorInstance(queryConstructor);
        }
    }

    QueryExecutor getQueryExecutorInstanceRestrictByEkUniqueIds(Connection connection, QueryConstructor queryConstructor, String... ekUniqueIds) throws KnowledgeSourceReadException {
        if (connection != null) {
            return new QueryExecutor(connection, queryConstructor, getTableAccessReaderBuilder().restrictTablesBy(ekUniqueIds).build());
        } else {
            return getQueryExecutorInstance(queryConstructor);
        }
    }

}
