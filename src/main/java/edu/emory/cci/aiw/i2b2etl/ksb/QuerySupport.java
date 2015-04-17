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
import java.sql.SQLException;
import org.arp.javautil.sql.ConnectionSpec;
import org.arp.javautil.sql.DatabaseAPI;
import org.arp.javautil.sql.InvalidConnectionSpecArguments;
import org.protempa.KnowledgeSourceReadException;

/**
 *
 * @author Andrew Post
 */
class QuerySupport {
    private static final String DEFAULT_EUREKA_ID_COLUMN = "EK_UNIQUE_ID";
    
    private DatabaseAPI databaseApi;
    private String databaseId;
    private String username;
    private String password;
    private ConnectionSpec connectionSpecInstance;
    private String excludeTableName;
    private String eurekaIdColumn;

    QuerySupport() {
         this.databaseApi = DatabaseAPI.DRIVERMANAGER;
         this.eurekaIdColumn = DEFAULT_EUREKA_ID_COLUMN;
    }

    String getEurekaIdColumn() {
        return eurekaIdColumn;
    }

    void setEurekaIdColumn(String eurekaIdColumn) {
        this.eurekaIdColumn = eurekaIdColumn;
    }

    DatabaseAPI getDatabaseApi() {
        return this.databaseApi;
    }

    void setDatabaseApi(DatabaseAPI databaseApi) {
        this.databaseApi = databaseApi;
        this.connectionSpecInstance = null;
    }

    String getDatabaseId() {
        return databaseId;
    }

    void setDatabaseId(String databaseId) {
        this.databaseId = databaseId;
        this.connectionSpecInstance = null;
    }

    String getUsername() {
        return username;
    }

    void setUsername(String username) {
        this.username = username;
        this.connectionSpecInstance = null;
    }

    String getPassword() {
        return password;
    }

    void setPassword(String password) {
        this.password = password;
        this.connectionSpecInstance = null;
    }

    String getExcludeTableName() {
        return excludeTableName;
    }

    void setExcludeTableName(String excludeTableName) {
        this.excludeTableName = excludeTableName;
    }
    
    Connection getConnection() throws InvalidConnectionSpecArguments, SQLException {
        return this.databaseApi.newConnectionSpecInstance(databaseId, username, password).getOrCreate();
    }
    
    ConnectionSpecQueryExecutor getQueryExecutorInstance(QueryConstructor queryConstructor) throws KnowledgeSourceReadException {
        try {
            return new ConnectionSpecQueryExecutor(this.databaseApi, this.databaseId, this.username, this.password, this.connectionSpecInstance, queryConstructor, this.excludeTableName);
        } catch (InvalidConnectionSpecArguments | SQLException ex) {
            throw new KnowledgeSourceReadException(ex);
        }
    }
    
    QueryExecutor getQueryExecutorInstance(Connection connection, QueryConstructor queryConstructor) throws KnowledgeSourceReadException {
        if (connection != null) {
            return new QueryExecutor(connection, queryConstructor, this.excludeTableName);
        } else {
            return getQueryExecutorInstance(queryConstructor);
        }
    }
    
}
