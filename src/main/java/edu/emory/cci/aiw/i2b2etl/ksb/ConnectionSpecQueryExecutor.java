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
import java.sql.SQLException;
import org.arp.javautil.sql.ConnectionSpec;
import org.arp.javautil.sql.DatabaseAPI;
import org.arp.javautil.sql.InvalidConnectionSpecArguments;
import org.protempa.KnowledgeSourceReadException;

/**
 *
 * @author Andrew Post
 */
class ConnectionSpecQueryExecutor extends QueryExecutor {

    ConnectionSpecQueryExecutor(DatabaseAPI databaseApi, String databaseId, String username, String password, ConnectionSpec connectionSpecInstance, QueryConstructor queryConstructor, TableAccessReader ontTableReader) throws InvalidConnectionSpecArguments, SQLException {
        super(databaseApi.newConnectionSpecInstance(databaseId, username, password, false).getOrCreate(), queryConstructor, ontTableReader);
    }
    
    ConnectionSpecQueryExecutor(DatabaseAPI databaseApi, String databaseId, String username, String password, ConnectionSpec connectionSpecInstance, QueryConstructor queryConstructor, String... tables) throws InvalidConnectionSpecArguments, SQLException {
        super(databaseApi.newConnectionSpecInstance(databaseId, username, password, false).getOrCreate(), queryConstructor, tables);
    }

    @Override
    public void close() throws KnowledgeSourceReadException {
        Connection connection = getConnection();
        try {
            super.close();
            if (connection != null) {
                connection.close();
                connection = null;
            }
        } catch (SQLException ex) {
            throw new KnowledgeSourceReadException(ex);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ignore) {
                }
            }
        }
    }

}
