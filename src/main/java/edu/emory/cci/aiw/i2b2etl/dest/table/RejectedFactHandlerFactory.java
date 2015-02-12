package edu.emory.cci.aiw.i2b2etl.dest.table;

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

import java.sql.SQLException;
import org.arp.javautil.sql.ConnectionSpec;

/**
 *
 * @author Andrew Post
 */
public class RejectedFactHandlerFactory {
    private final ConnectionSpec connectionSpec;
    private final String tableName;

    public RejectedFactHandlerFactory(ConnectionSpec connectionSpec, String tableName) {
        if (connectionSpec == null) {
            throw new IllegalArgumentException("connectionSpec cannot be null");
        }
        if (tableName == null) {
            throw new IllegalArgumentException("tableName cannot be null");
        }
        this.connectionSpec = connectionSpec;
        this.tableName = tableName;
    }
    
    public ConnectionSpec getConnectionSpec() {
        return this.connectionSpec;
    }
    
    public String getTableName() {
        return this.tableName;
    }
    
    public RejectedFactHandler getInstance() throws SQLException {
        return new RejectedFactHandler(this.connectionSpec, this.tableName);
    }
}
