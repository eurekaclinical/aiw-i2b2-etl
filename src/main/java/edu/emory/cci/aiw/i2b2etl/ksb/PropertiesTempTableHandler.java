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
import java.sql.SQLException;
import java.sql.Statement;
import org.arp.javautil.sql.DatabaseProduct;

/**
 *
 * @author Andrew Post
 */
class PropertiesTempTableHandler {

    private final Connection connection;
    private final String statement;
    
    PropertiesTempTableHandler(DatabaseProduct databaseProduct, Connection connection, String table, String eurekaIdColumn) throws SQLException {
        this.connection = connection;
        
        this.statement = "insert into ek_temp_properties (SELECT " + eurekaIdColumn + ", M_APPLIED_PATH, DISPLAYNAME C_NAME, VALUETYPE_CD, PROPERTYNAME, C_METADATAXML FROM" +
        " ((SELECT " + eurekaIdColumn + ", M_APPLIED_PATH, C_NAME DISPLAYNAME, VALUETYPE_CD, C_BASECODE PROPERTYNAME, C_METADATAXML FROM " +
        table +
        " WHERE C_METADATAXML IS NOT NULL AND M_APPLIED_PATH  <> '@' AND C_BASECODE IS NOT NULL AND C_SYNONYM_CD = 'N')" +
        " UNION ALL" +
        " (SELECT NULL, M_APPLIED_PATH, DISPLAYNAME, NULL, PROPERTYNAME, NULL " +
        "FROM (SELECT DISTINCT T1.M_APPLIED_PATH, DISPLAYNAME, NULL, PROPERTYNAME FROM EK_MODIFIER_INTERP EMI JOIN " +
        table + 
        " T1 ON (EMI.C_BASECODE =T1.C_BASECODE) WHERE T1.C_METADATAXML IS NULL AND T1.M_APPLIED_PATH  <> '@' AND T1.C_SYNONYM_CD = 'N') U0)) U1)";
        createTempTableIfNeeded(connection, databaseProduct);
    }
    
    void execute() throws SQLException {
        try (Statement stmt = this.connection.createStatement()) {
            stmt.execute(statement);
        }
    }
    
    private void createTempTableIfNeeded(Connection connection, DatabaseProduct databaseProduct) throws SQLException {
        
        switch (databaseProduct) {
            case POSTGRESQL:
                try (Statement stmt = connection.createStatement()) {
                    stmt.execute("CREATE GLOBAL TEMPORARY TABLE IF NOT EXISTS EK_TEMP_PROPERTIES (EK_UNIQUE_ID VARCHAR(50), M_APPLIED_PATH VARCHAR(700), C_NAME VARCHAR(2000), VALUETYPE_CD VARCHAR(50), PROPERTYNAME VARCHAR(255), C_METADATAXML TEXT) ON COMMIT DELETE ROWS");
                }
                break;
            default:
                break;
        }
    }

}
