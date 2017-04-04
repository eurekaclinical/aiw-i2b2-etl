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
import org.protempa.KnowledgeSourceReadException;

/**
 *
 * @author Andrew Post
 */
class PropertiesTempTableHandler {

    private final Connection connection;
    private final String statement;
    
    PropertiesTempTableHandler(QuerySupport querySupport, Connection connection, String table, String eurekaIdColumn) throws SQLException, KnowledgeSourceReadException {
        this.connection = connection;
        
        this.statement = "INSERT INTO ek_temp_properties (SELECT " + eurekaIdColumn + ", M_APPLIED_PATH, DECLARING_CONCEPT_ID, DISPLAYNAME C_NAME, VALUETYPE_CD, PROPERTYNAME, C_METADATAXML FROM" +
        " ((SELECT a2." + eurekaIdColumn + ", a2.M_APPLIED_PATH, a3." + eurekaIdColumn + " DECLARING_CONCEPT_ID, a2.C_NAME DISPLAYNAME, a2.VALUETYPE_CD, a2.C_BASECODE PROPERTYNAME, a2.C_METADATAXML FROM " +
        table +
        " a2 " +
        "JOIN " +
        table +
        " a3 ON (a3.C_FULLNAME LIKE a2.M_APPLIED_PATH " + 
        (querySupport.getDatabaseProduct() == DatabaseProduct.POSTGRESQL ? "ESCAPE '' " : "") +
        " AND a3.C_SYNONYM_CD='N') " + 
        "WHERE a2.C_METADATAXML IS NOT NULL AND a2.M_APPLIED_PATH  <> '@' AND a2.C_BASECODE IS NOT NULL AND a2.C_SYNONYM_CD = 'N' " +
        " AND NOT EXISTS (SELECT * FROM " +
        table + 
        " aa1 " + 
        "WHERE aa1.c_fullname like a2.m_applied_path " +
        (querySupport.getDatabaseProduct() == DatabaseProduct.POSTGRESQL ? "ESCAPE '' " : "") +
        "AND a3.c_hlevel > aa1.c_hlevel))" +
        " UNION ALL" +
        " (SELECT NULL, M_APPLIED_PATH, DECLARING_CONCEPT DECLARING_CONCEPT_ID, DISPLAYNAME, NULL, PROPERTYNAME, NULL " +
        "FROM (SELECT DISTINCT a1.M_APPLIED_PATH, a2.DISPLAYNAME, a2.DECLARING_CONCEPT, NULL, a2.PROPERTYNAME FROM " +
        table +
        " a1 JOIN ek_modifier_interp a2 on (a1.c_basecode=a2.c_basecode and a1.C_SYNONYM_CD = 'N' and a1.c_metadataxml is null) JOIN " +
        table + 
        " a3 on (a2.declaring_concept=a3.ek_unique_id AND a3.c_fullname LIKE a1.m_applied_path " +
        (querySupport.getDatabaseProduct() == DatabaseProduct.POSTGRESQL ? "ESCAPE '' " : "") +
        "and a3.c_synonym_cd='N') " + 
        ") U0)) U1)";
    }
    
    void execute() throws SQLException {
        try (Statement stmt = this.connection.createStatement()) {
            stmt.execute(statement);
        }
    }
    
    static void createTempTableIfNeeded(Connection connection, DatabaseProduct databaseProduct) throws SQLException {
        
        switch (databaseProduct) {
            case POSTGRESQL:
                try (Statement stmt = connection.createStatement()) {
                    stmt.execute("CREATE GLOBAL TEMPORARY TABLE IF NOT EXISTS EK_TEMP_PROPERTIES (EK_UNIQUE_ID VARCHAR(50), M_APPLIED_PATH VARCHAR(700), DECLARING_CONCEPT_ID VARCHAR(50), C_NAME VARCHAR(2000), VALUETYPE_CD VARCHAR(50), PROPERTYNAME VARCHAR(255), C_METADATAXML TEXT) ON COMMIT PRESERVE ROWS");
                }
                break;
            default:
                break;
        }
    }

}
