/*
 * #%L
 * AIW i2b2 ETL
 * %%
 * Copyright (C) 2012 - 2013 Emory University
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
package edu.emory.cci.aiw.i2b2etl.table;

import edu.emory.cci.aiw.i2b2etl.metadata.Concept;
import edu.emory.cci.aiw.i2b2etl.metadata.MetadataUtil;
import java.sql.*;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * Represents records in the i2b2 provider dimension.
 * 
 * The provider dimension has the following DDL:
 * <pre>
 *   CREATE TABLE  "PROVIDER_DIMENSION" 
 *    	(
 *   	"PROVIDER_ID"		VARCHAR2(50) NOT NULL ENABLE, 
 *    	"PROVIDER_PATH"		VARCHAR2(700) NOT NULL ENABLE, 
 *    	"NAME_CHAR"			VARCHAR2(850), 
 *    	"PROVIDER_BLOB"		CLOB, 
 *    	"UPDATE_DATE"		DATE, 
 *    	"DOWNLOAD_DATE"		DATE, 
 *    	"IMPORT_DATE"		DATE, 
 *    	"SOURCESYSTEM_CD"	VARCHAR2(50), 
 *    	"UPLOAD_ID"			NUMBER(38,0), 
 *    	CONSTRAINT "PROVIDER_DIMENSION_PK" PRIMARY KEY ("PROVIDER_PATH", "PROVIDER_ID") ENABLE
 *    	)
 * </pre>
 * 
 * 
 * @author Andrew Post
 */
public class ProviderDimension {
    private static final Logger LOGGER = TableUtil.logger();
    
    private final Concept concept;
    private final String sourceSystem;
    
    /**
     * Constructs a provider dimension record. The provider path, which also
     * is in this dimension, is added later on with the 
     * {@link #setI2b2Path(java.lang.String) } after the ontology hierarchy is
     * created.
     * 
     * @param id the provider unique id, or <code>null</code> if the provider 
     * is not recorded or unknown.
     * @param firstName the provider's first name, if known.
     * @param middleName the provider's middle name, if known.
     * @param lastName the provider's last name, if known.
     * @param fullName the provider's full name, if known.
     */
    public ProviderDimension(Concept concept, String fullName, String sourceSystem) {
        if (concept == null) {
            throw new IllegalArgumentException("concept cannot be null");
        }
        this.concept = concept;
        this.sourceSystem = sourceSystem;
    }
    
    /**
     * Returns the provider's unique id, or <code>null</code> if the provider 
     * is not recorded or unknown.
     * 
     * @return a {@link String}.
     */
    public Concept getConcept() {
        return this.concept;
    }
    
    /**
     * Returns the source system of this provider, or <code>null</code> if it
     * is not recorded.
     * 
     * @return a {@link String}. 
     */
    public String getSourceSystem() {
        return this.sourceSystem;
    }
    
    public static void insertFacts(Connection dataSchemaConnection) throws SQLException {
        PreparedStatement stmt = 
                dataSchemaConnection.prepareStatement(
                "INSERT INTO OBSERVATION_FACT (ENCOUNTER_NUM, PATIENT_NUM, CONCEPT_CD, PROVIDER_ID, START_DATE, END_DATE, MODIFIER_CD, IMPORT_DATE) SELECT DISTINCT a1.ENCOUNTER_NUM, a1.PATIENT_NUM, a1.PROVIDER_ID as CONCEPT_CD, a1.PROVIDER_ID, a2.START_DATE, a2.END_DATE, 0 as MODIFIER_CD, ? as IMPORT_DATE FROM OBSERVATION_FACT a1 JOIN VISIT_DIMENSION a2 on (a1.ENCOUNTER_NUM=a2.ENCOUNTER_NUM) WHERE a1.PROVIDER_ID <> '@' AND a2.START_DATE IS NOT NULL");
        try {
            stmt.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
            stmt.execute();
            stmt.close();
            stmt = null;
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ignored) {
                    
                }
            }
        }
        
        stmt = 
                dataSchemaConnection.prepareStatement(
                "SELECT DISTINCT a1.ENCOUNTER_NUM, a1.PATIENT_NUM, a1.PROVIDER_ID as CONCEPT_CD, a1.PROVIDER_ID, a2.START_DATE, a2.END_DATE, 0 as MODIFIER_CD, ? as IMPORT_DATE FROM OBSERVATION_FACT a1 JOIN VISIT_DIMENSION a2 on (a1.ENCOUNTER_NUM=a2.ENCOUNTER_NUM) WHERE a1.PROVIDER_ID <> '@' AND a2.START_DATE IS NULL");
        try {
            stmt.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
            ResultSet resultSet = stmt.executeQuery();
            try {
                while (resultSet.next()) {
                    LOGGER.log(Level.WARNING, "Rejected fact {0}; {1}; {2}; {3}; {4}; {5}; {6}; {7}", new Object[]{resultSet.getInt(1), resultSet.getInt(2), resultSet.getString(3), resultSet.getString(4), resultSet.getTimestamp(5), resultSet.getTimestamp(6), resultSet.getInt(7), resultSet.getTimestamp(8)});
                }
                resultSet.close();
                resultSet = null;
            } finally {
                if (resultSet != null) {
                    try {
                        resultSet.close();
                    } catch (SQLException ignored) {
                        
                    }
                }
            }
            stmt.close();
            stmt = null;
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ignored) {
                    
                }
            }
        }
    }

    public static void insertAll(Collection<ProviderDimension> providers, 
            Connection cn) throws SQLException {
        Logger logger = TableUtil.logger();
        PreparedStatement ps = cn.prepareStatement("insert into PROVIDER_DIMENSION values (?,?,?,?,?,?,?,?,?)");
        try {
            for (ProviderDimension provider : providers) {
                if (provider.concept.getI2B2Path() == null) {
                    throw new AssertionError("i2b2path cannot be null: " + provider);
                }
                try {
                    ps.setString(1, TableUtil.setStringAttribute(provider.concept.getConceptCode()));
                    ps.setString(2, provider.concept.getI2B2Path());
                    ps.setString(3, provider.concept.getDisplayName());
                    ps.setObject(4, null);
                    ps.setTimestamp(5, null);
                    ps.setTimestamp(6, null);
                    ps.setTimestamp(7, new java.sql.Timestamp(System.currentTimeMillis()));
                    ps.setString(8, MetadataUtil.toSourceSystemCode(provider.sourceSystem));
                    ps.setObject(9, null);

                    ps.execute();
                    logger.log(Level.FINEST, "DB_RD_INSERT {0}", provider);
                } catch (SQLException e) {
                    logger.log(Level.SEVERE, "DB_RD_INSERT_FAIL {0}", provider);
                    throw e;
                }
                ps.clearParameters();
            }
            ps.close();
            ps = null;
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException sqle) {
                    
                }
            }
        }
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
    
    
}
