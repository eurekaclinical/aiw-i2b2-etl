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
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

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
     * Constructs a provider dimension record.
	 *
	 * @param concept the i2b2 concept representing the provider
	 * @param sourceSystem the source system where the provider came from
     */
	public ProviderDimension(Concept concept, String sourceSystem) {
        if (concept == null) {
            throw new IllegalArgumentException("concept cannot be null");
        }
        this.concept = concept;
        this.sourceSystem = sourceSystem;
    }

    /**
     * Returns the provider's unique id, or
     * <code>null</code> if the provider is not recorded or unknown.
     *
     * @return a {@link String}.
     */
    public Concept getConcept() {
        return this.concept;
    }

    /**
     * Returns the source system of this provider, or
     * <code>null</code> if it is not recorded.
     *
     * @return a {@link String}.
     */
    public String getSourceSystem() {
        return this.sourceSystem;
    }

    public static void insertAll(Collection<ProviderDimension> providers,
            Connection cn) throws SQLException {
        Logger logger = TableUtil.logger();
        int batchSize = 1000;
        int counter = 0;
        int commitSize = 10000;
        int commitCounter = 0;
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
                    ps.addBatch();
                    ps.clearParameters();
                    counter++;
                    commitCounter++;
                    if (counter >= batchSize) {
                        ps.executeBatch();
                        ps.clearBatch();
                        counter = 0;
                    }
                    if (commitCounter >= commitSize) {
                        cn.commit();
                        commitCounter = 0;
                    }

                    logger.log(Level.FINEST, "DB_RD_INSERT {0}", provider);
                } catch (SQLException e) {
                    logger.log(Level.SEVERE, "DB_RD_INSERT_FAIL {0}", provider);
                    throw e;
                }
            }
            if (counter > 0) {
                ps.executeBatch();
                ps.clearBatch();
            }
            if (commitCounter > 0) {
                cn.commit();
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
