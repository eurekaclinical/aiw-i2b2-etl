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
package edu.emory.cci.aiw.i2b2etl.dest.table;

import edu.emory.cci.aiw.etl.table.AbstractRecord;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.Concept;
import java.sql.Timestamp;
import org.apache.commons.lang3.builder.ToStringBuilder;


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
public class ProviderDimension extends AbstractRecord {

    private Concept concept;
    private String sourceSystem;
    private Timestamp downloaded;
    private Timestamp updated;
    private Timestamp deleted;

    public ProviderDimension() {
    }
    
    public void setConcept(Concept concept) {
        this.concept = concept;
    }

    public void setSourceSystem(String sourceSystem) {
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

    public Timestamp getDownloaded() {
        return downloaded;
    }

    public void setDownloaded(Timestamp downloaded) {
        this.downloaded = downloaded;
    }

    public Timestamp getUpdated() {
        return updated;
    }

    public void setUpdated(Timestamp updated) {
        this.updated = updated;
    }

    public Timestamp getDeleted() {
        return deleted;
    }

    public void setDeleted(Timestamp deleted) {
        this.deleted = deleted;
    }
    
    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

}
