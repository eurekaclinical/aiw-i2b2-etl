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

import java.sql.Timestamp;
import java.util.Date;

/**
 * Represents records in the concept dimension.
 * 
 * The concept dimension has the following DDL:
 * <pre>
 * CREATE TABLE "CONCEPT_DIMENSION"
 * (
 * "CONCEPT_CD" VARCHAR2(50) NOT NULL ENABLE,
 * "CONCEPT_PATH" VARCHAR2(700) NOT NULL ENABLE,
 * "NAME_CHAR" VARCHAR2(2000),
 * "CONCEPT_BLOB" CLOB,
 * "UPDATE_DATE" DATE,
 * "DOWNLOAD_DATE" DATE,
 * "IMPORT_DATE" DATE,
 * "SOURCESYSTEM_CD" VARCHAR2(50),
 * "UPLOAD_ID" NUMBER(38,0),
 * CONSTRAINT "CONCEPT_DIMENSION_PK" PRIMARY KEY ("CONCEPT_PATH") ENABLE
 * )
 * </pre>
 * 
 * @author Andrew Post
 */
public class ConceptDimension extends AbstractRecord {
    private String path;
    private String conceptCode;
    private String displayName;
    private String sourceSystemCode;
    private Timestamp downloaded;
    private Timestamp updated;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getConceptCode() {
        return conceptCode;
    }

    public void setConceptCode(String conceptCode) {
        this.conceptCode = conceptCode;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getSourceSystemCode() {
        return sourceSystemCode;
    }

    public void setSourceSystemCode(String sourceSystemCode) {
        this.sourceSystemCode = sourceSystemCode;
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
    
}
