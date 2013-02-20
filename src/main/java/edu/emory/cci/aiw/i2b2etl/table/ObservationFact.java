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
import org.apache.commons.lang.builder.ToStringBuilder;
import org.protempa.proposition.value.*;

public class ObservationFact {
    
    //	CREATE TABLE  "OBSERVATION_FACT" 
    //  (
    //  "ENCOUNTER_NUM"		NUMBER(38,0)  NOT NULL ENABLE, 
    //	"PATIENT_NUM"		NUMBER(38,0)  NOT NULL ENABLE, 
    //	"CONCEPT_CD"		VARCHAR2(50)  NOT NULL ENABLE, 
    //	"PROVIDER_ID"		VARCHAR2(50)  NOT NULL ENABLE, 
    //	"START_DATE"		DATE          NOT NULL ENABLE, 
    //	"MODIFIER_CD"		VARCHAR2(100) NOT NULL ENABLE, 
    //	"VALTYPE_CD"		VARCHAR2(50), 
    //	"TVAL_CHAR"			VARCHAR2(255), 
    //	"NVAL_NUM"			NUMBER(18,5), 
    //	"VALUEFLAG_CD"		VARCHAR2(50), 
    //	"QUANTITY_NUM"		NUMBER(18,5), 
    //	"INSTANCE_NUM"		NUMBER(18,0), 
    //	"UNITS_CD"			VARCHAR2(50), 
    //	"END_DATE"			DATE, 
    //	"LOCATION_CD"		VARCHAR2(50), 
    //	"CONFIDENCE_NUM"	NUMBER(18,5), 
    //	"OBSERVATION_BLOB"	CLOB, 
    //	"UPDATE_DATE"		DATE, 
    //	"DOWNLOAD_DATE"		DATE, 
    //	"IMPORT_DATE"		DATE, 
    //	"SOURCESYSTEM_CD"	VARCHAR2(50), 
    //	"UPLOAD_ID"			NUMBER(38,0), 
    //	CONSTRAINT "OBSERVATION_FACT_PK" PRIMARY KEY ("ENCOUNTER_NUM", "CONCEPT_CD", "PROVIDER_ID", "START_DATE", "MODIFIER_CD") ENABLE
    //  )
//	private static long ctr = 0L;
    private final String displayName;
    private final Value value;
    private final String observationBlob;
    private final java.util.Date startDate;
    private final java.util.Date endDate;
    private final PatientDimension patient;
    private final VisitDimension visit;
    private final ProviderDimension provider;
    private final Concept concept;
    private final String sourceSystem;
    private final String units;
    private final ValueFlagCode valueFlagCode;

    public ObservationFact(java.util.Date startDate, 
            java.util.Date finishDate,
            PatientDimension patient,
            VisitDimension visit, ProviderDimension provider,
            Concept concept, Value value, ValueFlagCode valueFlagCode,
            String observationBlob, String displayName,
            String units,
            String sourceSystem) {
        if (patient == null) {
            throw new IllegalArgumentException("patient cannot be null");
        }
        if (visit == null) {
            throw new IllegalArgumentException("visit cannot be null");
        }
        if (provider == null) {
            throw new IllegalArgumentException("provider cannot be null");
        }
        if (startDate == null) {
            throw new IllegalArgumentException("startDate cannot be null");
        }
        if (valueFlagCode == null) {
            throw new IllegalArgumentException("valueFlagCode cannot be null");
        }
        this.startDate = startDate;
        this.endDate = finishDate;
        this.patient = patient;
        this.visit = visit;
        this.provider = provider;
        this.concept = concept;
        this.displayName = displayName;
        this.value = value;
        this.observationBlob = observationBlob;
        this.sourceSystem = sourceSystem;
        this.units = units;
        this.valueFlagCode = valueFlagCode;
    }

    public Concept getConcept() {
        return concept;
    }

    public java.util.Date getStartDate() {
        return startDate;
    }

    public java.util.Date getEndDate() {
        return endDate;
    }

    public Value getValue() {
        return value;
    }

    public String getObservationBlob() {
        return observationBlob;
    }

    public String getDisplayName() {
        return displayName;
    }

    public PatientDimension getPatient() {
        return patient;
    }

    public ProviderDimension getProvider() {
        return provider;
    }

    public VisitDimension getVisit() {
        return visit;
    }
    
    public String getUnits() {
        return this.units;
    }
    
    public ValueFlagCode getValueFlagCode() {
        return this.valueFlagCode;
    }
    
    public String getSourceSystem() {
        return this.sourceSystem;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
