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
import java.util.Date;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.protempa.proposition.value.*;

public class ObservationFact implements Record {

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
    private String displayName;
    private Value value;
    private java.util.Date startDate;
    private java.util.Date endDate;
    private PatientDimension patient;
    private VisitDimension visit;
    private ProviderDimension provider;
    private Concept concept;
    private String sourceSystem;
    private String units;
    private ValueFlagCode valueFlagCode;
    private boolean rejected;
    private long instanceNum;
    private String modifierCd;
    private Date updateDate;
    private Date downloadDate;

    public ObservationFact() {
        this.modifierCd = "@";  //using the default value since we do not use this i2b2 feature currently
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public void setValue(Value value) {
        this.value = value;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    public void setPatient(PatientDimension patient) {
        this.patient = patient;
    }

    public void setVisit(VisitDimension visit) {
        this.visit = visit;
    }

    public void setProvider(ProviderDimension provider) {
        this.provider = provider;
    }

    public void setConcept(Concept concept) {
        this.concept = concept;
    }

    public void setSourceSystem(String sourceSystem) {
        this.sourceSystem = sourceSystem;
    }

    public void setUnits(String units) {
        this.units = units;
    }

    public void setValueFlagCode(ValueFlagCode valueFlagCode) {
        this.valueFlagCode = valueFlagCode;
    }

    public void setRejected(boolean rejected) {
        this.rejected = rejected;
    }

    public void setInstanceNum(long instanceNum) {
        this.instanceNum = instanceNum;
    }

    public void setModifierCd(String modifierCd) {
        this.modifierCd = modifierCd;
    }

    public void setUpdateDate(Date updateDate) {
        this.updateDate = updateDate;
    }

    public void setDownloadDate(Date downloadDate) {
        this.downloadDate = downloadDate;
    }
    
    @Override
    public boolean isRejected() {
        return rejected;
    }
    
    public Concept getConcept() {
        return concept;
    }

    public Date getStartDate() {
        return startDate;
    }

    public Date getEndDate() {
        return endDate;
    }

    public Value getValue() {
        return value;
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

    public long getInstanceNum() {
        return this.instanceNum;
    }
    
    public String getModifierCd() {
        return this.modifierCd;
    }
    
    public Date getDownloadDate() {
        return this.downloadDate;
    }

    public Date getUpdateDate() {
        return updateDate;
    }
    
    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
