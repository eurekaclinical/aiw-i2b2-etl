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

import edu.emory.cci.aiw.i2b2etl.dest.metadata.Concept;
import java.sql.Timestamp;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.protempa.proposition.value.*;

public class ObservationFact extends AbstractRecord {

    private String displayName;
    private Value value;
    private Timestamp startDate;
    private Timestamp endDate;
    private PatientDimension patient;
    private VisitDimension visit;
    private ProviderDimension provider;
    private Concept concept;
    private String sourceSystem;
    private String units;
    private ValueFlagCode valueFlagCode;
    private long instanceNum;
    private String modifierCd;
    private Timestamp updateDate;
    private Timestamp downloadDate;
    private Timestamp deletedDate;

    public ObservationFact() {
        this.modifierCd = "@";  //using the default value since we do not use this i2b2 feature currently
    }
    
    @Override
    public void reset() {
        super.reset();
        this.displayName = null;
        this.value = null;
        this.startDate = null;
        this.endDate = null;
        this.patient = null;
        this.visit = null;
        this.provider = null;
        this.concept = null;
        this.sourceSystem = null;
        this.units = null;
        this.valueFlagCode = null;
        this.instanceNum = 0;
        this.modifierCd = "@";
        this.updateDate = null;
        this.downloadDate = null;
        this.deletedDate = null;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public void setValue(Value value) {
        this.value = value;
    }

    public void setStartDate(Timestamp startDate) {
        this.startDate = startDate;
    }

    public void setEndDate(Timestamp endDate) {
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

    public void setInstanceNum(long instanceNum) {
        this.instanceNum = instanceNum;
    }

    public void setModifierCd(String modifierCd) {
        this.modifierCd = modifierCd;
    }

    public void setUpdateDate(Timestamp updateDate) {
        this.updateDate = updateDate;
    }

    public void setDownloadDate(Timestamp downloadDate) {
        this.downloadDate = downloadDate;
    }
    
    public Concept getConcept() {
        return concept;
    }

    public Timestamp getStartDate() {
        return startDate;
    }

    public Timestamp getEndDate() {
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
    
    public Timestamp getDownloadDate() {
        return this.downloadDate;
    }

    public Timestamp getUpdateDate() {
        return updateDate;
    }

    public Timestamp getDeletedDate() {
        return deletedDate;
    }

    public void setDeletedDate(Timestamp deletedDate) {
        this.deletedDate = deletedDate;
    }
    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
