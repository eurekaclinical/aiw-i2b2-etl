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

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.sql.Date;
import java.sql.Timestamp;

public class VisitDimension extends AbstractRecord {

    private String encryptedVisitId;
    private Date startDate;
    private Date endDate;
    private String visitSourceSystem;
    private String encryptedPatientIdSourceSystem;
    private ActiveStatusCode activeStatus;
    private String encryptedPatientId;
    private Timestamp downloaded;
    private Timestamp updated;

    private String encryptedVisitIdSourceSystem;

    public VisitDimension() {
    }

    public void setEncryptedVisitId(String encryptedVisitId) {
        this.encryptedVisitId = encryptedVisitId;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    public void setVisitSourceSystem(String visitSourceSystem) {
        this.visitSourceSystem = visitSourceSystem;
    }

    public void setEncryptedPatientIdSourceSystem(String encryptedPatientIdSourceSystem) {
        this.encryptedPatientIdSourceSystem = encryptedPatientIdSourceSystem;
    }

    public void setActiveStatus(ActiveStatusCode activeStatus) {
        this.activeStatus = activeStatus;
    }

    public void setEncryptedPatientId(String encryptedPatientId) {
        this.encryptedPatientId = encryptedPatientId;
    }

    public void setDownloaded(Timestamp downloaded) {
        this.downloaded = downloaded;
    }

    public void setEncryptedVisitIdSourceSystem(String encryptedVisitIdSourceSystem) {
        this.encryptedVisitIdSourceSystem = encryptedVisitIdSourceSystem;
    }
    
    public String getEncryptedVisitId() {
        return this.encryptedVisitId;
    }

    public String getEncryptedVisitIdSourceSystem() {
        return this.encryptedVisitIdSourceSystem;
    }

    public Date getStartDate() {
        return startDate;
    }

    public Date getEndDate() {
        return endDate;
    }

    public String getVisitSourceSystem() {
        return visitSourceSystem;
    }

    public String getEncryptedPatientIdSourceSystem() {
        return encryptedPatientIdSourceSystem;
    }

    public ActiveStatusCode getActiveStatus() {
        return activeStatus;
    }

    public String getEncryptedPatientId() {
        return encryptedPatientId;
    }

    public Timestamp getDownloaded() {
        return downloaded;
    }

    public Timestamp getUpdated() {
        return updated;
    }

    public void setUpdated(Timestamp updated) {
        this.updated = updated;
    }
    
    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

}
