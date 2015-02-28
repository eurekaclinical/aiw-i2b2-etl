package edu.emory.cci.aiw.i2b2etl.dest.table;

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

import edu.emory.cci.aiw.i2b2etl.dest.config.Data;
import edu.emory.cci.aiw.i2b2etl.dest.config.Settings;
import edu.emory.cci.aiw.i2b2etl.util.CodeUtil;
import java.sql.SQLException;
import java.util.Date;
import java.util.Map;
import org.arp.javautil.sql.ConnectionSpec;
import org.protempa.proposition.Proposition;
import org.protempa.proposition.TemporalProposition;
import org.protempa.proposition.UniqueId;
import org.protempa.proposition.interval.Interval;
import org.protempa.proposition.value.AbsoluteTimeGranularityUtil;
import org.protempa.proposition.value.Value;

/**
 *
 * @author Andrew Post
 */
public class VisitDimensionFactory extends DimensionFactory {
    private final String qrhId;
    private final VisitDimension visitDimension;
    private final VisitDimensionHandler visitDimensionHandler;
    private final EncounterMappingHandler encounterMappingHandler;
    private final Settings settings;

    public VisitDimensionFactory(String qrhId,
            Settings settings,
            Data data, ConnectionSpec dataConnectionSpec) throws SQLException {
        super(data);
        this.settings = settings;
        this.qrhId = qrhId;
        this.visitDimension = new VisitDimension();
        this.visitDimensionHandler = new VisitDimensionHandler(dataConnectionSpec);
        this.encounterMappingHandler = new EncounterMappingHandler(dataConnectionSpec);
    }
    
    public VisitDimension getInstance(String encryptedPatientId,
            String encryptedPatientIdSourceSystem,
            TemporalProposition encounterProp, 
            Map<UniqueId, Proposition> references) throws SQLException {
        Interval interval = encounterProp.getInterval();
        java.util.Date visitStartDate = encounterProp != null ? AbsoluteTimeGranularityUtil.asDate(interval.getMinStart()) : null;
        java.util.Date visitEndDate = encounterProp != null ? AbsoluteTimeGranularityUtil.asDate(interval.getMinFinish()) : null;
        Value visitId = encounterProp != null ? getField(this.settings.getVisitDimensionId(), encounterProp, references) : null;
        String visitIdStr;
        if (visitId != null) {
            visitIdStr = visitId.getFormatted();
        } else {
            visitIdStr = '@' + encryptedPatientId;
        }
        Value inout = encounterProp != null ? getField(this.settings.getVisitDimensionInOut(), encounterProp, references) : null;

        visitDimension.setEncryptedPatientId(TableUtil.setStringAttribute(encryptedPatientId));
        visitDimension.setStartDate(TableUtil.setDateAttribute(visitStartDate));
        visitDimension.setEndDate(TableUtil.setDateAttribute(visitEndDate));
        visitDimension.setVisitId(TableUtil.setStringAttribute(visitIdStr));
        visitDimension.setVisitIdSourceSystem(encounterProp != null ? encounterProp.getSourceSystem().getStringRepresentation() : this.qrhId);
        visitDimension.setVisitSourceSystem(this.qrhId);
        visitDimension.setEncryptedPatientIdSourceSystem(encryptedPatientIdSourceSystem);
        visitDimension.setActiveStatus(ActiveStatusCode.getInstance(visitStartDate, interval.getStartGranularity(), visitEndDate, interval.getFinishGranularity()));
        visitDimension.setInOut(CodeUtil.toString(inout));
        Date updated = encounterProp != null ? encounterProp.getUpdateDate() : null;
        if (updated == null && encounterProp != null) {
            updated = encounterProp.getCreateDate();
        }
        visitDimension.setUpdated(TableUtil.setTimestampAttribute(updated));
        visitDimension.setDownloaded(TableUtil.setTimestampAttribute(encounterProp != null ? encounterProp.getDownloadDate() : null));
        this.visitDimensionHandler.insert(visitDimension);
        this.encounterMappingHandler.insert(visitDimension);
        return visitDimension;
    }
    
    public void close() throws SQLException {
        this.visitDimensionHandler.close();
        this.encounterMappingHandler.close();
    }
}
