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

import java.util.Date;
import java.util.EnumMap;
import java.util.Map;
import org.protempa.proposition.value.AbsoluteTimeGranularity;
import org.protempa.proposition.value.Granularity;

/**
 * Represents the possible values of the <code>ACTIVE_STATUS_CD</code> 
 * attribute in the <code>VISIT_DIMENSION</code> table.
 * 
 * @author Andrew Post
 */
public class ActiveStatusCode {
    
    /**
     * Infer the correct active status code given what dates are available and
     * whether or not the encounter information is finalized.
     * 
     * @param bFinal <code>true</code> if the visit information is finalized
     * according to the EHR, <code>false</code> if not. This parameter is
     * ignored if the visit has neither a start date nor an end date.
     * @param startDate the start date of the visit. May be <code>null</code>.
     * @param endDate the end date of the visit. May be <code>null</code>.
     * @return the appropriate active status code.
     */
    public static String getInstance(
            Date startDate, Granularity startGran, 
            Date endDate, Granularity endGran) {
        ActiveStatusCodeStartDate codeStartDate;
        if (startDate == null) {
            codeStartDate = ActiveStatusCodeStartDate.UNKNOWN;
        } else if (startGran == AbsoluteTimeGranularity.YEAR) {
            codeStartDate = ActiveStatusCodeStartDate.YEAR;
        } else if (startGran == AbsoluteTimeGranularity.MONTH) {
            codeStartDate = ActiveStatusCodeStartDate.MONTH;
        } else if (startGran == AbsoluteTimeGranularity.DAY) {
            codeStartDate = ActiveStatusCodeStartDate.DAY;
        } else if (startGran == AbsoluteTimeGranularity.HOUR) {
            codeStartDate = ActiveStatusCodeStartDate.HOUR;
        } else if (startGran == AbsoluteTimeGranularity.MINUTE) {
            codeStartDate = ActiveStatusCodeStartDate.MINUTE;
        } else if (startGran == AbsoluteTimeGranularity.SECOND) {
            codeStartDate = ActiveStatusCodeStartDate.SECOND;
        } else {
            codeStartDate = ActiveStatusCodeStartDate.NULL;
        }
        
        ActiveStatusCodeEndDate codeEndDate;
        if (startDate == null) {
            codeEndDate = ActiveStatusCodeEndDate.UNKNOWN;
        } else if (startGran == AbsoluteTimeGranularity.YEAR) {
            codeEndDate = ActiveStatusCodeEndDate.YEAR;
        } else if (startGran == AbsoluteTimeGranularity.MONTH) {
            codeEndDate = ActiveStatusCodeEndDate.MONTH;
        } else if (startGran == AbsoluteTimeGranularity.DAY) {
            codeEndDate = ActiveStatusCodeEndDate.DAY;
        } else if (startGran == AbsoluteTimeGranularity.HOUR) {
            codeEndDate = ActiveStatusCodeEndDate.HOUR;
        } else if (startGran == AbsoluteTimeGranularity.MINUTE) {
            codeEndDate = ActiveStatusCodeEndDate.MINUTE;
        } else if (startGran == AbsoluteTimeGranularity.SECOND) {
            codeEndDate = ActiveStatusCodeEndDate.SECOND;
        } else {
            codeEndDate = ActiveStatusCodeEndDate.NULL;
        }
        
        return codeEndDate.getCode() + codeStartDate.getCode();
    }
    
    private ActiveStatusCode() {
    }
    
}
