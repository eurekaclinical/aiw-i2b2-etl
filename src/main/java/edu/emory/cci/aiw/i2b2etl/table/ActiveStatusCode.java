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

import java.util.Date;

/**
 * Represents the possible values of the <code>ACTIVE_STATUS_CD</code> 
 * attribute in the <code>VISIT_DIMENSION</code> table.
 * 
 * @author Andrew Post
 */
public enum ActiveStatusCode {
    FINAL("F"),
    PRELIMINARY("P"),
    ACTIVE("A"),
    NO_DATES(null);
    
    private final String code;
    
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
    public static ActiveStatusCode getInstance(boolean bFinal, 
            Date startDate, Date endDate) {
        if (startDate == null && endDate == null) {
            return NO_DATES;
        } else if (startDate != null && endDate == null) {
            return ACTIVE;
        } else if (bFinal) {
            return FINAL;
        } else {
            return PRELIMINARY;
        }
    }
    
    private ActiveStatusCode(String code) {
        this.code = code;
    }
    
    /**
     * Gets the code to put into the <code>ACTIVE_STATUS_CD</code> attribute.
     * 
     * @return a code {@link String}.
     */
    public String getCode() {
        return this.code;
    }
}
