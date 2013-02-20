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
 * Represents the possible values of the <code>VITAL_STATUS_CD</code> attribute 
 * in the <code>PATIENT_DIMENSION</code> table.
 * 
 * @author Andrew Post
 */
enum VitalStatusCode {
    LIVING("N"),
    DECEASED_ACCURATE_TO_DAY("Y"),
    DECEASED_ACCURATE_TO_MONTH("M"),
    DECEASED_ACCURATE_TO_YEAR("X");
    
    private final String code;
    
    static VitalStatusCode getInstance(Date deathDate) {
        if (deathDate != null) {
            return DECEASED_ACCURATE_TO_DAY;
        } else {
            return LIVING;
        }
    }
    
    private VitalStatusCode(String code) {
        this.code = code;
    }
    
    /**
     * Gets the code to put into the <code>VITAL_STATUS_CD</code> attribute.
     * 
     * @return a code {@link String}.
     */
    String getCode() {
        return this.code;
    }
}
