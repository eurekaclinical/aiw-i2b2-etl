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

import java.util.HashMap;
import java.util.Map;

/**
 * Represents the possible values of the <code>VITAL_STATUS_CD</code> attribute 
 * in the <code>PATIENT_DIMENSION</code> table.
 * 
 * @author Andrew Post
 */
public enum VitalStatusCode {
    LIVING("N"),
    DECEASED_ACCURATE_TO_DAY("Y"),
    DECEASED_ACCURATE_TO_MONTH("M"),
    DECEASED_ACCURATE_TO_YEAR("X");
    
    private static final Map<String, VitalStatusCode> fromCodeMap =
            new HashMap<>();
    static {
        fromCodeMap.put("N", LIVING);
        fromCodeMap.put("Y", DECEASED_ACCURATE_TO_DAY);
        fromCodeMap.put("M", DECEASED_ACCURATE_TO_MONTH);
        fromCodeMap.put("X", DECEASED_ACCURATE_TO_YEAR);
    }
    
    private final String code;
    
    public static VitalStatusCode getInstance(Boolean knownDeceased) {
        if (knownDeceased != null && knownDeceased.booleanValue()) {
            return DECEASED_ACCURATE_TO_DAY;
        } else {
            return LIVING;
        }
    }
    
    public static VitalStatusCode fromCode(String code) {
        return fromCodeMap.get(code);
    }
    
    private VitalStatusCode(String code) {
        this.code = code;
    }
    
    /**
     * Gets the code to put into the <code>VITAL_STATUS_CD</code> attribute.
     * 
     * @return a code {@link String}.
     */
    public String getCode() {
        return this.code;
    }
}
