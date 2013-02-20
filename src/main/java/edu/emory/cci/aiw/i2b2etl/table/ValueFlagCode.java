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

/**
 * Represents the possible values of the <code>VALUEFLAG_CD</code> attribute of
 * the <code>OBSERVATION_FACT</code> table.
 * 
 * @author Andrew Post
 */
public enum ValueFlagCode {
    NO_VALUE_FLAG("@"),
    ABNORMAL("A"),
    HIGH("H"),
    LOW("L"),
    BLOB_FIELD_IS_ENCRYPTED("X");
    
    private final String code;
    
    private ValueFlagCode(String code) {
        this.code = code;
    }

    /**
     * Gets the code to put into the <code>VALUEFLAG_CD</code> attribute.
     * 
     * @return a code {@link String}.
     */
    public String getCode() {
        return this.code;
    }
}
