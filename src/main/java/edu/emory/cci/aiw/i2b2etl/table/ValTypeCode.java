/*
 * #%L
 * AIW i2b2 ETL
 * %%
 * Copyright (C) 2012 Emory University
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
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.emory.cci.aiw.i2b2etl.table;

/**
 * Represents the possible values of the <code>VALTYPE_CD</code> attribute of
 * the <code>OBSERVATION_FACT</code> table.
 * 
 * @author Andrew Post
 */
enum ValTypeCode {
    NO_VALUE("@"),
    NUMERIC("N"),
    TEXT("T"),
    RAW_TEXT("B"),
    NLP("NLP");
    
    private final String code;
    
    private ValTypeCode(String code) {
        this.code = code;
    }
    
    /**
     * Gets the code to put into the <code>VALTYPE_CD</code> attribute.
     * 
     * @return a code {@link String}.
     */
    String getCode() {
        return this.code;
    }
}
