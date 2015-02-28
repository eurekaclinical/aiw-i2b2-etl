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
package edu.emory.cci.aiw.i2b2etl.util;

import org.protempa.proposition.value.Value;

/**
 *
 * @author Andrew Post
 */
public class CodeUtil {
    public static final int CODE_LENGTH = 50;
    
    public static String toString(Value codeValue) {
        return codeValue != null ? truncateCodeStringIfNeeded(codeValue.getFormatted()) : null;
    }
    
    public static String truncateCodeStringIfNeeded(String codeString) {
        if (codeString != null && codeString.length() > CODE_LENGTH) {
            return codeString.substring(0, CODE_LENGTH);
        } else {
            return codeString;
        }
    }
    
}
