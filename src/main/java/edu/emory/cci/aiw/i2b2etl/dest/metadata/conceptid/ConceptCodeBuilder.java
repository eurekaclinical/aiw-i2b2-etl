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
package edu.emory.cci.aiw.i2b2etl.dest.metadata.conceptid;

import edu.emory.cci.aiw.i2b2etl.dest.metadata.Metadata;
import edu.emory.cci.aiw.etl.util.CodeUtil;

/**
 *
 * @author Andrew Post
 */
abstract class ConceptCodeBuilder {
    private static int NUM_LENGTH = Math.min(5, CodeUtil.CODE_LENGTH);
    private final Metadata metadata;
    
    ConceptCodeBuilder(Metadata metadata) {
        assert metadata != null : "metadata cannot be null";
        this.metadata = metadata;
        
    }
    
    protected Metadata getMetadata() {
        return this.metadata;
    }
    
    abstract String build() throws InvalidConceptCodeException;
    
    protected String truncateIfNeeded(String conceptCode) throws InvalidConceptCodeException {
        boolean chopped = false;
        int number = 0;
        String codeWithoutNumber = null;
        while (conceptCode.length() > CodeUtil.CODE_LENGTH) {
            if (String.valueOf(number).length() > NUM_LENGTH) {
                throw new InvalidConceptCodeException(
                        "Could not create a concept code that is not already in use");
            }
            if (!chopped) {
                codeWithoutNumber =
                        conceptCode.substring(0, Math.min(conceptCode.length(),
                                CodeUtil.CODE_LENGTH - NUM_LENGTH));
            }
            
            conceptCode = codeWithoutNumber + number++;
        }
        return conceptCode;
    }
    
}
