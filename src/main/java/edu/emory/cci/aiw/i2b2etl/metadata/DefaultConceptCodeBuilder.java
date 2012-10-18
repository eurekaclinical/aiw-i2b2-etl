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
package edu.emory.cci.aiw.i2b2etl.metadata;

import edu.emory.cci.aiw.i2b2etl.util.CodeUtil;
import org.protempa.proposition.value.Value;

/**
 *
 * @author Andrew Post
 */
final class DefaultConceptCodeBuilder extends ConceptCodeBuilder {
    private String propositionId;
    private String propertyName;
    private Value value;
    
    DefaultConceptCodeBuilder(Metadata metadata) {
        super(metadata);
    }

    String getPropertyName() {
        return propertyName;
    }

    void setPropertyName(String propertyName) {
        this.propertyName = propertyName;
    }

    String getPropositionId() {
        return propositionId;
    }

    void setPropositionId(String propositionId) {
        this.propositionId = propositionId;
    }

    Value getValue() {
        return value;
    }

    void setValue(Value value) {
        this.value = value;
    }
    
    @Override
    String build() throws InvalidConceptCodeException {
        StringBuilder conceptCodeBuilder = new StringBuilder();
            conceptCodeBuilder.append(this.propositionId);
        if (this.propertyName != null) {
            conceptCodeBuilder.append('.');
            conceptCodeBuilder.append(this.propertyName);
        }
        if (this.value != null) {
            conceptCodeBuilder.append(':');
            conceptCodeBuilder.append(this.value.getFormatted());
        }
        String conceptCode = conceptCodeBuilder.toString();
        
        boolean chopped = false;
        int number = 0;
        String codeWithoutNumber = null;
        int numLength = Math.min(5, CodeUtil.CODE_LENGTH);
        while (getMetadata().isInConceptCodeCache(conceptCode) 
                || conceptCode.length() > CodeUtil.CODE_LENGTH) {
            if (String.valueOf(number).length() > numLength) {
                throw new InvalidConceptCodeException(
                "Could not create a concept code that is not already in use");
            }
            if (!chopped) {
                codeWithoutNumber = 
                        conceptCode.substring(0, Math.min(conceptCode.length(), 
                        CodeUtil.CODE_LENGTH - numLength));
            }
            
            conceptCode = codeWithoutNumber + number++;
        }
        getMetadata().addToConceptCodeCache(conceptCode);
        return conceptCode;
    }
}
