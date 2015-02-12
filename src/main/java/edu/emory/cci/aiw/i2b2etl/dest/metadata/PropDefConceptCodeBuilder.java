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
package edu.emory.cci.aiw.i2b2etl.dest.metadata;

import org.protempa.proposition.value.Value;

/**
 *
 * @author Andrew Post
 */
final class PropDefConceptCodeBuilder extends ConceptCodeBuilder {
    private String propositionId;
    private String propertyName;
    private Value value;
    
    PropDefConceptCodeBuilder(Metadata metadata) {
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
        
        conceptCode = truncateIfNeeded(conceptCode);
        return conceptCode;
    }

}
