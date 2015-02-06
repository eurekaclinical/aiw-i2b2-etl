package edu.emory.cci.aiw.i2b2etl.ksb;

/*
 * #%L
 * AIW i2b2 ETL
 * %%
 * Copyright (C) 2012 - 2015 Emory University
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

import org.protempa.PropertyDefinition;
import org.protempa.proposition.value.ValueType;

/**
 *
 * @author Andrew Post
 */
class PropertyDefinitionPartial {
    private final String name;
    private final ValueType valueType;
    private final String valueSetId;
    private final String declaringPropId;
    private final String fullName;

    PropertyDefinitionPartial(String fullName, String name, ValueType valueType, String valueSetId, String declaringPropId) {
        this.fullName = fullName;
        this.name = name;
        this.valueType = valueType;
        this.valueSetId = valueSetId;
        this.declaringPropId = declaringPropId;
    }

    public String getFullName() {
        return fullName;
    }
    
    PropertyDefinition getPropertyDefinition(String propId) {
        return new PropertyDefinition(propId, this.name, this.valueType, this.valueSetId, this.declaringPropId);
    }
}
