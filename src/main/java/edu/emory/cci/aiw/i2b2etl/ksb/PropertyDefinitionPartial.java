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

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.protempa.PropertyDefinition;
import org.protempa.proposition.value.ValueType;

/**
 *
 * @author Andrew Post
 */
final class PropertyDefinitionPartial {
    private final String name;
    private final ValueType valueType;
    private final String declaringPropId;
    private final String fullName;
    private final String symbol;
    private final boolean hasClob;

    PropertyDefinitionPartial(String fullName, String symbol, String name, ValueType valueType, String declaringPropId, boolean hasClob) {
        this.fullName = fullName;
        this.symbol = symbol;
        this.name = name;
        this.valueType = valueType;
        this.declaringPropId = declaringPropId;
        this.hasClob = hasClob;
    }

    public boolean getHasClob() {
        return hasClob;
    }
    
    String getFullName() {
        return fullName;
    }

    public String getDeclaringPropId() {
        return declaringPropId;
    }

    public String getSymbol() {
        return symbol;
    }
    
    PropertyDefinition getPropertyDefinition(String propId) {
        return getPropertyDefinition(propId, null, null);
    }
    
    PropertyDefinition getPropertyDefinition(String propId, String propertySym, String propertyName) {
            return new PropertyDefinition(propId, propertySym != null ? propertySym : this.symbol, propertyName != null ? propertyName : this.name, this.valueType, propertySym != null ? propertySym : this.symbol, this.declaringPropId);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
    
    
}
