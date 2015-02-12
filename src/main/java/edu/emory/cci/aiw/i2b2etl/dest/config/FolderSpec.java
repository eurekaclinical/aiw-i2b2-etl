package edu.emory.cci.aiw.i2b2etl.dest.config;

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

import edu.emory.cci.aiw.i2b2etl.dest.metadata.ValueTypeCode;

/**
 *
 * @author Andrew Post
 */
public class FolderSpec {
    private static final ModifierSpec[] EMPTY_MODIFIER_ARRAY = new ModifierSpec[0];
    private final String displayName;
    private final String[] propositions;
    private final String property;
    private final String conceptCodePrefix;
    private final ValueTypeCode valueType;
    private final Boolean alreadyLoaded;
    private final ModifierSpec[] modifiers;

    public FolderSpec(String displayName, String[] propositions, String property, String conceptCodePrefix, ValueTypeCode valueType, boolean alreadyLoaded, ModifierSpec[] modifiers) {
        this.displayName = displayName;
        this.propositions = propositions.clone();
        this.property = property;
        this.conceptCodePrefix = conceptCodePrefix;
        this.valueType = valueType;
        this.alreadyLoaded = alreadyLoaded;
        if (modifiers != null)  {
            this.modifiers = modifiers.clone();
        } else {
            this.modifiers = EMPTY_MODIFIER_ARRAY;
        }
    }

    public String getDisplayName() {
        return displayName;
    }

    public String[] getPropositions() {
        return propositions;
    }

    public String getProperty() {
        return property;
    }

    public String getConceptCodePrefix() {
        return conceptCodePrefix;
    }

    public ValueTypeCode getValueType() {
        return valueType;
    }

    public boolean isAlreadyLoaded() {
        return alreadyLoaded;
    }
    
    public ModifierSpec[] getModifiers() {
        return this.modifiers.clone();
    }
    
}
