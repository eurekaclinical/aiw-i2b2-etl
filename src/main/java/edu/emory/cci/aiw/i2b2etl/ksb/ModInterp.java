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

/**
 *
 * @author Andrew Post
 */
class ModInterp {
    private final String propertyName;
    private final String displayName;

    ModInterp(String propertyName, String displayName) {
        this.propertyName = propertyName;
        this.displayName = displayName;
    }

    String getPropertyName() {
        return propertyName;
    }

    String getDisplayName() {
        return displayName;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
    
}
