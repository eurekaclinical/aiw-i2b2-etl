/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.emory.cci.aiw.i2b2etl.configuration;

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

/**
 *
 * @author arpost
 */
public class DataSpec {
    private final String key;
    private final String referenceName;
    private final String propertyName;
    private final String conceptCodePrefix;
    private final String start;
    private final String finish;
    private final String units;

    public DataSpec(String key, String referenceName, String propertyName, String conceptCodePrefix, String start, String finish, String units) {
        this.key = key;
        this.referenceName = referenceName;
        this.propertyName = propertyName;
        this.conceptCodePrefix = conceptCodePrefix;
        this.start = start;
        this.finish = finish;
        this.units = units;
    }

    public String getKey() {
        return key;
    }

    public String getReferenceName() {
        return referenceName;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public String getConceptCodePrefix() {
        return conceptCodePrefix;
    }

    public String getStart() {
        return start;
    }

    public String getFinish() {
        return finish;
    }

    public String getUnits() {
        return units;
    }
    
    
    
    
    
}
