package edu.emory.cci.aiw.i2b2etl.dest;

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

import org.protempa.SourceId;
import org.protempa.SourceIdBuilder;

/**
 *
 * @author Andrew Post
 */
public class I2B2QueryResultsHandlerSourceIdBuilder implements SourceIdBuilder {

    @Override
    public SourceId build() {
        return I2B2QueryResultsHandlerSourceId.getInstance();
    }

    @Override
    public boolean equals(Object obj) {
        return obj != null && getClass() == obj.getClass();
    }

    @Override
    public int hashCode() {
        return 1;
    }
    
}
