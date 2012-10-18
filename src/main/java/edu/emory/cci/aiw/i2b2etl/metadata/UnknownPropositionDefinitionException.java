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

/**
 *
 * @author Andrew Post
 */
class UnknownPropositionDefinitionException extends Exception {

    UnknownPropositionDefinitionException(Throwable thrwbl) {
        super(thrwbl);
    }

    UnknownPropositionDefinitionException(String propId, Throwable thrwbl) {
        super("The proposition definition '" + propId + "' is unknown", thrwbl);
    }

    UnknownPropositionDefinitionException(String propId) {
        super("The proposition definition '" + propId + "' is unknown");
    }

    UnknownPropositionDefinitionException() {
    }
    
}
