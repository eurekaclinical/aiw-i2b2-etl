package edu.emory.cci.aiw.i2b2etl.dest.table;

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
import edu.emory.cci.aiw.i2b2etl.dest.metadata.Concept;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Andrew Post
 */
public final class MetaTableConceptLoader extends ConceptHierarchyLoader {
    
    private static final Logger LOGGER = Logger.getLogger(MetaTableConceptLoader.class.getName());

    private final MetaTableConceptHandler handler;

    public MetaTableConceptLoader(MetaTableConceptHandler handler) {
        this.handler = handler;
    }

    @Override
    protected void loadConcept(Concept concept) throws SQLException {
        if (LOGGER.isLoggable(Level.FINER)) {
            LOGGER.log(Level.FINER, "Loading concept {0} into metadata table", concept.getFullName());
        }
        if (!concept.isAlreadyLoaded()) {
            if (LOGGER.isLoggable(Level.FINER)) {
                LOGGER.log(Level.FINER, "Really loading concept {0} into metadata table", concept.getFullName());
            }
            this.handler.insert(concept);
        }
    }

}
