/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.emory.cci.aiw.i2b2etl.table;

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

import edu.emory.cci.aiw.i2b2etl.metadata.Concept;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 *
 * @author Andrew Post
 */
public final class ConceptDimensionLoader extends ConceptHierarchyLoader {
    private final ConceptDimensionHandler handler;
    private final ConceptDimension conceptDimension;

    public ConceptDimensionLoader(ConceptDimensionHandler handler) {
        this.handler = handler;
        this.conceptDimension = new ConceptDimension();
    }
    
    @Override
    protected void loadConcept(Concept concept) throws SQLException {
        if (concept.isInUse()) {
            ArrayList<String> paths = concept.getHierarchyPaths();
            if (paths != null) {
                for (String path : paths) {
                    this.conceptDimension.setPath(path);
                    this.conceptDimension.setConceptCode(concept.getConceptCode());
                    this.conceptDimension.setDisplayName(concept.getDisplayName());
                    this.conceptDimension.setSourceSystemCode(concept.getSourceSystemCode());
                    this.conceptDimension.setDownloaded(concept.getDownloaded());
                    this.handler.insert(this.conceptDimension);
                }
            }
        }
    }
}
