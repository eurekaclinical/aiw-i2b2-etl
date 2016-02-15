/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
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

/**
 *
 * @author Andrew Post
 */
public final class ConceptDimensionLoader extends ConceptHierarchyLoader {

    private final ConceptDimensionHandler handler;

    public ConceptDimensionLoader(ConceptDimensionHandler handler) {
        this.handler = handler;
    }

    @Override
    protected void loadConcept(Concept concept) throws SQLException {
        if (concept.isInUse() && !concept.isModifier()) {
            for (String path : concept.getHierarchyPaths()) {
                ConceptDimension conceptDimension = new ConceptDimension();
                conceptDimension.setPath(path);
                conceptDimension.setConceptCode(concept.getConceptCode());
                conceptDimension.setDisplayName(concept.getDisplayName());
                conceptDimension.setSourceSystemCode(concept.getSourceSystemCode());
                conceptDimension.setDownloaded(TableUtil.setTimestampAttribute(concept.getDownloaded()));
                conceptDimension.setUpdated(TableUtil.setTimestampAttribute(concept.getUpdated()));
                this.handler.insert(conceptDimension);
            }
        }
    }
}
