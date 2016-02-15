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
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author Andrew Post
 */
public class ModifierDimensionLoader extends ConceptHierarchyLoader {

    private final ModifierDimensionHandler handler;
    /*
     * A cache of modifier paths so that we do not insert multiple records
     * with the same modifier path.
     */
    private final Set<String> modifierPaths;

    public ModifierDimensionLoader(ModifierDimensionHandler handler) {
        this.handler = handler;
        this.modifierPaths = new HashSet<>();
    }

    @Override
    protected void loadConcept(Concept concept) throws SQLException {
        if (concept.isInUse() && concept.isModifier()) {
            for (String path : concept.getHierarchyPaths()) {
                /*
                 * Modifier paths are not globally unique in the metadata
                 * schema, but there can only be one record per modifier path
                 * in the modifier dimension. Thus, we need to resolve
                 * duplicates.
                 */
                if (this.modifierPaths.add(path)) {
                    ModifierDimension modifierDimension = new ModifierDimension();
                    modifierDimension.setPath(path);
                    modifierDimension.setConceptCode(concept.getConceptCode());
                    modifierDimension.setDisplayName(concept.getDisplayName());
                    modifierDimension.setSourceSystemCode(concept.getSourceSystemCode());
                    modifierDimension.setUpdated(TableUtil.setTimestampAttribute(concept.getUpdated()));
                    modifierDimension.setDownloaded(TableUtil.setTimestampAttribute(concept.getDownloaded()));
                    this.handler.insert(modifierDimension);
                }
            }
        }
    }

}
