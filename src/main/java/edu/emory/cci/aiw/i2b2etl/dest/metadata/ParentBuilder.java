package edu.emory.cci.aiw.i2b2etl.dest.metadata;

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

import org.protempa.ProtempaUtil;

/**
 *
 * @author arpAndrew Post
 */
abstract class ParentBuilder implements OntologyBuilder {
    private final String conceptCode;
    private final String displayName;
    private final OntologyBuilder[] children;
    private final Metadata metadata;
    private final boolean alreadyLoaded;
    
    ParentBuilder(Metadata metadata, String displayName, String conceptCode, boolean alreadyLoaded, OntologyBuilder... children) {
        assert metadata != null : "metadata cannot be null";
        assert displayName != null : "displayName cannot be null";
        assert conceptCode != null : "conceptCode cannot be null";
        ProtempaUtil.checkArrayForNullElement(children, "children");
        this.displayName = displayName;
        this.conceptCode = conceptCode;
        this.children = children;
        this.metadata = metadata;
        this.alreadyLoaded = alreadyLoaded;
    }

    @Override
    public void build(Concept parent) throws OntologyBuildException {
        Concept root = metadata.newContainerConcept(this.displayName, this.conceptCode);
        root.setAlreadyLoaded(this.alreadyLoaded);
        if (parent != null) {
            parent.add(root);
        }
        for (OntologyBuilder child : this.children) {
            child.build(root);
        }
    }
    
}
