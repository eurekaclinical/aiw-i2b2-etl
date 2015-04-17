/*
 * #%L
 * AIW i2b2 ETL
 * %%
 * Copyright (C) 2012 - 2013 Emory University
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
package edu.emory.cci.aiw.i2b2etl.dest.metadata;

import edu.emory.cci.aiw.i2b2etl.dest.metadata.conceptid.InvalidConceptCodeException;
import edu.emory.cci.aiw.i2b2etl.dest.table.ProviderDimension;
import java.util.TreeMap;

/**
 *
 * @author Andrew Post
 */
class ProviderConceptTreeBuilder implements OntologyBuilder, SubtreeBuilder {

    private final Metadata metadata;
    private Concept root;
    private TreeMap<Character, Concept> alpha;
    private final boolean skipProviderHierarchy;

    ProviderConceptTreeBuilder(Metadata metadata) {
        assert metadata != null : "metadata cannot be null";
        this.metadata = metadata;
        this.skipProviderHierarchy = this.metadata.getSettings().getSkipProviderHierarchy();
    }

    /**
     * Create a hierarchy of providers organized by first letter of their full
     * name.
     *
     * @return the root of the hierarchy.
     *
     * @throws OntologyBuildException if an error occurs building the hierarchy.
     */
    @Override
    public void build(Concept parent) throws OntologyBuildException {
        if (!this.skipProviderHierarchy) {
            try {
                root = this.metadata.getOrCreateHardCodedFolder("Provider");
                root.setFactTableColumn("provider_id");
                root.setTableName("provider_dimension");
                root.setColumnName("provider_path");
                if (parent != null) {
                    root.setAlreadyLoaded(parent.isAlreadyLoaded());
                    parent.add(root);
                }
                alpha = createAlphaCategoryConcepts();
            } catch (InvalidConceptCodeException ex) {
                throw new OntologyBuildException("Could not build provider concept tree", ex);
            }
        }
    }

    void add(ProviderDimension pd) throws InvalidConceptCodeException {
        if (!this.skipProviderHierarchy) {
            String fullName = pd.getConcept().getDisplayName();
            Concept parent = alpha.get(fullName.toUpperCase().charAt(0));
            if (parent == null) {
                parent = this.metadata.getOrCreateHardCodedFolder("Provider", "Other");
                parent.setFactTableColumn("provider_id");
                parent.setTableName("provider_dimension");
                parent.setColumnName("provider_path");
                parent.setAlreadyLoaded(root.isAlreadyLoaded());
                root.add(parent);
            }
            Concept child = pd.getConcept();
            child.setAlreadyLoaded(child.isAlreadyLoaded());
            parent.add(child);
        }
    }

    @Override
    public Concept[] getRoots() {
        if (this.root != null) {
            return new Concept[]{this.root};
        } else {
            return EMPTY_CONCEPT_ARRAY;
        }
    }

    private TreeMap<Character, Concept> createAlphaCategoryConcepts() throws InvalidConceptCodeException {
        String ca = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        TreeMap<Character, Concept> alpha = new TreeMap<>();
        for (char c : ca.toCharArray()) {
            Concept ontologyNode = this.metadata.getOrCreateHardCodedFolder("Provider", String.valueOf(c));
            ontologyNode.setFactTableColumn("provider_id");
            ontologyNode.setTableName("provider_dimension");
            ontologyNode.setColumnName("provider_path");
            ontologyNode.setAlreadyLoaded(root.isAlreadyLoaded());
            alpha.put(c, ontologyNode);
            root.add(ontologyNode);
        }
        return alpha;
    }
}
