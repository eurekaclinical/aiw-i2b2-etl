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
package edu.emory.cci.aiw.i2b2etl.metadata;

import edu.emory.cci.aiw.i2b2etl.table.ProviderDimension;
import java.util.Collection;
import java.util.TreeMap;

/**
 *
 * @author Andrew Post
 */
class ProviderConceptTreeBuilder {

    private final ProviderDimension[] providers;
    private final Metadata metadata;

    ProviderConceptTreeBuilder(Collection<ProviderDimension> providers, Metadata metadata) {
        assert metadata != null : "metadata cannot be null";
        if (providers == null) {
            this.providers = new ProviderDimension[0];
        } else {
            this.providers =
                    providers.toArray(new ProviderDimension[providers.size()]);
        }
        this.metadata = metadata;
    }

    /**
     * Create a hierarchy of providers organized by first letter of their full
     * name.
     * @return the root of the hierarchy.
     * 
     * @throws OntologyBuildException if an error occurs building the 
     * hierarchy.
     */
    Concept build() throws OntologyBuildException {
        try {
            Concept root = this.metadata.getOrCreateHardCodedFolder("Provider");
            TreeMap<Character, Concept> alpha = 
                    createAlphaCategoryConcepts(root);
            createProviderConcepts(alpha, root);
            return root;
        } catch (InvalidConceptCodeException ex) {
            throw new OntologyBuildException("Could not build provider concept tree", ex);
        }
    }

    /**
     * Creates provider concepts and assigns each provider to the correct group.
     * @param alpha map of character to its corresponding category concept.
     * @param root the root concept of the provider hierarchy.
     * @throws InvalidConceptCodeException 
     */
    private void createProviderConcepts(TreeMap<Character, Concept> alpha, Concept root) throws InvalidConceptCodeException {
        for (ProviderDimension pd : this.providers) {
            String fullName = pd.getConcept().getDisplayName();
            Concept parent = alpha.get(fullName.toUpperCase().charAt(0));
            if (parent == null) {
                parent = this.metadata.getOrCreateHardCodedFolder("Other");
                root.add(parent);
            }
            
            Concept child = pd.getConcept();
            parent.add(child);
            
            this.metadata.addToIdCache(child);
        }
    }

    private TreeMap<Character, Concept> createAlphaCategoryConcepts(Concept root) throws InvalidConceptCodeException {
        String ca = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        TreeMap<Character, Concept> alpha = new TreeMap<>();
        for (char c : ca.toCharArray()) {
            Concept ontologyNode = this.metadata.getOrCreateHardCodedFolder(String.valueOf(c));
            alpha.put(c, ontologyNode);
            root.add(ontologyNode);
        }
        return alpha;
    }
}
