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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.TreeMap;
import org.arp.javautil.sql.ConnectionSpec;

/**
 *
 * @author Andrew Post
 */
class ProviderConceptTreeBuilder {

    private final Metadata metadata;
    private Concept root;
    private TreeMap<Character, Concept> alpha;

    ProviderConceptTreeBuilder(Metadata metadata) {
        assert metadata != null : "metadata cannot be null";
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
            root = this.metadata.getOrCreateHardCodedFolder("Provider");
            alpha = 
                    createAlphaCategoryConcepts(root);
            return root;
        } catch (InvalidConceptCodeException ex) {
            throw new OntologyBuildException("Could not build provider concept tree", ex);
        }
    }
    
    void add(ProviderDimension pd) throws InvalidConceptCodeException {
        String fullName = pd.getConcept().getDisplayName();
        Concept parent = alpha.get(fullName.toUpperCase().charAt(0));
        if (parent == null) {
            parent = this.metadata.getOrCreateHardCodedFolder("Provider", "Other");
            root.add(parent);
        }
        Concept child = pd.getConcept();
        parent.add(child);

        this.metadata.addToIdCache(child);
    }

    private TreeMap<Character, Concept> createAlphaCategoryConcepts(Concept root) throws InvalidConceptCodeException {
        String ca = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        TreeMap<Character, Concept> alpha = new TreeMap<>();
        for (char c : ca.toCharArray()) {
            Concept ontologyNode = this.metadata.getOrCreateHardCodedFolder("Provider", String.valueOf(c));
            alpha.put(c, ontologyNode);
            root.add(ontologyNode);
        }
        return alpha;
    }
}
