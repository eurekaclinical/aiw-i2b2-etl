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

	Concept build() throws OntologyBuildException {
		try {
			ConceptId conceptId = ConceptId.getInstance(MetadataUtil.DEFAULT_CONCEPT_ID_PREFIX_INTERNAL + "|Provider", this.metadata);
			Concept root = this.metadata.getFromIdCache(conceptId);
			if (root == null) {
				root = new Concept(conceptId, null, this.metadata);
				root.setSourceSystemCode(MetadataUtil.toSourceSystemCode(I2B2QueryResultsHandlerSourceId.getInstance().getStringRepresentation()));
				root.setDisplayName("Providers");
				root.setDataType(DataType.TEXT);
				this.metadata.addToIdCache(root);
			}
			String ca = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
			TreeMap<Character, Concept> alpha =
					new TreeMap<Character, Concept>();
			for (char c : ca.toCharArray()) {
				ConceptId cid = ConceptId.getInstance(MetadataUtil.DEFAULT_CONCEPT_ID_PREFIX_INTERNAL + "|Provider|" + String.valueOf(c), this.metadata);
				Concept ontologyNode = this.metadata.getFromIdCache(cid);
				if (ontologyNode == null) {
					ontologyNode = new Concept(cid, null, this.metadata);
					ontologyNode.setDisplayName(String.valueOf(c));
					ontologyNode.setDataType(DataType.TEXT);
					ontologyNode.setSourceSystemCode(I2B2QueryResultsHandlerSourceId.getInstance().getStringRepresentation());
					this.metadata.addToIdCache(ontologyNode);
					alpha.put(c, ontologyNode);
				}
				root.add(ontologyNode);
			}
			for (ProviderDimension pd : this.providers) {
				String fullName = pd.getConcept().getDisplayName();
				Concept parent = alpha.get(fullName.toUpperCase().charAt(0));
				if (parent == null) {
					ConceptId cid = ConceptId.getInstance(MetadataUtil.DEFAULT_CONCEPT_ID_PREFIX_INTERNAL + "|Provider|Other", this.metadata);
					parent = this.metadata.getFromIdCache(cid);
					if (parent == null) {
						parent = new Concept(cid, null, this.metadata);
						parent.setDisplayName("Other");
						parent.setDataType(DataType.TEXT);
						parent.setSourceSystemCode(MetadataUtil.toSourceSystemCode(I2B2QueryResultsHandlerSourceId.getInstance().getStringRepresentation()));
						this.metadata.addToIdCache(parent);
						root.add(parent);
					}
				}
				assert parent != null : "Failed to get Other provider category for provider '" + fullName + "'";
				//ConceptId cid = ConceptId.getInstance(id, this.metadata);
				//Concept child = this.metadata.getFromIdCache(cid);
				//if (child == null) {
				Concept child = pd.getConcept();
				//child = new Concept(cid, id, this.metadata);
				parent.add(child);

				this.metadata.addToIdCache(child);
				//} else {
				//   throw new OntologyBuildException("Duplicate provider concept: " + child.getConceptCode());
				//}
//                } else {
//                    Concept notRecordedConcept = new Concept(ConceptId.getInstance(MetadataUtil.DEFAULT_CONCEPT_ID_PREFIX_INTERNAL + "Provider|NotRecorded", this.metadata), null, this.metadata);
//                    notRecordedConcept.setDisplayName(notRecordedConcept.getDisplayName());
//                    notRecordedConcept.setSourceSystemCode(MetadataUtil.toSourceSystemCode(I2B2QueryResultsHandlerSourceId.getInstance().getStringRepresentation()));
//                    notRecordedConcept.setDataType(DataType.TEXT);
//                    this.metadata.addToIdCache(notRecordedConcept);
//                    root.add(notRecordedConcept);
//                    pd.setI2b2Path(notRecordedConcept.getI2B2Path());
//                }
			}
			return root;
		} catch (InvalidConceptCodeException ex) {
			throw new OntologyBuildException("Could not build provider concept tree", ex);
		}
	}
}
