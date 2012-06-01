package edu.emory.cci.aiw.i2b2etl.metadata;

import edu.emory.cci.aiw.i2b2etl.table.ProviderDimension;
import java.util.Collection;
import java.util.TreeMap;
import org.protempa.KnowledgeSource;

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
            Concept root = new Concept(ConceptId.getInstance(MetadataUtil.DEFAULT_CONCEPT_ID_PREFIX_INTERNAL + "|Provider", this.metadata), null, this.metadata);
            root.setSourceSystemCode(MetadataUtil.toSourceSystemCode(I2B2QueryResultsHandlerSourceId.getInstance().getStringRepresentation()));
            root.setDisplayName("Providers");
            String ca = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
            TreeMap<Character, Concept> alpha =
                    new TreeMap<Character, Concept>();
            for (char c : ca.toCharArray()) {
                Concept ontologyNode = new Concept(ConceptId.getInstance(MetadataUtil.DEFAULT_CONCEPT_ID_PREFIX_INTERNAL + "|Provider|" + String.valueOf(c), this.metadata), null, this.metadata);
                ontologyNode.setDisplayName(String.valueOf(c));
                alpha.put(c, ontologyNode);
                root.add(ontologyNode);
            }
            for (ProviderDimension pd : this.providers) {
                String id = pd.getId();
                if (id != null) {
                    String fullName = pd.getFullName();
                    Concept parent = alpha.get(fullName.charAt(0));
                    if (parent == null && root.getChildCount() == ca.length()) {
                        parent = new Concept(ConceptId.getInstance(MetadataUtil.DEFAULT_CONCEPT_ID_PREFIX_INTERNAL + "|Provider|Other", this.metadata), null, this.metadata);
                        parent.setDisplayName("Other");
                        root.add(parent);
                    }
                    Concept child = new Concept(ConceptId.getInstance(id, this.metadata), id, this.metadata);
                    parent.add(child);
                    child.setSourceSystemCode(MetadataUtil.toSourceSystemCode(I2B2QueryResultsHandlerSourceId.getInstance().getStringRepresentation()));
                    child.setDisplayName(pd.getFullName());
                    child.setInUse(true);
                    this.metadata.addToIdCache(child);
                    pd.setI2b2Path(child.getI2B2Path());
                } else {
                    Concept notRecordedConcept = new Concept(ConceptId.getInstance(MetadataUtil.DEFAULT_CONCEPT_ID_PREFIX_INTERNAL + "Provider|NotRecorded", this.metadata), null, this.metadata);
                    notRecordedConcept.setDisplayName(notRecordedConcept.getDisplayName());
                    notRecordedConcept.setSourceSystemCode(MetadataUtil.toSourceSystemCode(I2B2QueryResultsHandlerSourceId.getInstance().getStringRepresentation()));
                    root.add(notRecordedConcept);
                    pd.setI2b2Path(notRecordedConcept.getI2B2Path());
                }
            }
            return root;
        } catch (InvalidConceptCodeException ex) {
            throw new OntologyBuildException("Could not build provider concept tree", ex);
        }
    }
}
