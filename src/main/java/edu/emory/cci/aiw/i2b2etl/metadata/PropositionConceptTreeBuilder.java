package edu.emory.cci.aiw.i2b2etl.metadata;


import org.protempa.KnowledgeSource;
import org.protempa.KnowledgeSourceReadException;
import org.protempa.PropositionDefinition;

final class PropositionConceptTreeBuilder {

    private final KnowledgeSource knowledgeSource;
    private final PropositionDefinition rootPropositionDefinition;
    private final String conceptCode;
    private final Metadata metadata;

    PropositionConceptTreeBuilder(KnowledgeSource knowledgeSource, String propId, String conceptCode, Metadata metadata) throws KnowledgeSourceReadException, UnknownPropositionDefinitionException {
        assert knowledgeSource != null : "knowledgeSource cannot be null";
        assert propId != null : "propId cannot be null";
        assert metadata != null : "metadata cannot be null";

        this.knowledgeSource = knowledgeSource;
        this.rootPropositionDefinition = readPropositionDefinition(propId);
        this.conceptCode = conceptCode;
        this.metadata = metadata;
    }

    Concept build() throws OntologyBuildException {
        try {
            Concept rootConcept = addNode(this.rootPropositionDefinition, null);
            String[] childrenPropIds =
                    this.rootPropositionDefinition.getDirectChildren();
            buildHelper(childrenPropIds, rootConcept);
            return rootConcept;
        } catch (UnknownPropositionDefinitionException ex) {
            throw new OntologyBuildException("Could not build proposition concept tree", ex);
        } catch (KnowledgeSourceReadException ex) {
            throw new OntologyBuildException("Could not build proposition concept tree", ex);
        } catch (InvalidConceptCodeException ex) {
            throw new OntologyBuildException("Could not build proposition concept tree", ex);
        }
    }

    private void buildHelper(String[] propIds, Concept concept) throws UnknownPropositionDefinitionException, KnowledgeSourceReadException, InvalidConceptCodeException {
        for (String propId : propIds) {
            PropositionDefinition propDef = readPropositionDefinition(propId);
            Concept c = addNode(propDef, concept);
            String[] directChildren = propDef.getDirectChildren();
            buildHelper(directChildren, c);
        }
    }

    private Concept addNode(PropositionDefinition propDef, Concept parent) throws InvalidConceptCodeException {
        Concept child = new Concept(ConceptId.getInstance(propDef.getId(), this.metadata), this.conceptCode, this.metadata);
        child.setInDatasource(propDef.getInDataSource());
        child.setDisplayName(propDef.getDisplayName());
        child.setSourceSystemCode(MetadataUtil.toSourceSystemCode(propDef.getSourceId().getStringRepresentation()));
        if (child.isLeaf() || child.isInDatasource()) {
            this.metadata.addToIdCache(child);
        }
        if (parent != null) {
            parent.add(child);
        }
        return child;
    }

    private PropositionDefinition readPropositionDefinition(String propId) throws UnknownPropositionDefinitionException, KnowledgeSourceReadException {
        PropositionDefinition result =
                this.knowledgeSource.readPropositionDefinition(propId);
        if (result != null) {
            return result;
        } else {
            throw new UnknownPropositionDefinitionException(propId);
        }
    }
}
