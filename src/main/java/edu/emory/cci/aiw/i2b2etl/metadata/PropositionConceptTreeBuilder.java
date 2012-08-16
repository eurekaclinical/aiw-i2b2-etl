package edu.emory.cci.aiw.i2b2etl.metadata;

import org.protempa.AbstractionDefinition;
import org.protempa.KnowledgeSource;
import org.protempa.KnowledgeSourceReadException;
import org.protempa.ParameterDefinition;
import org.protempa.PropositionDefinition;
import org.protempa.proposition.value.ValueType;

final class PropositionConceptTreeBuilder {

    private final KnowledgeSource knowledgeSource;
    private final PropositionDefinition rootPropositionDefinition;
    private final String conceptCode;
    private final Metadata metadata;
    private final ValueTypeCode valueTypeCode;

    PropositionConceptTreeBuilder(KnowledgeSource knowledgeSource,
            String propId, String conceptCode, ValueTypeCode valueTypeCode,
            Metadata metadata)
            throws KnowledgeSourceReadException,
            UnknownPropositionDefinitionException {
        assert knowledgeSource != null : "knowledgeSource cannot be null";
        assert propId != null : "propId cannot be null";
        assert metadata != null : "metadata cannot be null";

        this.knowledgeSource = knowledgeSource;
        this.rootPropositionDefinition = readPropositionDefinition(propId);
        this.conceptCode = conceptCode;
        this.metadata = metadata;
        this.valueTypeCode = valueTypeCode;
    }

    Concept build() throws OntologyBuildException {
        try {
            Concept rootConcept =
                    addNode(this.rootPropositionDefinition, null);
            if (!rootConcept.isDerived()) {
                String[] children = this.rootPropositionDefinition.getChildren();
                buildHelper(children, rootConcept);
            }
            return rootConcept;
        } catch (UnknownPropositionDefinitionException ex) {
            throw new OntologyBuildException(
                    "Could not build proposition concept tree", ex);
        } catch (KnowledgeSourceReadException ex) {
            throw new OntologyBuildException(
                    "Could not build proposition concept tree", ex);
        } catch (InvalidConceptCodeException ex) {
            throw new OntologyBuildException(
                    "Could not build proposition concept tree", ex);
        }
    }

    private void buildHelper(String[] propIds, Concept concept)
            throws UnknownPropositionDefinitionException,
            KnowledgeSourceReadException, InvalidConceptCodeException {
        for (String propId : propIds) {
            PropositionDefinition propDef = readPropositionDefinition(propId);
            Concept childConcept = addNode(propDef, concept);
            if (!childConcept.isDerived()) {
                String[] children = propDef.getChildren();
                buildHelper(children, childConcept);
            }      
        }
    }

    private Concept addNode(PropositionDefinition propDef, Concept parent)
            throws InvalidConceptCodeException {
        ConceptId conceptId =
                ConceptId.getInstance(propDef.getId(), this.metadata);
        Concept concept =
                new Concept(conceptId, this.conceptCode, this.metadata);
        concept.setInDataSource(propDef.getInDataSource());
        concept.setDisplayName(propDef.getDisplayName());
        concept.setSourceSystemCode(
                MetadataUtil.toSourceSystemCode(
                propDef.getSourceId().getStringRepresentation()));
        concept.setValueTypeCode(this.valueTypeCode);
        if (propDef instanceof AbstractionDefinition) {
            concept.setDerived(true);
        }
        if (propDef instanceof ParameterDefinition) {
            ValueType valueType =
                    ((ParameterDefinition) propDef).getValueType();
            concept.setDataType(DataType.dataTypeFor(valueType));
        } else {
            concept.setDataType(DataType.TEXT);
        }
        if (concept.isLeaf() || concept.isInDataSource() || concept.isDerived()) {
            this.metadata.addToIdCache(concept);
        }
        if (parent != null) {
            parent.add(concept);
        }
        return concept;
    }

    private PropositionDefinition readPropositionDefinition(String propId)
            throws UnknownPropositionDefinitionException,
            KnowledgeSourceReadException {
        PropositionDefinition result =
                this.knowledgeSource.readPropositionDefinition(propId);
        if (result != null) {
            return result;
        } else {
            throw new UnknownPropositionDefinitionException(propId);
        }
    }
}
