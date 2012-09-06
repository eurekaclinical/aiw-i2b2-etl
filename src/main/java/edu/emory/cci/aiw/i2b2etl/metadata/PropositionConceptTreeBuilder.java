package edu.emory.cci.aiw.i2b2etl.metadata;

import org.protempa.AbstractionDefinition;
import org.protempa.KnowledgeSource;
import org.protempa.KnowledgeSourceReadException;
import org.protempa.ParameterDefinition;
import org.protempa.PropositionDefinition;
import org.protempa.ProtempaUtil;
import org.protempa.proposition.value.ValueType;

final class PropositionConceptTreeBuilder {

    private final KnowledgeSource knowledgeSource;
    private final PropositionDefinition[] rootPropositionDefinitions;
    private final String conceptCode;
    private final Metadata metadata;
    private final ValueTypeCode valueTypeCode;

    PropositionConceptTreeBuilder(KnowledgeSource knowledgeSource,
            String[] propIds, String conceptCode, ValueTypeCode valueTypeCode,
            Metadata metadata)
            throws KnowledgeSourceReadException,
            UnknownPropositionDefinitionException {
        assert knowledgeSource != null : "knowledgeSource cannot be null";
        ProtempaUtil.checkArray(propIds, "propIds");
        assert metadata != null : "metadata cannot be null";

        this.knowledgeSource = knowledgeSource;
        this.rootPropositionDefinitions =
                new PropositionDefinition[propIds.length];
        for (int i = 0; i < propIds.length; i++) {
            this.rootPropositionDefinitions[i] =
                    readPropositionDefinition(propIds[i]);
        }
        this.conceptCode = conceptCode;
        this.metadata = metadata;
        this.valueTypeCode = valueTypeCode;
    }

    Concept[] build() throws OntologyBuildException {
        try {
            Concept[] result =
                    new Concept[this.rootPropositionDefinitions.length];
            for (int i = 0; i < this.rootPropositionDefinitions.length; i++) {
                PropositionDefinition rootPropositionDefinition =
                        this.rootPropositionDefinitions[i];
                Concept rootConcept =
                        addNode(rootPropositionDefinition);
                if (!rootConcept.isDerived()) {
                    String[] children =
                            rootPropositionDefinition.getChildren();
                    buildHelper(children, rootConcept);
                }
                result[i] = rootConcept;
            }
            return result;
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

    private void buildHelper(String[] childPropIds, Concept parent)
            throws UnknownPropositionDefinitionException,
            KnowledgeSourceReadException, InvalidConceptCodeException {
        for (String childPropId : childPropIds) {
            PropositionDefinition childPropDef =
                    readPropositionDefinition(childPropId);
            Concept child = addNode(childPropDef);
            if (parent != null) {
                parent.add(child);
            }
            if (child.isCopy()) {
                parent.setDerived(true);
                parent.removeAllChildren();
                break;
            }
            if (!child.isDerived()) {
                String[] grandChildrenPropIds = childPropDef.getChildren();
                buildHelper(grandChildrenPropIds, child);
            }
        }
    }

    private Concept addNode(PropositionDefinition propDef)
            throws InvalidConceptCodeException {
        ConceptId conceptId =
                ConceptId.getInstance(propDef.getId(), this.metadata);
        Concept child = this.metadata.getFromIdCache(conceptId);
        if (child == null) {
            child =
                    new Concept(conceptId, this.conceptCode, this.metadata);
            child.setInDataSource(propDef.getInDataSource());
            child.setDisplayName(propDef.getDisplayName());
            child.setSourceSystemCode(
                    MetadataUtil.toSourceSystemCode(
                    propDef.getSourceId().getStringRepresentation()));
            child.setValueTypeCode(this.valueTypeCode);
            if (propDef instanceof AbstractionDefinition) {
                child.setDerived(true);
            }
            if (propDef instanceof ParameterDefinition) {
                ValueType valueType =
                        ((ParameterDefinition) propDef).getValueType();
                child.setDataType(DataType.dataTypeFor(valueType));
            } else {
                child.setDataType(DataType.TEXT);
            }
            this.metadata.addToIdCache(child);
        } else {
            Concept childCopy = new Concept(child, this.metadata);
            childCopy.setDimCode(child.getDimCode());
            child = childCopy;
        }

        return child;
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
