package edu.emory.cci.aiw.i2b2etl.metadata;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.protempa.KnowledgeSource;
import org.protempa.PropertyDefinition;
import org.protempa.PropositionDefinition;
import org.protempa.ValueSet;
import org.protempa.ValueSet.ValueSetElement;

import org.protempa.*;

class ValueSetConceptTreeBuilder {

    private KnowledgeSource knowledgeSource;
    private String propertyName;
    private String conceptCodePrefix;
    private final String propId;
    private final PropositionDefinition propDefinition;
    private final Metadata metadata;

    ValueSetConceptTreeBuilder(KnowledgeSource knowledgeSource, String propId, String property,
            String conceptCodePrefix, Metadata metadata) throws KnowledgeSourceReadException, UnknownPropositionDefinitionException {
        assert knowledgeSource != null : "knowledgeSource cannot be null";
        assert propId != null : "propId cannot be null";
        assert metadata != null : "metadata cannot be null";
        this.knowledgeSource = knowledgeSource;
        this.propertyName = property;
        this.conceptCodePrefix = conceptCodePrefix;
        this.propId = propId;
        this.propDefinition =
                knowledgeSource.readPropositionDefinition(this.propId);
        if (this.propDefinition == null) {
            throw new UnknownPropositionDefinitionException(this.propId);
        }
        this.metadata = metadata;
    }
    
    Concept build() throws OntologyBuildException {
        try {
            Concept root = new Concept(ConceptId.getInstance(this.propId, this.propertyName, this.metadata), this.conceptCodePrefix, this.metadata);
            root.setSourceSystemCode(MetadataUtil.toSourceSystemCode(I2B2QueryResultsHandlerSourceId.getInstance().getStringRepresentation()));
            root.setDataType(DataType.TEXT);
            PropertyDefinition propertyDef =
                    propDefinition.propertyDefinition(propertyName);
            ValueSet valueSet =
                    knowledgeSource.readValueSet(propertyDef.getValueSetId());
            ValueSetElement[] vse = valueSet.getValueSetElements();
            for (ValueSetElement e : vse) {
                Concept concept = new Concept(ConceptId.getInstance(
                        this.propId, this.propertyName, e.getValue(), this.metadata), this.conceptCodePrefix, this.metadata);
                concept.setSourceSystemCode(MetadataUtil.toSourceSystemCode(valueSet.getSourceId().getStringRepresentation()));
                concept.setInDataSource(true);
                concept.setDisplayName(e.getDisplayName());
                concept.setDataType(DataType.TEXT);
                this.metadata.addToIdCache(concept);
                root.add(concept);
            }
            return root;
        } catch (KnowledgeSourceReadException ex) {
            throw new OntologyBuildException("Could not build value set concept tree", ex);
        } catch (InvalidConceptCodeException ex) {
            throw new OntologyBuildException("Could not build value set concept tree", ex);
        }
    }
}
