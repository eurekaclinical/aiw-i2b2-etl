package edu.emory.cci.aiw.i2b2etl.metadata;

import edu.emory.cci.aiw.i2b2etl.util.CodeUtil;
import org.protempa.proposition.value.Value;

/**
 *
 * @author Andrew Post
 */
public final class OverriddenConceptCodeBuilder extends ConceptCodeBuilder {
    private String id;
    private Value value;
    
    OverriddenConceptCodeBuilder(Metadata metadata) {
        super(metadata);
    }

    String getId() {
        return id;
    }

    void setId(String id) {
        this.id = id;
    }

    Value getValue() {
        return value;
    }

    void setValue(Value value) {
        this.value = value;
    }

    @Override
    String build() throws InvalidConceptCodeException {
        StringBuilder conceptCodeBuilder = new StringBuilder();
        conceptCodeBuilder.append(this.id);
        if (this.value != null) {
            conceptCodeBuilder.append(':');
            conceptCodeBuilder.append(this.value.getFormatted());
        }
        String conceptCode = conceptCodeBuilder.toString();
        
        if (getMetadata().isInConceptCodeCache(conceptCode)) {
            throw new InvalidConceptCodeException("Concept code " + conceptCode + " is already in use");
        }
        if (conceptCode.length() > CodeUtil.CODE_LENGTH) {
            throw new InvalidConceptCodeException("Concept code " + conceptCode + " is too long (max length is " + CodeUtil.CODE_LENGTH + " characters)");
        }
        getMetadata().addToConceptCodeCache(conceptCode);
        return conceptCode;
    }
    
}
