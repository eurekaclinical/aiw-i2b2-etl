package edu.emory.cci.aiw.i2b2etl.metadata;

import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author Andrew Post
 */
abstract class ConceptCodeBuilder {
    private final Metadata metadata;
    
    ConceptCodeBuilder(Metadata metadata) {
        assert metadata != null : "metadata cannot be null";
        this.metadata = metadata;
        
    }
    
    protected Metadata getMetadata() {
        return this.metadata;
    }
    
    abstract String build() throws InvalidConceptCodeException;
    
}
