package edu.emory.cci.aiw.i2b2etl.metadata;

/**
 *
 * @author Andrew Post
 */
class UnknownPropositionDefinitionException extends Exception {

    UnknownPropositionDefinitionException(Throwable thrwbl) {
        super(thrwbl);
    }

    UnknownPropositionDefinitionException(String propId, Throwable thrwbl) {
        super("The proposition definition '" + propId + "' is unknown", thrwbl);
    }

    UnknownPropositionDefinitionException(String propId) {
        super("The proposition definition '" + propId + "' is unknown");
    }

    UnknownPropositionDefinitionException() {
    }
    
}
