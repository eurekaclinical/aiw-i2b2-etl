package edu.emory.cci.aiw.i2b2etl.metadata;

/**
 *
 * @author Andrew Post
 */
public class InvalidConceptCodeException extends Exception {

    InvalidConceptCodeException(Throwable thrwbl) {
        super(thrwbl);
    }

    InvalidConceptCodeException(String string, Throwable thrwbl) {
        super(string, thrwbl);
    }

    InvalidConceptCodeException(String string) {
        super(string);
    }

    InvalidConceptCodeException() {
    }
    
}
