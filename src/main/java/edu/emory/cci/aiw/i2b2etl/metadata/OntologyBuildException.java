package edu.emory.cci.aiw.i2b2etl.metadata;

/**
 *
 * @author Andrew Post
 */
public class OntologyBuildException extends Exception {

    public OntologyBuildException(Throwable thrwbl) {
        super(thrwbl);
    }

    public OntologyBuildException(String string, Throwable thrwbl) {
        super(string, thrwbl);
    }

    public OntologyBuildException(String string) {
        super(string);
    }

    public OntologyBuildException() {
    }
    
}
