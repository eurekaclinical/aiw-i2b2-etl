package edu.emory.cci.aiw.i2b2etl.metadata;

/**
 *
 * @author Andrew Post
 */
public class InvalidPromoteArgumentException extends Exception {

    InvalidPromoteArgumentException(Concept concept) {
        super("Invalid promote argument for '" + concept.getDisplayName() + "'");
    }
    
    
    
}
