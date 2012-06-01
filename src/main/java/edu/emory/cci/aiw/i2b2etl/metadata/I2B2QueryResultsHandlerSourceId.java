package edu.emory.cci.aiw.i2b2etl.metadata;

import org.protempa.SourceId;

/**
 *
 * @author Andrew Post
 */
public class I2B2QueryResultsHandlerSourceId implements SourceId {
    private static final I2B2QueryResultsHandlerSourceId SINGLETON =
            new I2B2QueryResultsHandlerSourceId();
    
    public static I2B2QueryResultsHandlerSourceId getInstance() {
        return SINGLETON;
    }
    
    private I2B2QueryResultsHandlerSourceId() {
        
    }

    @Override
    public String getStringRepresentation() {
        return "AIW I2B2 Loader";
    }
    
}
