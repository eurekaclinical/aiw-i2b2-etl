package edu.emory.cci.aiw.i2b2etl;

import java.io.File;
import org.protempa.query.handler.QueryResultsHandlerFactory;
import org.protempa.query.handler.QueryResultsHandlerInitException;

/**
 *
 * @author Andrew Post
 */
public class I2B2QueryResultsHandlerFactory implements QueryResultsHandlerFactory {
    private File confXML;
    private boolean inferPropositionIdsNeeded;
    
    /**
     * Creates a new query results handler that will use the provided
     * configuration file. It is the same as calling the two-argument
     * constructor with <code>inferPropositionIdsNeeded</code> set to
     * <code>true</code>.
     *
     * @param confXML an i2b2 query results handler configuration file. Cannot
     * be <code>null</code>.
     */
    public I2B2QueryResultsHandlerFactory(File confXML) {
        this(confXML, true);
    }
    
    /**
     * Creates a new query results handler that will use the provided
     * configuration file. This constructor, through the
     * <code>inferPropositionIdsNeeded</code> parameter, lets you control
     * whether proposition ids to be returned from the Protempa processing run
     * should be inferred from the i2b2 configuration file.
     *
     * @param confXML an i2b2 query results handler configuration file. Cannot
     * be <code>null</code>.
     * @param inferPropositionIdsNeeded <code>true</code> if proposition ids to
     * be returned from the Protempa processing run should include all of those
     * specified in the i2b2 configuration file, <code>false</code> if the
     * proposition ids returned should be only those specified in the Protempa
     * {@link Query}.
     */
    public I2B2QueryResultsHandlerFactory(File confXML,
            boolean inferPropositionIdsNeeded) { 
        if (confXML == null) {
            throw new IllegalArgumentException("confXML cannot be null");
        }
        this.confXML = confXML;
        this.inferPropositionIdsNeeded = inferPropositionIdsNeeded;
    }

    @Override
    public I2B2QueryResultsHandler getInstance() throws QueryResultsHandlerInitException {
       return new I2B2QueryResultsHandler(this.confXML, this.inferPropositionIdsNeeded);
    }
    
}
