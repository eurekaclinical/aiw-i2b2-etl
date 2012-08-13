package edu.emory.cci.aiw.i2b2etl.table;

/**
 *
 * @author Andrew Post
 */
abstract class BuiltInNumFactory implements NumFactory {
    
    BuiltInNumFactory() {
        
    }
    
    public String getSourceSystem() {
        return "aiw-i2b2-etl";
    }
}
