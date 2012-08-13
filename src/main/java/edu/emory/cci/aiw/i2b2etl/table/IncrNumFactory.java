package edu.emory.cci.aiw.i2b2etl.table;

/**
 *
 * @author Andrew Post
 */
public class IncrNumFactory extends BuiltInNumFactory {
    private long num;
    
    public IncrNumFactory() {
        this.num = 0L;
    }

    public long getInstance() {
        return num++;
    }

    
    
}
