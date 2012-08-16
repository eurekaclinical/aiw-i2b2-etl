package edu.emory.cci.aiw.i2b2etl.metadata;

/**
 *
 * @author Andrew Post
 */
public enum ConceptOperator {
    LIKE("LIKE"),
    EQUAL("=");
    
    private final String operator;
    
    private ConceptOperator(String operator) {
        assert operator != null : "operator cannot be null";
        this.operator = operator;
    }
    
    public String getSQLOperator() {
        return this.operator;
    }
}
