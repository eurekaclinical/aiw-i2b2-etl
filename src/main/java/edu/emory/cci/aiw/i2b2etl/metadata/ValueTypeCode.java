package edu.emory.cci.aiw.i2b2etl.metadata;

/**
 *
 * @author Andrew Post
 */
public enum ValueTypeCode {
    LABORATORY_TESTS("LAB"),
    DOCUMENTS("DOC"),
    UNSPECIFIED(null);
    
    private final String code;
    
    private ValueTypeCode(String code) {
        this.code = code;
    }
    
    public String getCode() {
        return this.code;
    }
}
