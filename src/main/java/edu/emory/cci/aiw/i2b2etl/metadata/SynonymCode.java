package edu.emory.cci.aiw.i2b2etl.metadata;

/**
 *
 * @author Andrew Post
 */
public enum SynonymCode {
    SYNONYM("Y"),
    NOT_SYNONYM("N");
    
    private final String code;
    
    private SynonymCode(String code) {
        assert code != null : "code cannot be null";
        this.code = code;
    }
    
    public String getCode() {
        return this.code;
    }
}
