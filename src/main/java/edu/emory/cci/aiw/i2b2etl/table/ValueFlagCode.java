package edu.emory.cci.aiw.i2b2etl.table;

/**
 * Represents the possible values of the <code>VALUEFLAG_CD</code> attribute of
 * the <code>OBSERVATION_FACT</code> table.
 * 
 * @author Andrew Post
 */
enum ValueFlagCode {
    NO_VALUE_FLAG("@"),
    ABNORMAL("A"),
    HIGH("H"),
    LOW("L"),
    BLOB_FIELD_IS_ENCRYPTED("X");
    
    private final String code;
    
    private ValueFlagCode(String code) {
        this.code = code;
    }

    /**
     * Gets the code to put into the <code>VALUEFLAG_CD</code> attribute.
     * 
     * @return a code {@link String}.
     */
    String getCode() {
        return this.code;
    }
}
