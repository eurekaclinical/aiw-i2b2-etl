package edu.emory.cci.aiw.i2b2etl.metadata;

import org.protempa.proposition.value.ValueType;

/**
 *
 * @author Andrew Post
 */
public enum DataType {
    TEXT("T"),
    NUMERIC("N");
    
    private final String code;
    
    public static DataType dataTypeFor(ValueType valueType) {
        if (ValueType.NUMERICALVALUE == valueType ||
                    ValueType.NUMBERVALUE == valueType ||
                    ValueType.INEQUALITYNUMBERVALUE == valueType) {
                return DataType.NUMERIC;
            } else {
                return DataType.TEXT;
            }
    }
    
    private DataType(String code) {
        assert code != null : "code cannot be null";
        this.code = code;
    }
    
    public String getCode() {
        return this.code;
    }
}
