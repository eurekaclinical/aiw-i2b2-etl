/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.emory.cci.aiw.i2b2etl.table;

/**
 * Represents the possible values of the <code>VALTYPE_CD</code> attribute of
 * the <code>OBSERVATION_FACT</code> table.
 * 
 * @author Andrew Post
 */
enum ValTypeCode {
    NO_VALUE("@"),
    NUMERIC("N"),
    TEXT("T"),
    RAW_TEXT("B"),
    NLP("NLP");
    
    private final String code;
    
    private ValTypeCode(String code) {
        this.code = code;
    }
    
    /**
     * Gets the code to put into the <code>VALTYPE_CD</code> attribute.
     * 
     * @return a code {@link String}.
     */
    String getCode() {
        return this.code;
    }
}
