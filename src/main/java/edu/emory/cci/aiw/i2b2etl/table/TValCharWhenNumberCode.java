/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.emory.cci.aiw.i2b2etl.table;

import org.protempa.proposition.value.ValueComparator;

/**
 * Represents the possible values of the <code>TVAL_CHAR</code> attribute of
 * the <code>FACT_OBSERVATION</code> table when the value of the 
 * <code>VALTYPE_CD</code> attribute is <code>T</code>.
 * 
 * @author Andrew Post
 */
enum TValCharWhenNumberCode {
    NO_VALUE(null),
    EQUAL("E"),
    NOT_EQUAL("NE"),
    LESS_THAN("L"),
    LESS_THAN_OR_EQUAL_TO("LE"),
    GREATER_THAN("G"),
    GREATER_THAN_OR_EQUAL_TO("GE");

    /**
     * Returns the value of the enum corresponding to the Protempa
     * {@link ValueComparator} value.
     * 
     * @param comp a {@link ValueComparator} value. Cannot be 
     * <code>null</code>.
     * @return a {@link TValCharWhenNumberCode}. 
     * Guaranteed not <code>null</code>.
     */
    static TValCharWhenNumberCode codeFor(ValueComparator comp) {
        assert comp != null : "comp cannot be null";
        switch (comp) {
            case EQUAL_TO:
                return TValCharWhenNumberCode.EQUAL;
            case NOT_EQUAL_TO:
                return TValCharWhenNumberCode.NOT_EQUAL;
            case LESS_THAN:
                return TValCharWhenNumberCode.LESS_THAN;
            case LESS_THAN_OR_EQUAL_TO:
                return TValCharWhenNumberCode.LESS_THAN_OR_EQUAL_TO;
            case GREATER_THAN:
                return TValCharWhenNumberCode.GREATER_THAN;
            case GREATER_THAN_OR_EQUAL_TO:
                return TValCharWhenNumberCode.GREATER_THAN_OR_EQUAL_TO;
            default:
                return TValCharWhenNumberCode.NO_VALUE;
        }
    }
    private final String code;

    private TValCharWhenNumberCode(String code) {
        this.code = code;
    }

    /**
     * Gets the code to put into the <code>TVAL_CHAR</code> attribute.
     * 
     * @return a code {@link String}.
     */
    String getCode() {
        return this.code;
    }
}
