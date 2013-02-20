/*
 * #%L
 * AIW i2b2 ETL
 * %%
 * Copyright (C) 2012 - 2013 Emory University
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
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
