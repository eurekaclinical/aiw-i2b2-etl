package edu.emory.cci.aiw.i2b2etl.table;

import java.util.Date;

/**
 * Represents the possible values of the <code>VITAL_STATUS_CD</code> attribute 
 * in the <code>PATIENT_DIMENSION</code> table.
 * 
 * @author Andrew Post
 */
enum VitalStatusCode {
    LIVING("N"),
    DECEASED_ACCURATE_TO_DAY("Y"),
    DECEASED_ACCURATE_TO_MONTH("M"),
    DECEASED_ACCURATE_TO_YEAR("X");
    
    private final String code;
    
    static VitalStatusCode getInstance(Date deathDate) {
        if (deathDate != null) {
            return DECEASED_ACCURATE_TO_DAY;
        } else {
            return LIVING;
        }
    }
    
    private VitalStatusCode(String code) {
        this.code = code;
    }
    
    /**
     * Gets the code to put into the <code>VITAL_STATUS_CD</code> attribute.
     * 
     * @return a code {@link String}.
     */
    String getCode() {
        return this.code;
    }
}
