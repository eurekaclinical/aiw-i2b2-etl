/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.emory.cci.aiw.i2b2etl.table;

import edu.emory.cci.aiw.i2b2etl.metadata.MetadataUtil;
import java.util.Date;
import java.util.logging.Logger;

/**
 *
 * @author Andrew Post
 */
public final class TableUtil {
    private TableUtil() {}
    
    private static class LazyLoggerHolder {

        private static Logger instance =
                Logger.getLogger(MetadataUtil.class.getPackage().getName());
    }

    /**
     * Gets the logger for this package.
     *
     * @return a {@link Logger} object.
     */
    static Logger logger() {
        return TableUtil.LazyLoggerHolder.instance;
    }
    
    public static String setStringAttribute(String attribute, int maxLength) {
        if (maxLength < 1) {
            throw new IllegalArgumentException("maxLength cannot be < 1");
        }
        if (attribute == null || attribute.length() == 0) {
            return "@";
        } else if (attribute.length() > maxLength) {
            return attribute.substring(0, maxLength);
        } else {
            return attribute;
        }
    }
    
    public static String setStringAttribute(String attribute) {
        if (attribute == null || attribute.length() == 0) {
            return "@";
        } else {
            return attribute;
        }
    }
    
    public static java.sql.Date setDateAttribute(Date date) {
        if (date == null) {
            return null;
        } else {
            return new java.sql.Date(date.getTime());
        }
    }
    
    public static java.sql.Timestamp setTimestampAttribute(Date date) {
        if (date == null) {
            return null;
        } else {
            return new java.sql.Timestamp(date.getTime());
        }
    }
    
    public static String setNullableStringAttribute(String attribute,
            int maxLength) {
        if (maxLength < 1) {
            throw new IllegalArgumentException("maxLength cannot be < 1");
        }
        if (attribute == null) {
            return null;
        } else if (attribute.length() > maxLength) {
            return attribute.substring(0, maxLength);
        } else {
            return attribute;
        }
    }
}
