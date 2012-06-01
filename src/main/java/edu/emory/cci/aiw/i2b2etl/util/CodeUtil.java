package edu.emory.cci.aiw.i2b2etl.util;

/**
 *
 * @author Andrew Post
 */
public class CodeUtil {
    public static final int CODE_LENGTH = 50;
    
    public static String truncateCodeStringIfNeeded(String codeString) {
        if (codeString != null && codeString.length() > CODE_LENGTH) {
            return codeString.substring(0, CODE_LENGTH);
        } else {
            return codeString;
        }
    }
    
}
