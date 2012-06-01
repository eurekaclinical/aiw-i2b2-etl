package edu.emory.cci.aiw.i2b2etl.table;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.arp.javautil.string.StringUtil;

/**
 * Abstract class for constructing i2b2 blobs.
 * 
 * @author Andrew Post
 */
abstract class BlobBuilder {
    public static final char DEFAULT_DELIMITER='|';
    
    private static final Map<String, String> REPLACE =
            new HashMap<String, String>();
    static {
        REPLACE.put(null, "");
    }
    
    private char delimiter;
    
    /**
     * Constructs a blob builder configured to use (<code>|</code>) as a 
     * delimiter.
     */
    protected BlobBuilder() {
        this.delimiter = DEFAULT_DELIMITER;
    }
    
    /**
     * Returns the delimiter.
     * 
     * @return a delimiter <code>char</code>.
     */
    final char getDelimiter() {
        return this.delimiter;
    }
    
    /**
     * Sets the delimiter.
     * 
     * @param delimiter a delimiter <code>char</code>.
     */
    final void setDelimiter(char delimiter) {
        this.delimiter = delimiter;
    }
    
    /**
     * Returns a newly created array of field values.
     * 
     * @return a {@link String[]}.
     */
    protected abstract String[] getFields();
    
    final String build() {
        String[] fields = getFields();
        StringUtil.escapeDelimitedColumnsInPlace(fields, REPLACE, 
                this.delimiter);
        return StringUtils.join(fields, this.delimiter);
    }
    
}
