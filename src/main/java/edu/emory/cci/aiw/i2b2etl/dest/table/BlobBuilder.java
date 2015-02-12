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
package edu.emory.cci.aiw.i2b2etl.dest.table;

import org.apache.commons.lang3.StringUtils;
import org.arp.javautil.string.StringUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * Abstract class for constructing i2b2 blobs.
 * 
 * @author Andrew Post
 */
abstract class BlobBuilder {
    public static final char DEFAULT_DELIMITER='|';
    
    private static final Map<String, String> REPLACE =
            new HashMap<>();
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
     * @return a {@link String}<code>[]</code>.
     */
    protected abstract String[] getFields();
    
    final String build() {
        String[] fields = getFields();
        StringUtil.escapeDelimitedColumnsInPlace(fields, REPLACE, 
                this.delimiter);
        return StringUtils.join(fields, this.delimiter);
    }
    
}
