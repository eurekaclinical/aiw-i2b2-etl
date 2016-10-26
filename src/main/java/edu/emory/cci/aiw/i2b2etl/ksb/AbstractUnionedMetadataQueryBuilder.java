package edu.emory.cci.aiw.i2b2etl.ksb;

/*-
 * #%L
 * AIW i2b2 ETL
 * %%
 * Copyright (C) 2012 - 2016 Emory University
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

import org.protempa.ProtempaUtil;

/**
 *
 * @author Andrew Post
 */
public abstract class AbstractUnionedMetadataQueryBuilder {
    private static final String[] EMPTY_STRING_ARRAY = new String[0];
    
    private String[] ontTables;
    
    protected AbstractUnionedMetadataQueryBuilder() {
        this.ontTables = EMPTY_STRING_ARRAY;
    }
    
    public AbstractUnionedMetadataQueryBuilder ontTables(String... ontTables) {
        this.ontTables = ontTables;
        return this;
    }

    public String build() {
        ProtempaUtil.checkArray(ontTables, "ontTables");
        StringBuilder sql = new StringBuilder();
        if (this.ontTables.length > 1) {
            sql.append('(');
        }
        for (int i = 0, n = this.ontTables.length; i < n; i++) {
            String table = this.ontTables[i];
            if (i > 0) {
                sql.append(") UNION ALL (");
            }
            appendStatement(sql, table);
        }
        if (this.ontTables.length > 1) {
            sql.append(')');
        }

        return sql.toString();
    }
    
    protected abstract void appendStatement(StringBuilder sql, String table);
}
