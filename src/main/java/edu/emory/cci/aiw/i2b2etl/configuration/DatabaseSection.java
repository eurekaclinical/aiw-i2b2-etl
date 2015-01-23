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
package edu.emory.cci.aiw.i2b2etl.configuration;

import java.util.TreeMap;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.w3c.dom.*;

/**
 *
 * @author Andrew Post
 */
public final class DatabaseSection extends ConfigurationSection implements Database {

    DatabaseSection() {
    }

    private TreeMap<String, DatabaseSpec> dbs = new TreeMap<>();
    
    public DatabaseSpec get(String schema) {
        return this.dbs.get(schema);
    }

    @Override
    protected void put(NamedNodeMap nnm) throws ConfigurationReadException {
        DatabaseSpec databaseSpec = new DatabaseSpec(
                readAttribute(nnm, "key", true), 
                readAttribute(nnm, "user", true), 
                readAttribute(nnm, "passwd", true), 
                readAttribute(nnm, "connect", true));
        this.dbs.put(databaseSpec.getKey(), databaseSpec);
    }

    @Override
    protected String getNodeName() {
        return "dbschema";
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    @Override
    public DatabaseSpec getMetadataSpec() {
        return this.dbs.get("metaschema");
    }

    @Override
    public DatabaseSpec getDataSpec() {
        return this.dbs.get("dataschema");
    }
    
}
