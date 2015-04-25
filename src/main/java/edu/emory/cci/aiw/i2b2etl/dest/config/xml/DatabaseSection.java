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
package edu.emory.cci.aiw.i2b2etl.dest.config.xml;

import edu.emory.cci.aiw.i2b2etl.dest.config.ConfigurationReadException;
import edu.emory.cci.aiw.i2b2etl.dest.config.DataSourceDatabaseSpec;
import edu.emory.cci.aiw.i2b2etl.dest.config.Database;
import edu.emory.cci.aiw.i2b2etl.dest.config.DatabaseSpec;
import edu.emory.cci.aiw.i2b2etl.dest.config.DriverManagerDatabaseSpec;
import java.util.TreeMap;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.w3c.dom.*;

/**
 *
 * @author Andrew Post
 */
final class DatabaseSection extends ConfigurationSection implements Database {
    private static final String META_SCHEMA_DRIVER_MANAGER = "metaschema";
    private static final String DATA_SCHEMA_DRIVER_MANAGER = "dataschema";
    private static final String META_SCHEMA_DATA_SOURCE = "metaschemaDataSource";
    private static final String DATA_SCHEMA_DATA_SOURCE = "dataschemaDataSource";

    DatabaseSection() {
    }

    private TreeMap<String, DatabaseSpec> dbs = new TreeMap<>();

    DatabaseSpec get(String schema) {
        return this.dbs.get(schema);
    }

    @Override
    protected void put(Node node) throws ConfigurationReadException {
        NamedNodeMap nnm = node.getAttributes();
        String keyAttribute = readAttribute(nnm, "key", true);
        DatabaseSpec databaseSpec;
        if (keyAttribute.equals(META_SCHEMA_DRIVER_MANAGER) || keyAttribute.equals(DATA_SCHEMA_DRIVER_MANAGER)) {
            databaseSpec = new DriverManagerDatabaseSpec(
                    readAttribute(nnm, "connect", true),
                    readAttribute(nnm, "user", true),
                    readAttribute(nnm, "passwd", true));
        } else if (keyAttribute.equals(META_SCHEMA_DATA_SOURCE) || keyAttribute.equals(DATA_SCHEMA_DATA_SOURCE)) {
            databaseSpec = new DataSourceDatabaseSpec(
                    readAttribute(nnm, "connect", true),
                    readAttribute(nnm, "user", false),
                    readAttribute(nnm, "passwd", false)
                    );
        } else {
            throw new ConfigurationReadException("Invalid dbschema key: " + keyAttribute);
        }
        this.dbs.put(keyAttribute, databaseSpec);
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
        DatabaseSpec databaseSpec = this.dbs.get(META_SCHEMA_DRIVER_MANAGER);
        if (databaseSpec != null) {
            return databaseSpec;
        } else {
            return this.dbs.get(META_SCHEMA_DATA_SOURCE);
        }
    }

    @Override
    public DatabaseSpec getDataSpec() {
        DatabaseSpec databaseSpec = this.dbs.get(DATA_SCHEMA_DRIVER_MANAGER);
        if (databaseSpec != null) {
            return databaseSpec;
        } else {
            return this.dbs.get(DATA_SCHEMA_DATA_SOURCE);
        }
    }

}
