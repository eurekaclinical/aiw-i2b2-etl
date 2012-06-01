package edu.emory.cci.aiw.i2b2etl.configuration;

import java.util.TreeMap;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.w3c.dom.*;

/**
 *
 * @author Andrew Post
 */
public final class DatabaseSection extends ConfigurationSection {

    DatabaseSection() {
    }

    // this is simply a place for database schema connect information.
    public class DatabaseSpec {
        public String key;
        public String user;
        public String passwd;
        public String connect;
    }
    private TreeMap<String, DatabaseSpec> dbs = new TreeMap<String, DatabaseSpec>();
    
    public DatabaseSpec get(String schema) {
        return this.dbs.get(schema);
    }

    @Override
    protected void put(NamedNodeMap nnm) throws ConfigurationReadException {
        DatabaseSpec databaseSpec = new DatabaseSpec();
        databaseSpec.key = readAttribute(nnm, "key", true);
        databaseSpec.user = readAttribute(nnm, "user", true);
        databaseSpec.passwd = readAttribute(nnm, "passwd", true);
        databaseSpec.connect = readAttribute(nnm, "connect", true);
        this.dbs.put(databaseSpec.key, databaseSpec);
    }

    @Override
    protected String getNodeName() {
        return "dbschema";
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
    
}
