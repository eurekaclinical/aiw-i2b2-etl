package edu.emory.cci.aiw.i2b2etl.configuration;

import java.util.TreeMap;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.w3c.dom.*;

/**
 *
 * @author Andrew Post
 */
public final class DictionarySection extends ConfigurationSection {
    private TreeMap<String, String> dictionary = new TreeMap<String, String>();

    DictionarySection() {
    }
    
    public String get(String key) {
        return dictionary.get(key);
    }

    @Override
    protected void put(NamedNodeMap nnm) throws ConfigurationReadException {
        String key = readAttribute(nnm, "key", true);
        String value = readAttribute(nnm, "value", true);
        dictionary.put(key, value);
    }

    @Override
    protected String getNodeName() {
        return "entry";
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
    
}
