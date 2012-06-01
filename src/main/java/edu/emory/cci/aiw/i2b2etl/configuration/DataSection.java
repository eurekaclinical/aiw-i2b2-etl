package edu.emory.cci.aiw.i2b2etl.configuration;

import java.util.*;
import org.w3c.dom.Attr;
import org.w3c.dom.NamedNodeMap;

/**
 *
 * @author Andrew Post
 */
public final class DataSection extends ConfigurationSection {

    private final Map<String, DataSpec> obxSpecs;

    public class DataSpec {
        public String key;
        public String referenceName;
        public String propertyName;
        public String conceptCodePrefix;
        public String start;
        public String finish;
        public String units;
    }

    DataSection() {
        this.obxSpecs = new HashMap<String, DataSpec>();
    }
    
    public DataSpec get(String key) {
        return this.obxSpecs.get(key);
    }
    
    public Collection<DataSpec> getAll() {
        return this.obxSpecs.values();
    }

    @Override
    protected void put(NamedNodeMap nnm) throws ConfigurationReadException {
        DataSpec dataSpec = new DataSpec();
        dataSpec.key = readAttribute(nnm, "key", true);
        dataSpec.referenceName = readAttribute(nnm, "reference", false);
        dataSpec.propertyName = readAttribute(nnm, "property", false);
        dataSpec.conceptCodePrefix = readAttribute(nnm, "conceptCodePrefix", false);
        dataSpec.start = readAttribute(nnm, "start", false);
        dataSpec.finish = readAttribute(nnm, "finish", false);
        dataSpec.units = readAttribute(nnm, "units", false);
        if (dataSpec.start != null && !dataSpec.start.equals("start") && !dataSpec.start.equals("finish")) {
            throw new ConfigurationReadException("The start attribute must have a value of 'start' or 'finish'");
        }
        if (dataSpec.finish != null && !dataSpec.finish.equals("start") && !dataSpec.finish.equals("finish")) {
            throw new ConfigurationReadException("The finish attribute must have a value of 'start' or 'finish'");
        }
        if (dataSpec.referenceName == null && dataSpec.propertyName == null) {
            throw new ConfigurationReadException("Either referenceName or propertyName must be defined in dataType");
        }
        this.obxSpecs.put(dataSpec.key, dataSpec);
    }

    @Override
    protected String getNodeName() {
        return "dataType";
    }

    

    public String getString() {
        StringBuilder sb = new StringBuilder();
        for (DataSpec obx : this.obxSpecs.values()) {
            sb.append(obx.referenceName);
            sb.append(" : ");
            sb.append(obx.propertyName).append("\n");
        }
        return sb.toString();
    }
}
