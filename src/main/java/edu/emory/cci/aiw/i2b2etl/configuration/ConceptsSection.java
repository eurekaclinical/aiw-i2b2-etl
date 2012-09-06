package edu.emory.cci.aiw.i2b2etl.configuration;

import edu.emory.cci.aiw.i2b2etl.metadata.ValueTypeCode;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.w3c.dom.NamedNodeMap;

/**
 *
 * @author Andrew Post
 */
public final class ConceptsSection extends ConfigurationSection {

    public static class FolderSpec {

        public int skipGen;
        public String displayName;
        public String[] propositions;
        public String property;
        public String conceptCodePrefix;
        public ValueTypeCode valueType;
    }
    private List<FolderSpec> folders = new ArrayList<FolderSpec>();

    ConceptsSection() {
    }

    public FolderSpec[] getFolderSpecs() {
        return this.folders.toArray(new FolderSpec[this.folders.size()]);
    }

    @Override
    protected void put(NamedNodeMap nnm) throws ConfigurationReadException {
        FolderSpec folderSpec = new FolderSpec();
        String skipGenStr = readAttribute(nnm, "skipGen", false);
        if (skipGenStr != null) {
            folderSpec.skipGen = Integer.parseInt(skipGenStr);
        }
        folderSpec.displayName = readAttribute(nnm, "displayName", true);
        folderSpec.propositions = 
                new String[]{readAttribute(nnm, "proposition", true)};
        folderSpec.property = readAttribute(nnm, "property", false);
        folderSpec.conceptCodePrefix = readAttribute(nnm, "conceptCodePrefix",
                false);
        String valueTypeStr = readAttribute(nnm, "valueType", false);
        if (valueTypeStr != null) {
            folderSpec.valueType = ValueTypeCode.valueOf(valueTypeStr);
        } else {
            folderSpec.valueType = ValueTypeCode.UNSPECIFIED;
        }
        this.folders.add(folderSpec);
    }

    @Override
    protected String getNodeName() {
        return "folder";
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
