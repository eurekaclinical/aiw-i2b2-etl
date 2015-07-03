package edu.emory.cci.aiw.i2b2etl.dest.config.xml;

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
import edu.emory.cci.aiw.i2b2etl.dest.config.Concepts;
import edu.emory.cci.aiw.i2b2etl.dest.config.ConfigurationInitException;
import edu.emory.cci.aiw.i2b2etl.dest.config.FolderSpec;
import edu.emory.cci.aiw.i2b2etl.dest.config.ModifierSpec;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.ValueTypeCode;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 *
 * @author Andrew Post
 */
final class ConceptsSection extends ConfigurationSection implements Concepts {

    private List<FolderSpec> folders = new ArrayList<>();

    ConceptsSection() {
    }

    @Override
    public FolderSpec[] getFolderSpecs() {
        return this.folders.toArray(new FolderSpec[this.folders.size()]);
    }

    @Override
    protected void put(Node node) throws ConfigurationInitException {
        List<ModifierSpec> msList = new ArrayList<>();
        NodeList nL = node.getChildNodes();
        for (int i = 0; i < nL.getLength(); i++) {
            Node section = nL.item(i);
            if (section.getNodeType() == Node.ELEMENT_NODE) {
                if (section.getNodeName().equals("modifier")) {
                    NamedNodeMap nnm = section.getAttributes();
                    msList.add(new ModifierSpec(
                            readAttribute(nnm, "displayName", false),
                            readAttribute(nnm, "codePrefix", false),
                            readAttribute(nnm, "property", true),
                            readAttribute(nnm, "value", false)));
                }
            }
        }
        NamedNodeMap nnm = node.getAttributes();
        String valueTypeStr = readAttribute(nnm, "valueType", false);
        FolderSpec folderSpec = new FolderSpec(
                null,
                new String[]{readAttribute(nnm, "proposition", true)},
                readAttribute(nnm, "property", false),
                readAttribute(nnm, "conceptCodePrefix", false),
                valueTypeStr != null ? ValueTypeCode.valueOf(valueTypeStr) : ValueTypeCode.UNSPECIFIED,
                Boolean.parseBoolean(readAttribute(nnm, "alreadyLoaded", false)),
                msList.toArray(new ModifierSpec[msList.size()])
        );
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
