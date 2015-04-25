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
import edu.emory.cci.aiw.i2b2etl.dest.config.Data;
import edu.emory.cci.aiw.i2b2etl.dest.config.DataSpec;
import java.util.*;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

/**
 *
 * @author Andrew Post
 */
final class DataSection extends ConfigurationSection implements Data {

    private final Map<String, DataSpec> obxSpecs;


    DataSection() {
        this.obxSpecs = new HashMap<>();
    }
    
    @Override
    public DataSpec get(String key) {
        return this.obxSpecs.get(key);
    }
    
    @Override
    public Collection<DataSpec> getAll() {
        return this.obxSpecs.values();
    }

    @Override
    protected void put(Node node) throws ConfigurationReadException {
        NamedNodeMap nnm = node.getAttributes();
        DataSpec dataSpec = new DataSpec(
                readAttribute(nnm, "key", true),
                readAttribute(nnm, "reference", false),
                readAttribute(nnm, "property", false),
                readAttribute(nnm, "conceptCodePrefix", false),
                readAttribute(nnm, "start", false),
                readAttribute(nnm, "finish", false),
                readAttribute(nnm, "units", false)
        );
        if (dataSpec.getStart() != null && !dataSpec.getStart().equals("start") && !dataSpec.getStart().equals("finish")) {
            throw new ConfigurationReadException("The start attribute must have a value of 'start' or 'finish'");
        }
        if (dataSpec.getFinish() != null && !dataSpec.getFinish().equals("start") && !dataSpec.getFinish().equals("finish")) {
            throw new ConfigurationReadException("The finish attribute must have a value of 'start' or 'finish'");
        }
        if (dataSpec.getReferenceName() == null && dataSpec.getPropertyName() == null) {
            throw new ConfigurationReadException("Either referenceName or propertyName must be defined in dataType");
        }
        this.obxSpecs.put(dataSpec.getKey(), dataSpec);
    }

    @Override
    protected String getNodeName() {
        return "dataType";
    }

    

    String getString() {
        StringBuilder sb = new StringBuilder();
        for (DataSpec obx : this.obxSpecs.values()) {
            sb.append(obx.getReferenceName());
            sb.append(" : ");
            sb.append(obx.getPropertyName()).append("\n");
        }
        return sb.toString();
    }
}
