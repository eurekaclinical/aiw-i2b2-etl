package edu.emory.cci.aiw.i2b2etl.dest.table;

/*
 * #%L
 * AIW i2b2 ETL
 * %%
 * Copyright (C) 2012 - 2015 Emory University
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

import edu.emory.cci.aiw.i2b2etl.dest.config.Data;
import edu.emory.cci.aiw.i2b2etl.dest.config.DataSpec;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.protempa.proposition.Proposition;
import org.protempa.proposition.UniqueId;
import org.protempa.proposition.value.Value;

/**
 *
 * @author Andrew Post
 */
public class DimensionFactory {
    private final Data data;

    public DimensionFactory(Data data) {
        this.data = data;
    }

    protected Data getData() {
        return data;
    }
    
    protected Value getField(String field, Proposition encounterProp, Map<UniqueId, Proposition> references) {
        Value val;
        if (field != null) {
            DataSpec obxSpec = data.get(field);
            assert obxSpec.getPropertyName() != null : "propertyName cannot be null";
            if (obxSpec != null) {
                if (obxSpec.getReferenceName() != null) {
                    List<UniqueId> uids = encounterProp.getReferences(obxSpec.getReferenceName());
                    int size = uids.size();
                    if (size > 0) {
                        if (size > 1) {
                            Logger logger = TableUtil.logger();
                            logger.log(Level.WARNING,
                                    "Multiple propositions with {0} property found for {1}, using only the first one",
                                    new Object[]{field, encounterProp});
                        }
                        Proposition prop = references.get(uids.get(0));
                        val = prop.getProperty(obxSpec.getPropertyName());
                    } else {
                        val = null;
                    }
                } else {
                    val = encounterProp.getProperty(obxSpec.getPropertyName());
                }
            } else {
                throw new AssertionError("Invalid key: " + field);
            }
        } else {
            val = null;
        }
        return val;
    }
}
