package edu.emory.cci.aiw.i2b2etl.dest.metadata.conceptid;

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

import edu.emory.cci.aiw.i2b2etl.dest.metadata.Metadata;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.protempa.proposition.value.Value;

/**
 *
 * @author Andrew Post
 */
public final class ModifierConceptId implements PropertyConceptId {
    private final String propId;
    private final String propertyName;
    private final Value value;
    private String conceptCode;
    private final Metadata metadata;

    /**
     * Returns a concept propId with the given proposition propId and property 
     * name and value. It searches the concept id cache for an existing concept 
     * id with the same propId, property name and value. Next, it searches the
     * concept id cache for an existing concept id with the same propId and 
     * property name. Then, if none is found, it creates a new one.
     * 
     * @param propId a proposition propId {@link String}. Cannot be 
     * <code>null</code>.
     * @param propertyName a property name {@link String}.
     * @return a {@link PropDefConceptId}.
     */
    public static ModifierConceptId getInstance(String propId, String propertyName, Value value, Metadata metadata) {
        List<Object> key = new ArrayList<>(3);
        key.add(propId);
        key.add(propertyName);
        key.add(value);
        ModifierConceptId conceptId = (ModifierConceptId) metadata.getFromConceptIdCache(key);
        if (conceptId == null) {
            conceptId = new ModifierConceptId(propId, propertyName, value, metadata);
            metadata.putInConceptIdCache(key, conceptId);
        }
        return conceptId;
    }

    private ModifierConceptId(String propId, String propertyName, Value value, Metadata metadata) {
        if (metadata == null) {
            throw new IllegalArgumentException("metadata cannot be null");
        }
        this.propId = propId;
        this.propertyName = propertyName;
        this.metadata = metadata;
        this.value = value;
    }

    @Override
    public String getId() {
        return this.propId;
    }

    @Override
    public String getPropertyName() {
        return this.propertyName;
    }

    public Value getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 79 * hash + Objects.hashCode(this.propId);
        hash = 79 * hash + Objects.hashCode(this.propertyName);
        hash = 79 * hash + Objects.hashCode(this.conceptCode);
        hash = 79 * hash + Objects.hashCode(this.value);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final ModifierConceptId other = (ModifierConceptId) obj;
        if (!Objects.equals(this.propId, other.propId)) {
            return false;
        }
        if (!Objects.equals(this.propertyName, other.propertyName)) {
            return false;
        }
        if (!Objects.equals(this.conceptCode, other.conceptCode)) {
            return false;
        }
        if (!Objects.equals(this.value, other.value)) {
            return false;
        }
        return true;
    }
    
    @Override
    public String toConceptCode() throws InvalidConceptCodeException {
        return toConceptCode(null);
    }

    @Override
    public String toConceptCode(String prefix) throws InvalidConceptCodeException {
        if (this.conceptCode == null) {
            PropDefConceptCodeBuilder ccBuilder = new PropDefConceptCodeBuilder(this.metadata);
            ccBuilder.setPropositionId(prefix != null ? prefix : propId);
            ccBuilder.setPropertyName(prefix != null ? null : propertyName);
            ccBuilder.setValue(value);
            this.conceptCode = ccBuilder.build();
        }
        return this.conceptCode;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
