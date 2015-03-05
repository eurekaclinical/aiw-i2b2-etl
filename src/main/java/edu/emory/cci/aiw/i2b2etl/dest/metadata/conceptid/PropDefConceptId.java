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
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.emory.cci.aiw.i2b2etl.dest.metadata.conceptid;

import edu.emory.cci.aiw.i2b2etl.dest.metadata.Metadata;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.protempa.proposition.value.Value;

/**
 * An unique identifier for a concept in an i2b2 ontology.
 * 
 * @author Andrew Post
 */
public final class PropDefConceptId implements PropertyConceptId {
    
    private final String propId;
    private final String propertyName;
    private final Value value;
    private String conceptCode;
    private final Metadata metadata;

    /**
     * Returns a concept propId with the given proposition propId and property name.
     * 
     * @param propId a proposition propId {@link String}. Cannot be 
     * <code>null</code>.
     * @param propertyName a property name {@link String}.
     * @return a {@link PropDefConceptId}.
     */
    public static ConceptId getInstance(String propId, String propertyName, Metadata metadata) {
        return getInstance(propId, propertyName, null, metadata);
    }

    /**
     * Returns a concept propId with the given proposition propId, property name and
 value.
     * 
     * @param propId a proposition propId {@link String}. Cannot be 
     * <code>null</code>.
     * @param propertyName a property name {@link String}.
     * @param value a {@link Value}.
     * @return a {@link PropDefConceptId}.
     */
    public static PropDefConceptId getInstance(String propId,
            String propertyName, Value value, Metadata metadata) {
        List<Object> key = new ArrayList<>(3);
        key.add(propId);
        key.add(propertyName);
        key.add(value);
        PropDefConceptId conceptId = (PropDefConceptId) metadata.getFromConceptIdCache(key);
        if (conceptId == null) {
            conceptId = new PropDefConceptId(propId, propertyName, value, metadata);
            metadata.putInConceptIdCache(key, conceptId);
        }
        return conceptId;
    }

    private PropDefConceptId(String propId, String propertyName, Value value, Metadata metadata) {
        if (metadata == null) {
            throw new IllegalArgumentException("metadata cannot be null");
        }
        this.propId = propId;
        this.propertyName = propertyName;
        this.value = value;
        this.metadata = metadata;
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
        return this.value;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final PropDefConceptId other = (PropDefConceptId) obj;
        if ((this.propId == null) ? (other.propId != null) : !this.propId.equals(other.propId)) {
            return false;
        }
        if ((this.propertyName == null) ? (other.propertyName != null) : !this.propertyName.equals(other.propertyName)) {
            return false;
        }
        if ((this.value == null) ? (other.value != null) : !this.value.equals(other.value)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 11 * hash + (this.propId != null ? this.propId.hashCode() : 0);
        hash = 11 * hash + (this.propertyName != null ? this.propertyName.hashCode() : 0);
        hash = 11 * hash + (this.value != null ? this.value.hashCode() : 0);
        return hash;
    }

    @Override
    public String toConceptCode() throws InvalidConceptCodeException {
        return toConceptCode(null);
    }

    @Override
    public String toConceptCode(String prefix) throws InvalidConceptCodeException {
        if (this.conceptCode == null) {
            PropDefConceptCodeBuilder ccBuilder = new PropDefConceptCodeBuilder(this.metadata);
            ccBuilder.setPropositionId(prefix != null ? prefix : this.propId);
            ccBuilder.setPropertyName(prefix != null ? null : this.propertyName);
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
