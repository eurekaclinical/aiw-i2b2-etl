package edu.emory.cci.aiw.i2b2etl.dest.metadata;

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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 *
 * @author Andrew Post
 */
public final class ModifierConceptId implements PropertyConceptId {
    private final String propId;
    private final String propertyName;
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
    public static ModifierConceptId getInstance(String propId, String propertyName, Metadata metadata) {
        List<Object> key = new ArrayList<>(3);
        key.add(propId);
        key.add(propertyName);
        ModifierConceptId conceptId = (ModifierConceptId) metadata.getFromConceptIdCache(key);
        if (conceptId == null) {
            conceptId = new ModifierConceptId(propId, propertyName, metadata);
            metadata.putInConceptIdCache(key, conceptId);
        }
        return conceptId;
    }

    private ModifierConceptId(String propId, String propertyName, Metadata metadata) {
        if (metadata == null) {
            throw new IllegalArgumentException("metadata cannot be null");
        }
        this.propId = propId;
        this.propertyName = propertyName;
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

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 79 * hash + Objects.hashCode(this.propId);
        hash = 79 * hash + Objects.hashCode(this.propertyName);
        hash = 79 * hash + Objects.hashCode(this.conceptCode);
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
        return true;
    }
    
    @Override
    public String toConceptCode() throws InvalidConceptCodeException {
        if (this.conceptCode == null) {
            PropDefConceptCodeBuilder ccBuilder = new PropDefConceptCodeBuilder(this.metadata);
            ccBuilder.setPropositionId(propId);
            ccBuilder.setPropertyName(propertyName);
            this.conceptCode = ccBuilder.build();
        }
        return this.conceptCode;
    }

    @Override
    public String toConceptCode(String prefix) throws InvalidConceptCodeException {
        if (this.conceptCode == null) {
            if (prefix != null) {
                SimpleConceptCodeBuilder ccBuilder = new SimpleConceptCodeBuilder(this.metadata);
                ccBuilder.setId(prefix);
                this.conceptCode = ccBuilder.build();
                return this.conceptCode;
            } else {
                return toConceptCode();
            }
        } else {
            return this.conceptCode;
        }
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
