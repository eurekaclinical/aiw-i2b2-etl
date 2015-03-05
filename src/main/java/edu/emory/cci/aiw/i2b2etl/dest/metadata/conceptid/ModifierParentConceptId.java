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
import java.util.Objects;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 *
 * @author Andrew Post
 */
public final class ModifierParentConceptId implements ConceptId {
    /**
     * Returns a concept propId with the given string identifier.
     * 
     * @param id a concept identifier {@link String}. Cannot be 
     * <code>null</code>.
     * @return a {@link PropDefConceptId}.
     */
    public static ConceptId getInstance(String id, Metadata metadata) {
        return new ModifierParentConceptId(id, metadata);
    }
    private final Metadata metadata;
    private final String id;
    private String conceptCode;

    private ModifierParentConceptId(String id, Metadata metadata) {
        this.id = id;
        this.metadata = metadata;
    }

    @Override
    public String getId() {
        return this.id;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 59 * hash + Objects.hashCode(this.id);
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
        final ModifierParentConceptId other = (ModifierParentConceptId) obj;
        if (!Objects.equals(this.id, other.id)) {
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
            SimpleConceptCodeBuilder ccBuilder = new SimpleConceptCodeBuilder(this.metadata);
            ccBuilder.setId(prefix != null ? prefix : this.id);
            this.conceptCode = ccBuilder.build();
        }
        return this.conceptCode;
    }
    
    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

}
