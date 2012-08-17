/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.emory.cci.aiw.i2b2etl.metadata;

import java.util.*;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.protempa.proposition.value.Value;

/**
 * An unique identifier for a concept in an i2b2 ontology.
 * 
 * @author Andrew Post
 */
public class ConceptId {
    
    private final String propId;
    private final String propertyName;
    private final Value value;
    private DefaultConceptCodeBuilder ccBuilder;
    private final Metadata metadata;

//    public static ConceptId newInstance(String id, String propertyName, Value value, KnowledgeSource knowledgeSource) throws OntologyBuildException {
//        int index = 0;
//        try {
//            PropositionDefinition propositionDef = knowledgeSource.readPropositionDefinition(id);
//            while (true) {
//                if (propositionDef != null && propertyName != null) {
//                    PropertyDefinition propertyDef = propositionDef.propertyDefinition(propertyName);
//                    String valueSetId = propertyDef.getValueSetId();
//                    if (valueSetId == null) {
//                        id = id + index++;
//                        continue;
//                    } else {
//                        ValueSet valueSet = knowledgeSource.readValueSet(valueSetId);
//                        if (valueSet.isInValueSet(value)) {
//                            id = id + index++;
//                            continue;
//                        }
//                    }
//                }
//                break;
//            }
//        } catch (KnowledgeSourceReadException e) {
//            throw new OntologyBuildException("Error reading knowledge Source", e);
//        }
//
//        List<Object> key = new ArrayList<Object>();
//        key.add(id);
//        key.add(propertyName);
//        key.add(value);
//        if (CACHE.containsKey(key)) {
//            return null;
//        }
//
//        return new ConceptId(id, propertyName, value);
//    }
//
//    /**
//     * Forces the creation of a new concept id based on the given concept 
//     * identifier string, even if a concept id with the given identifier
//     * already exists.
//     *
//     * @param id a concept identifier {@link String}.
//     * @param knowledgeSource the {@link KnowledgeSource}.
//     * @return a new concept id.
//     * @throws OntologyBuildException if an error occurred while creating the
//     * new concept id.
//     */
//    public static ConceptId newInstance(String id, KnowledgeSource knowledgeSource) throws OntologyBuildException {
//        if (id == null) {
//            throw new IllegalArgumentException("id cannot be null");
//        }
//        int index = 0;
//        try {
//            while (knowledgeSource.readPropositionDefinition(id) != null) {
//                id = id + index++;
//            }
//            List<String> key = new ArrayList<String>();
//            key.add(id);
//            key.add(null);
//            key.add(null);
//            while (CACHE.containsKey(key)) {
//                id = id + index++;
//                key.set(0, id);
//            }
//        } catch (KnowledgeSourceReadException e) {
//            throw new OntologyBuildException("Error reading knowledge source", e);
//        }
//        return ConceptId.getInstance(id);
//    }

    /**
     * Returns a concept id with the given string identifier.
     * 
     * @param id a concept identifier {@link String}. Cannot be 
     * <code>null</code>.
     * @return a {@link ConceptId}.
     */
    public static ConceptId getInstance(String id, Metadata metadata) {
        return getInstance(id, null, metadata);
    }

    /**
     * Returns a concept id with the given proposition id and property name.
     * 
     * @param propId a proposition id {@link String}. Cannot be 
     * <code>null</code>.
     * @param propertyName a property name {@link String}.
     * @return a {@link ConceptId}.
     */
    public static ConceptId getInstance(String propId, String propertyName, Metadata metadata) {
        return getInstance(propId, propertyName, null, metadata);
    }

    /**
     * Returns a concept id with the given proposition id, property name and
     * value.
     * 
     * @param propId a proposition id {@link String}. Cannot be 
     * <code>null</code>.
     * @param propertyName a property name {@link String}.
     * @param value a {@link Value}.
     * @return a {@link ConceptId}.
     */
    public static ConceptId getInstance(String propId,
            String propertyName, Value value, Metadata metadata) {
//        if (propId == null) {
//            throw new IllegalArgumentException("propId cannot be null");
//        }
        List<Object> key = new ArrayList<Object>(3);
        key.add(propId);
        key.add(propertyName);
        key.add(value);
        ConceptId conceptId = metadata.getFromConceptIdCache(key);
        if (conceptId == null) {
            conceptId = new ConceptId(propId, propertyName, value, metadata);
            metadata.putInConceptIdCache(key, conceptId);
        }
        return conceptId;
    }

    private ConceptId(String propId, String propertyName, Value value, Metadata metadata) {
//        if (propId == null) {
//            throw new IllegalArgumentException("propId cannot be null");
//        }
        if (metadata == null) {
            throw new IllegalArgumentException("metadata cannot be null");
        }
        this.propId = propId;
        this.propertyName = propertyName;
        this.value = value;
        this.metadata = metadata;
    }

    public String getPropositionId() {
        return this.propId;
    }

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
        final ConceptId other = (ConceptId) obj;
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

    String toConceptCode() throws InvalidConceptCodeException {
        if (this.ccBuilder == null) {
            this.ccBuilder = new DefaultConceptCodeBuilder(this.metadata);
            this.ccBuilder.setPropositionId(propId);
            this.ccBuilder.setPropertyName(propertyName);
            this.ccBuilder.setValue(value);
        }
        return this.ccBuilder.build();
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
