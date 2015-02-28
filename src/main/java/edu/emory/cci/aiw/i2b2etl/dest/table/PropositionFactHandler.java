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
package edu.emory.cci.aiw.i2b2etl.dest.table;

import edu.emory.cci.aiw.i2b2etl.dest.metadata.Metadata;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.Concept;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.ConceptId;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.ModifierConceptId;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.PropDefConceptId;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.PropertyConceptId;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.UnknownPropositionDefinitionException;

import java.sql.*;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.arp.javautil.sql.ConnectionSpec;
import org.drools.util.StringUtils;
import org.protempa.KnowledgeSource;
import org.protempa.KnowledgeSourceReadException;
import org.protempa.PropertyDefinition;
import org.protempa.PropositionDefinition;
import org.protempa.proposition.*;
import org.protempa.proposition.value.*;
import org.protempa.dest.table.Derivation;
import org.protempa.dest.table.Link;
import org.protempa.dest.table.LinkTraverser;
import org.protempa.proposition.comparator.AllPropositionIntervalComparator;

public final class PropositionFactHandler extends FactHandler {

    private static final Comparator<Proposition> PROP_COMP
            = new AllPropositionIntervalComparator();

    private final LinkTraverser linkTraverser;
    private final Link[] links;
    private final Link[] derivationLinks;
    private final KnowledgeSource knowledgeSource;
    private Map<String, PropositionDefinition> cache;
    private final Set<ConceptId> missingConcepts;

    public PropositionFactHandler(ConnectionSpec connSpec,
            Link[] links, String propertyName,
            String start, String finish, String unitsPropertyName,
            String[] potentialDerivedPropIds, Metadata metadata,
            KnowledgeSource knowledgeSource,
            Map<String, PropositionDefinition> cache,
            RejectedFactHandlerFactory rejectedFactHandlerFactory) throws SQLException {
        super(connSpec, propertyName, start, finish, unitsPropertyName, metadata, rejectedFactHandlerFactory);
        if (knowledgeSource == null) {
            throw new IllegalArgumentException("knowledgeSource cannot be null");
        }
        if (cache == null) {
            throw new IllegalArgumentException("cache cannot be null");
        }

        this.linkTraverser = new LinkTraverser();
        this.links = links;
        if (potentialDerivedPropIds == null) {
            potentialDerivedPropIds = StringUtils.EMPTY_STRING_ARRAY;
        }
        this.derivationLinks = new Link[]{
            new Derivation(potentialDerivedPropIds,
            Derivation.Behavior.MULT_FORWARD)
        };
        this.knowledgeSource = knowledgeSource;
        this.cache = cache;
        this.missingConcepts = new HashSet<>();
    }

    private abstract class PropositionWrapper implements Comparable<PropositionWrapper> {

        abstract Concept getConcept();

        abstract Proposition getProposition();

        @Override
        public int compareTo(PropositionWrapper o) {
            return PROP_COMP.compare(getProposition(), o.getProposition());
        }
    }

    @Override
    public void handleRecord(PatientDimension patient, VisitDimension visit,
            ProviderDimension provider,
            Proposition encounterProp,
            Map<Proposition, List<Proposition>> forwardDerivations,
            Map<Proposition, List<Proposition>> backwardDerivations,
            Map<UniqueId, Proposition> references,
            Set<Proposition> derivedPropositions)
            throws InvalidFactException {
        assert patient != null : "patient cannot be null";
        assert visit != null : "visit cannot be null";
        assert provider != null : "provider cannot be null";
        List<Proposition> props;
        try {
            props = this.linkTraverser.traverseLinks(this.links, encounterProp,
                    forwardDerivations,
                    backwardDerivations, references, knowledgeSource);
            for (Proposition prop : props) {
                String propertyName = getPropertyName();
                Value propertyVal = propertyName != null
                        ? prop.getProperty(propertyName) : null;
                PropDefConceptId conceptId = PropDefConceptId.getInstance(prop.getId(), propertyName, propertyVal, getMetadata());
                doInsert(conceptId, prop, encounterProp, patient, visit, provider);
                List<Proposition> derivedProps;
                try {
                    derivedProps = this.linkTraverser.traverseLinks(
                            this.derivationLinks, prop, forwardDerivations,
                            backwardDerivations, references,
                            knowledgeSource);
                } catch (KnowledgeSourceReadException ex) {
                    throw new InvalidFactException(ex);
                }
                for (Proposition derivedProp : derivedProps) {
                    PropDefConceptId derivedConceptId = PropDefConceptId.getInstance(derivedProp.getId(), null, null, getMetadata());
                    doInsert(derivedConceptId, derivedProp, encounterProp, patient, visit, provider);
                }
            }
        } catch (KnowledgeSourceReadException | UnknownPropositionDefinitionException ex) {
            throw new InvalidFactException(ex);
        }
    }

    private void doInsert(PropertyConceptId conceptId, Proposition prop, Proposition encounterProp, PatientDimension patient, VisitDimension visit, ProviderDimension provider) throws InvalidFactException, UnknownPropositionDefinitionException {
        assert conceptId != null : "conceptId cannot be null";
        assert prop != null : "prop cannot be null";
        assert encounterProp != null : "encounterProp cannot be null";
        assert patient != null : "patient cannot be null";
        assert visit != null : "visit cannot be null";
        assert provider != null : "provider cannot be null";
        if (getMetadata().getFromIdCache(conceptId) == null) {
            // Just log the problem on its first occurrence.
            if (this.missingConcepts.add(conceptId)) {
                TableUtil.logger().log(Level.WARNING, "No metadata for concept {0}; this data will not be loaded", conceptId);
            }
        } else {
            ObservationFact obx = populateObxFact(prop,
                    encounterProp, patient, visit, provider, conceptId, null, 1);
            PropositionDefinition propDef = this.cache.get(prop.getId());
            if (propDef == null) {
                throw new UnknownPropositionDefinitionException(prop);
            }
            try {
                insert(obx);
            } catch (SQLException ex) {
                String msg = "Observation fact not created";
                throw new InvalidFactException(msg, ex);
            }
            for (String propertyName : prop.getPropertyNames()) {
                PropertyDefinition propertyDefinition = propDef.propertyDefinition(propertyName);
                if (propertyDefinition != null) {
                    ModifierConceptId modConceptId = ModifierConceptId.getInstance(propertyDefinition.getDeclaringPropId(), propertyName, getMetadata());
                    if (getMetadata().getFromIdCache(modConceptId) == null) {
                        if (this.missingConcepts.add(modConceptId)) {
                            TableUtil.logger().log(Level.WARNING, "No metadata for modifier concept {0}; this modifier data will not be loaded", modConceptId);
                        }
                    } else {
                        ObservationFact modObx = populateObxFact(prop, encounterProp, patient, visit, provider, conceptId, modConceptId, 1);
                        try {
                            insert(modObx);
                        } catch (SQLException ex) {
                            throw new InvalidFactException("Modifier fact not created", ex);
                        }
                    }
                }
            }
        }
    }

    @Override
    public void close() throws SQLException {
        this.missingConcepts.clear();
        super.close();
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

}
