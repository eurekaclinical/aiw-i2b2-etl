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
package edu.emory.cci.aiw.i2b2etl.table;

import edu.emory.cci.aiw.i2b2etl.metadata.Metadata;
import edu.emory.cci.aiw.i2b2etl.metadata.Concept;
import edu.emory.cci.aiw.i2b2etl.metadata.InvalidConceptCodeException;

import java.sql.*;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.drools.util.StringUtils;
import org.protempa.KnowledgeSource;
import org.protempa.KnowledgeSourceReadException;
import org.protempa.proposition.*;
import org.protempa.proposition.value.*;
import org.protempa.dest.table.Derivation;
import org.protempa.dest.table.Link;
import org.protempa.dest.table.LinkTraverser;

public final class PropositionFactHandler extends FactHandler {

    private final LinkTraverser linkTraverser;
    private final Link[] links;
    private Metadata metadata;
    private final Link[] derivationLinks;

    public PropositionFactHandler(Link[] links, String propertyName, String start,
                                  String finish, String unitsPropertyName,
                                  String[] potentialDerivedPropIds, Metadata metadata) {
        super(propertyName, start, finish, unitsPropertyName);
        if (metadata == null) {
            throw new IllegalArgumentException("metadata cannot be null");
        }

        this.metadata = metadata;
        this.linkTraverser = new LinkTraverser();
        this.links = links;
        if (potentialDerivedPropIds == null) {
            potentialDerivedPropIds = StringUtils.EMPTY_STRING_ARRAY;
        }
        this.derivationLinks = new Link[]{
                new Derivation(potentialDerivedPropIds,
                        Derivation.Behavior.MULT_FORWARD)
        };
    }

    @Override
    public void handleRecord(PatientDimension patient, VisitDimension visit,
                             ProviderDimension provider,
                             Proposition encounterProp,
                             Map<Proposition, List<Proposition>> forwardDerivations,
                             Map<Proposition, List<Proposition>> backwardDerivations,
                             Map<UniqueId, Proposition> references,
                             KnowledgeSource knowledgeSource,
                             Set<Proposition> derivedPropositions, Connection cn)
            throws InvalidFactException {
        assert patient != null : "patient cannot be null";
        assert visit != null : "visit cannot be null";
        assert provider != null : "provider cannot be null";
        List<Proposition> props;
        try {
            props = this.linkTraverser.traverseLinks(this.links, encounterProp,
                    forwardDerivations,
                    backwardDerivations, references, knowledgeSource);
        } catch (KnowledgeSourceReadException ex) {
            throw new InvalidFactException(ex);
        }

        String propertyName = getPropertyName();

        for (Proposition prop : props) {
            Value propertyVal = propertyName != null
                    ? prop.getProperty(propertyName) : null;
            Concept concept =
                    this.metadata.getFromIdCache(prop.getId(),
                            propertyName, propertyVal);
            if (concept != null) {
                ObservationFact obx = createObservationFact(prop,
                        encounterProp, patient, visit, provider, concept);
                try {
                    insert(obx, cn);

                    List<Proposition> derivedProps;
                    try {
                        derivedProps = this.linkTraverser.traverseLinks(
                                this.derivationLinks, prop, forwardDerivations,
                                backwardDerivations, references,
                                knowledgeSource);
                    } catch (KnowledgeSourceReadException ex) {
                        throw new InvalidFactException(ex);
                    }
                    for (Proposition derivedProp
                            : new HashSet<>(derivedProps)) {
                        if (derivedPropositions.add(derivedProp)) {
                            Concept derivedConcept =
                                    this.metadata.getFromIdCache(derivedProp.getId(),
                                            null, null);
                            if (derivedConcept != null) {
                                ObservationFact derivedObx = createObservationFact(
                                        derivedProp, encounterProp, patient, visit,
                                        provider, derivedConcept);
                                try {
                                    insert(derivedObx, cn);
                                } catch (SQLException | InvalidConceptCodeException sqle) {
                                    String msg = "Observation fact not created for " + prop.getId();
                                    throw new InvalidFactException(msg, sqle);
                                }
                            }
                        }
                    }
                } catch (SQLException | InvalidConceptCodeException ex) {
                    String msg = "Observation fact not created for " + prop.getId() + "." + propertyName + "=" + propertyVal;
                    throw new InvalidFactException(msg, ex);
                }
            }
        }
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    private ObservationFact createObservationFact(Proposition prop,
                                                  Proposition encounterProp, PatientDimension patient,
                                                  VisitDimension visit, ProviderDimension provider, Concept concept)
            throws InvalidFactException {
        Date start = handleStartDate(prop, encounterProp, null);
        Date finish = handleFinishDate(prop, encounterProp, null);
        Value value = handleValue(prop);
        ValueFlagCode valueFlagCode = ValueFlagCode.NO_VALUE_FLAG;
        String units = handleUnits(prop);
        ObservationFact derivedObx = new ObservationFact(
                start, finish, patient,
                visit, provider, concept,
                value, valueFlagCode,
                concept.getDisplayName(),
                units,
                prop.getDataSourceType().getStringRepresentation(),
                start == null);
        concept.setInUse(true);
        return derivedObx;
    }
}
