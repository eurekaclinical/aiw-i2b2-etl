package edu.emory.cci.aiw.i2b2etl.table;

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

import edu.emory.cci.aiw.i2b2etl.metadata.Concept;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.protempa.KnowledgeSource;
import org.protempa.proposition.Proposition;
import org.protempa.proposition.UniqueId;

/**
 *
 * @author Andrew Post
 */
public final class ProviderFactHandler extends FactHandler {

    public ProviderFactHandler() {
        super(null, "start", "finish", null);
    }
    
    @Override
    public void handleRecord(PatientDimension patient, VisitDimension visit, ProviderDimension provider, Proposition encounterProp, Map<Proposition, List<Proposition>> forwardDerivations, Map<Proposition, List<Proposition>> backwardDerivations, Map<UniqueId, Proposition> references, KnowledgeSource knowledgeSource, Set<Proposition> derivedPropositions, Connection cn) throws InvalidFactException {
        ObservationFact providerFact =
                createProviderObservationFact(
                encounterProp, patient, visit, provider,
                provider.getConcept());
        try {
            insert(providerFact, cn);
        } catch (SQLException ex) {
            throw new InvalidFactException("Provider fact not created", ex);
        }
    }
    
    private ObservationFact createProviderObservationFact(
            Proposition encounterProp, PatientDimension patient,
            VisitDimension visit, ProviderDimension provider, Concept concept)
            throws InvalidFactException {
        Date start = handleStartDate(null, encounterProp, null);
        Date finish = handleFinishDate(null, encounterProp, null);
        ValueFlagCode valueFlagCode = ValueFlagCode.NO_VALUE_FLAG;
        ObservationFact derivedObx = new ObservationFact(
                start, finish, patient,
                visit, provider, concept,
                null, valueFlagCode,
                concept.getDisplayName(),
                null,
                provider.getSourceSystem(),
                start == null,
                null, null, 1);
        concept.setInUse(true);
        return derivedObx;
    }
}
