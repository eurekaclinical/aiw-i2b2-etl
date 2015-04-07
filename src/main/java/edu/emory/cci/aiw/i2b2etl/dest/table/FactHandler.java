package edu.emory.cci.aiw.i2b2etl.dest.table;

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
import edu.emory.cci.aiw.i2b2etl.dest.metadata.Concept;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.Metadata;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.conceptid.ModifierConceptId;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.conceptid.PropertyConceptId;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.arp.javautil.sql.ConnectionSpec;
import org.protempa.proposition.Parameter;
import org.protempa.proposition.Proposition;
import org.protempa.proposition.TemporalProposition;
import org.protempa.proposition.UniqueId;
import org.protempa.proposition.value.AbsoluteTimeGranularityUtil;
import org.protempa.proposition.value.NominalValue;
import org.protempa.proposition.value.Value;

/**
 *
 * @author Andrew Post
 */
public abstract class FactHandler extends AbstractFactHandler {

    public static final String TEMP_OBSERVATION_TABLE = "ek_temp_observation";

    private final String startConfig;
    private final String finishConfig;
    private final String unitsPropertyName;
    private final String propertyName;
    private final Metadata metadata;
    private final RejectedFactHandler rejectedFactHandler;
    private final ObservationFact obx;

    public FactHandler(ConnectionSpec connSpec, String propertyName, String startConfig, String finishConfig, String unitsPropertyName, Metadata metadata, RejectedFactHandlerFactory rejectedFactHandlerFactory) throws SQLException {
        super(connSpec,
                "insert into " + TEMP_OBSERVATION_TABLE + "(encounter_id, encounter_id_source, concept_cd, "
                + "patient_id, patient_id_source, provider_id, start_date, modifier_cd, instance_num, valtype_cd, tval_char, nval_num, valueflag_cd, quantity_num, "
                + "confidence_num, observation_blob, units_cd, end_date, location_cd, update_date, download_date, import_date, sourcesystem_cd, upload_id)"
                + " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        if (metadata == null) {
            throw new IllegalArgumentException("metadata cannot be null");
        }
        this.propertyName = propertyName;
        this.startConfig = startConfig;
        this.finishConfig = finishConfig;
        this.unitsPropertyName = unitsPropertyName;
        this.metadata = metadata;
        if (rejectedFactHandlerFactory != null) {
            this.rejectedFactHandler = rejectedFactHandlerFactory.getInstance();
        } else {
            this.rejectedFactHandler = null;
        }
        this.obx = new ObservationFact();
    }

    Metadata getMetadata() {
        return metadata;
    }

    public String getStartConfig() {
        return startConfig;
    }

    public String getFinishConfig() {
        return finishConfig;
    }

    public String getUnitsPropertyName() {
        return unitsPropertyName;
    }

    public String getPropertyName() {
        return propertyName;
    }

    @Override
    public void close() throws SQLException {
        try {
            super.close();
        } finally {
            if (this.rejectedFactHandler != null) {
                this.rejectedFactHandler.close();
            }
        }
    }
    
    public abstract void handleRecord(PatientDimension patient, VisitDimension visit, ProviderDimension provider, Proposition encounterProp, Map<Proposition, List<Proposition>> forwardDerivations, Map<Proposition, List<Proposition>> backwardDerivations, Map<UniqueId, Proposition> references, Set<Proposition> derivedPropositions) throws InvalidFactException;

    protected ObservationFact populateObxFact(Proposition prop,
            Proposition encounterProp, PatientDimension patient,
            VisitDimension visit, ProviderDimension provider,
            PropertyConceptId conceptId, ModifierConceptId modConceptId)
            throws InvalidFactException {
        Concept concept = this.metadata.getFromIdCache(conceptId);
        assert concept != null : "No concept found matching concept id " + conceptId;
        Concept modConcept;
        if (modConceptId != null) {
            modConcept = this.metadata.getFromIdCache(modConceptId);
            assert modConcept != null : "No modifier concept found matching concept id " + modConceptId;
        } else {
            modConcept = null;
        }
        Date start = handleStartDate(prop, encounterProp, null);
        Date finish = handleFinishDate(prop, encounterProp, null);
        Value value = modConcept != null ? handleValue(prop, modConcept, modConceptId) : handleValue(prop, concept, conceptId);
        ValueFlagCode valueFlagCode = ValueFlagCode.NO_VALUE_FLAG;
        String units = handleUnits(prop);
        obx.reset();
        obx.setStartDate(TableUtil.setTimestampAttribute(start));
        if (start == null) {
            obx.setRejected(true);
            obx.addRejectionReason("Null start date");
        }
        obx.setEndDate(TableUtil.setTimestampAttribute(finish));
        obx.setPatient(patient);
        obx.setVisit(visit);
        obx.setProvider(provider);
        obx.setConcept(concept);
        obx.setValue(value);
        obx.setValueFlagCode(valueFlagCode);
        obx.setUnits(units);
        obx.setSourceSystem(prop.getSourceSystem().getStringRepresentation());
        obx.setDownloadDate(TableUtil.setTimestampAttribute(prop.getDownloadDate()));
        Date updateDate = prop.getUpdateDate();
        if (updateDate == null) {
            updateDate = prop.getCreateDate();
        }
        obx.setUpdateDate(TableUtil.setTimestampAttribute(updateDate));
        obx.setInstanceNum(prop.getUniqueId().getLocalUniqueId().getNumericalId());
        if (concept != null) {
            obx.setDisplayName(concept.getDisplayName());
            if (modConcept != null) {
                obx.setModifierCd(modConcept.getConceptCode());
            }
            if (!obx.isRejected()) {
                concept.setInUse(true);
                if (modConcept != null) {
                    modConcept.setInUse(true);
                }
            }
        }
        return obx;
    }

    @Override
    public void insert(ObservationFact record) throws SQLException {
        if (record != null) {
            if (record.isRejected()) {
                if (this.rejectedFactHandler != null) {
                    this.rejectedFactHandler.insert(record);
                }
            } else {
                super.insert(record);
            }
        }
    }

    protected final String handleUnits(Proposition prop) {
        String value;
        if (this.unitsPropertyName != null && prop != null) {
            Value unitsVal = prop.getProperty(this.unitsPropertyName);
            if (unitsVal != null) {
                value = unitsVal.getFormatted();
            } else {
                value = null;
            }
        } else {
            value = null;
        }
        return value;
    }

    private Value handleValue(Proposition prop, Concept concept, PropertyConceptId conceptId) {
        Value value = null;
        if (prop != null && concept != null && conceptId != null) {
            if (this.propertyName != null) {
                Value tvalCharVal = prop.getProperty(this.propertyName);
                if (tvalCharVal != null) {
                    value = tvalCharVal;
                }
            } else if (concept.isModifier()) {
                value = prop.getProperty(conceptId.getPropertyName());
            } else if (prop instanceof Parameter) {
                value = ((Parameter) prop).getValue();
            } else {
                value = NominalValue.getInstance(prop.getId());
            }
        }
        return value;
    }

    protected final Date handleStartDate(Proposition prop, Proposition encounterProp, Value propertyVal) throws InvalidFactException {
        Date start;
        if (prop instanceof TemporalProposition) {
            start = AbsoluteTimeGranularityUtil.asDate(((TemporalProposition) prop).getInterval().getMinStart());
        } else if (this.startConfig != null) {
            switch (this.startConfig) {
                case "start":
                    start = AbsoluteTimeGranularityUtil.asDate(((TemporalProposition) encounterProp).getInterval().getMinStart());
                    break;
                case "finish":
                    start = AbsoluteTimeGranularityUtil.asDate(((TemporalProposition) encounterProp).getInterval().getMinFinish());
                    break;
                default:
                    start = null;
                    break;
            }
        } else {
            start = null;
        }
        return start;
    }

    protected final Date handleFinishDate(Proposition prop, Proposition encounterProp, Value propertyVal) throws InvalidFactException {
        Date start;
        if (prop instanceof TemporalProposition) {
            start = AbsoluteTimeGranularityUtil.asDate(((TemporalProposition) prop).getInterval().getMinFinish());
        } else if (this.finishConfig != null) {
            switch (this.finishConfig) {
                case "start":
                    start = AbsoluteTimeGranularityUtil.asDate(((TemporalProposition) encounterProp).getInterval().getMinStart());
                    break;
                case "finish":
                    start = AbsoluteTimeGranularityUtil.asDate(((TemporalProposition) encounterProp).getInterval().getMinFinish());
                    break;
                default:
                    start = null;
                    break;
            }
        } else {
            start = null;
        }
        return start;
    }

}
