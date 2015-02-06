/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import org.arp.javautil.sql.ConnectionSpec;
import org.protempa.KnowledgeSource;
import org.protempa.proposition.Parameter;
import org.protempa.proposition.Proposition;
import org.protempa.proposition.TemporalProposition;
import org.protempa.proposition.UniqueId;
import org.protempa.proposition.value.AbsoluteTimeGranularityUtil;
import org.protempa.proposition.value.InequalityNumberValue;
import org.protempa.proposition.value.NominalValue;
import org.protempa.proposition.value.NumberValue;
import org.protempa.proposition.value.NumericalValue;
import org.protempa.proposition.value.Value;

/**
 *
 * @author arpost
 */
public abstract class FactHandler extends RecordHandler<ObservationFact> {
    
    public static final String TEMP_OBSERVATION_TABLE = "temp_observation";

    private Timestamp importTimestamp;
    private final String startConfig;
    private final String finishConfig;
    private final String unitsPropertyName;
    private final String propertyName;
    private final ObservationFact obx;

    public FactHandler(ConnectionSpec connSpec, String propertyName, String startConfig, String finishConfig, String unitsPropertyName) throws SQLException {
        super(connSpec, "insert into " + TEMP_OBSERVATION_TABLE + "(encounter_id, encounter_id_source, concept_cd, " +
                            "patient_id, patient_id_source, provider_id, start_date, modifier_cd, instance_num, valtype_cd, tval_char, nval_num, valueflag_cd, quantity_num, " +
                            "confidence_num, observation_blob, units_cd, end_date, location_cd, update_date, download_date, import_date, sourcesystem_cd, upload_id)" +
                            " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        this.propertyName = propertyName;
        this.startConfig = startConfig;
        this.finishConfig = finishConfig;
        this.unitsPropertyName = unitsPropertyName;
        this.obx = new ObservationFact();
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

    public abstract void handleRecord(PatientDimension patient, VisitDimension visit, ProviderDimension provider, Proposition encounterProp, Map<Proposition, List<Proposition>> forwardDerivations, Map<Proposition, List<Proposition>> backwardDerivations, Map<UniqueId, Proposition> references, Set<Proposition> derivedPropositions) throws InvalidFactException;

    protected ObservationFact populateObxFact(Proposition prop,
                                                  Proposition encounterProp, PatientDimension patient,
                                                  VisitDimension visit, ProviderDimension provider, 
                                                  Concept concept, int instanceNum)
            throws InvalidFactException {
        Date start = handleStartDate(prop, encounterProp, null);
        Date finish = handleFinishDate(prop, encounterProp, null);
        Value value = handleValue(prop, concept);
        ValueFlagCode valueFlagCode = ValueFlagCode.NO_VALUE_FLAG;
        String units = handleUnits(prop);
        Date updateDate = prop.getUpdateDate();
        if (updateDate == null) {
            updateDate = prop.getCreateDate();
        }
        obx.setStartDate(start);
        obx.setEndDate(finish);
        obx.setPatient(patient);
        obx.setVisit(visit);
        obx.setProvider(provider);
        obx.setConcept(concept);
        obx.setValue(value);
        obx.setValueFlagCode(valueFlagCode);
        obx.setDisplayName(concept.getDisplayName());
        obx.setUnits(units);
        obx.setSourceSystem(prop.getSourceSystem().getStringRepresentation());
        obx.setRejected(start == null);
        obx.setDownloadDate(prop.getDownloadDate());
        obx.setUpdateDate(updateDate);
        obx.setInstanceNum(instanceNum);
        if (concept.isModifier()) {
            obx.setModifierCd(concept.getConceptCode());
        }
        concept.setInUse(true);
        return obx;
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

    protected final Value handleValue(Proposition prop, Concept concept) {
        Value value = null;
        if (prop != null) {
            if (this.propertyName != null) {
                Value tvalCharVal = prop.getProperty(this.propertyName);
                if (tvalCharVal != null) {
                    value = tvalCharVal;
                }
            } else if (concept.isModifier()) {
                value = prop.getProperty(concept.getId().getPropertyName());
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
            if (this.startConfig.equals("start")) {
                start = AbsoluteTimeGranularityUtil.asDate(((TemporalProposition) encounterProp).getInterval().getMinStart());
            } else if (this.startConfig.equals("finish")) {
                start = AbsoluteTimeGranularityUtil.asDate(((TemporalProposition) encounterProp).getInterval().getMinFinish());
            } else {
                start = null;
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
            if (this.finishConfig.equals("start")) {
                start = AbsoluteTimeGranularityUtil.asDate(((TemporalProposition) encounterProp).getInterval().getMinStart());
            } else if (this.finishConfig.equals("finish")) {
                start = AbsoluteTimeGranularityUtil.asDate(((TemporalProposition) encounterProp).getInterval().getMinFinish());
            } else {
                start = null;
            }
        } else {
            start = null;
        }
        return start;
    }

    @Override
    protected void setParameters(PreparedStatement ps, ObservationFact obx) throws SQLException {
        ps.setString(1, obx.getVisit().getEncryptedVisitId());
        ps.setString(2, obx.getVisit().getEncryptedVisitIdSourceSystem());
        ps.setString(3, obx.getConcept().getConceptCode());
        ps.setString(4, obx.getPatient().getEncryptedPatientId());
        ps.setString(5, obx.getPatient().getEncryptedPatientIdSourceSystem());
        ps.setString(6, TableUtil.setStringAttribute(obx.getProvider().getConcept().getConceptCode()));
        ps.setDate(7, TableUtil.setDateAttribute(obx.getStartDate()));
        ps.setString(8, obx.getModifierCd());
        ps.setLong(9, obx.getInstanceNum());

        Value value = obx.getValue();
        if (value == null) {
            ps.setString(10, ValTypeCode.NO_VALUE.getCode());
            ps.setString(11, null);
            ps.setString(12, null);
        } else if (value instanceof NumericalValue) {
            ps.setString(10, ValTypeCode.NUMERIC.getCode());
            if (value instanceof NumberValue) {
                ps.setString(11, TValCharWhenNumberCode.EQUAL.getCode());
            } else {
                InequalityNumberValue inv = (InequalityNumberValue) value;
                TValCharWhenNumberCode tvalCode =
                        TValCharWhenNumberCode.codeFor(inv.getComparator());
                ps.setString(11, tvalCode.getCode());
            }
            ps.setObject(12, ((NumericalValue) value).getNumber());
        } else {
            ps.setString(10, ValTypeCode.TEXT.getCode());
            String tval = value.getFormatted();
            if (tval.length() > 255) {
                ps.setString(11, tval.substring(0, 255));
                TableUtil.logger().log(Level.WARNING, "Truncated text result to 255 characters: " + tval);
            } else {
                ps.setString(11, tval);
            }
            ps.setString(12, null);
        }

        ps.setString(13, obx.getValueFlagCode().getCode());
        ps.setObject(14, null);
        ps.setObject(15, null);
        ps.setObject(16, null);
        ps.setString(17, obx.getUnits());
        ps.setDate(18, TableUtil.setDateAttribute(obx.getEndDate()));
        ps.setString(19, null);
        ps.setDate(20, null);
        ps.setDate(21, null);
        if (this.importTimestamp == null) {
            this.importTimestamp = new Timestamp(System.currentTimeMillis());
        }
        ps.setTimestamp(22, this.importTimestamp);
        ps.setString(23, obx.getSourceSystem());
        ps.setInt(24, 0);
    }
}
