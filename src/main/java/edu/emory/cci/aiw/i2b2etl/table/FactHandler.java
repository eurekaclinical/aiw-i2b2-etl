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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.protempa.KnowledgeSource;
import org.protempa.KnowledgeSourceReadException;
import org.protempa.proposition.*;
import org.protempa.proposition.value.*;
import org.protempa.query.handler.table.Derivation;
import org.protempa.query.handler.table.Link;
import org.protempa.query.handler.table.LinkTraverser;

public final class FactHandler {

    private final LinkTraverser linkTraverser;
    private final Link[] links;
    private final String propertyName;
    private int plus = 0;
    private int minus = 0;
    private boolean inited = false;
    private int batchNumber = 0;
    private long ctr = 0L;
    private int idx = 0;
    private Metadata metadata;
    private PreparedStatement ps;
    private final String startConfig;
    private final String finishConfig;
    private final String unitsPropertyName;
    private final Link[] derivationLinks;
    private Timestamp importTimestamp;

    public FactHandler(Link[] links, String propertyName, String start,
            String finish, String unitsPropertyName,
            String[] potentialDerivedPropIds, Metadata metadata) {
        if (metadata == null) {
            throw new IllegalArgumentException("metadata cannot be null");
        }
        this.metadata = metadata;
        this.linkTraverser = new LinkTraverser();
        this.links = links;
        this.propertyName = propertyName;
        this.startConfig = start;
        this.finishConfig = finish;
        this.unitsPropertyName = unitsPropertyName;
        if (potentialDerivedPropIds == null) {
            potentialDerivedPropIds = new String[0];
        } else {
            potentialDerivedPropIds = potentialDerivedPropIds.clone();
        }
        this.derivationLinks = new Link[]{
            new Derivation(potentialDerivedPropIds,
            Derivation.Behavior.MULT_FORWARD)
        };
    }

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

        for (Proposition prop : props) {
            Value propertyVal = this.propertyName != null
                    ? prop.getProperty(this.propertyName) : null;
            Concept concept =
                    this.metadata.getFromIdCache(prop.getId(),
                    this.propertyName, propertyVal);
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
                    for (Proposition derivedProp :
                            new HashSet<Proposition>(derivedProps)) {
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
                                } catch (SQLException sqle) {
                                    String msg = "Observation fact not created for " + prop.getId();
                                    throw new InvalidFactException(msg, sqle);
                                } catch (InvalidConceptCodeException ex) {
                                    String msg = "Observation fact not created for " + prop.getId();
                                    throw new InvalidFactException(msg, ex);
                                }
                            }
                        }
                    }
                } catch (SQLException ex) {
                    String msg = "Observation fact not created for " + prop.getId() + "." + this.propertyName + "=" + propertyVal;
                    throw new InvalidFactException(msg, ex);
                } catch (InvalidConceptCodeException ex) {
                    String msg = "Observation fact not created for " + prop.getId() + "." + this.propertyName + "=" + propertyVal;
                    throw new InvalidFactException(msg, ex);
                }
            }
        }
    }

    private String handleUnits(Proposition prop) {
        String value;
        if (this.unitsPropertyName != null) {
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

    private Value handleValue(Proposition prop) {
        Value value = null;
        if (this.propertyName != null) {
            Value tvalCharVal = prop.getProperty(this.propertyName);
            if (tvalCharVal != null) {
                value = tvalCharVal;
            }
        } else if (prop instanceof Parameter) {
            value = ((Parameter) prop).getValue();
        } else {
            value = NominalValue.getInstance(prop.getId());
        }
        return value;
    }

    private Date handleStartDate(Proposition prop, Proposition encounterProp, Value propertyVal) throws InvalidFactException {
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

    private Date handleFinishDate(Proposition prop, Proposition encounterProp, Value propertyVal) throws InvalidFactException {
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

    public void clearOut(Connection cn) throws SQLException {
        Logger logger = TableUtil.logger();
        try {
            if (ps != null) {
                batchNumber++;
                ps.executeBatch();
                logger.log(Level.FINEST, "DB_OBX_BATCH={0}", batchNumber);
                ps.close();
                ps = null;
            }

        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException sqle) {
                }
            }
        }
    }

    private void insert(ObservationFact obx, Connection cn) throws SQLException, InvalidConceptCodeException {
        Logger logger = TableUtil.logger();
        if (obx.isRejected()) {
            logger.log(Level.WARNING, "Rejected fact {0}", obx);
        } else {
            try {
                setParameters(cn, obx);

                ps.addBatch();

                if ((++idx % 8192) == 0) {
                    this.importTimestamp =
                            new Timestamp(System.currentTimeMillis());
                    batchNumber++;
                    ps.executeBatch();
                    logger.log(Level.FINEST, "DB_OBX_BATCH={0}", batchNumber);
                    ps.clearBatch();
                    plus += 8192;
                    idx = 0;
                    logger.log(Level.INFO, "loaded obx {0}:{1}", new Object[]{plus, minus});
                }
                ps.clearParameters();
            } catch (SQLException e) {
                logger.log(Level.FINEST, "DB_OBX_BATCH_FAIL={0}", batchNumber);
                logger.log(Level.SEVERE, "Batch failed on ObservationFact. I2B2 will not be correct.", e);
                logger.log(Level.INFO, "loaded obx {0}:{1}", new Object[]{plus, minus});
                try {
                    ps.close();
                } catch (SQLException sqle) {
                }
                throw e;
            }
        }
    }

    private void setParameters(Connection cn, ObservationFact obx) throws SQLException, InvalidConceptCodeException {
        if (!inited) {
            ps = cn.prepareStatement("insert into OBSERVATION_FACT values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            inited = true;
        }
        ps.setLong(1, obx.getVisit().getEncounterNum());
        ps.setLong(2, obx.getPatient().getPatientNum());
        ps.setString(3, obx.getConcept().getConceptCode());
        ps.setString(4,
                TableUtil.setStringAttribute(obx.getProvider().getId()));							//	seems coupled to 'reports'
        ps.setTimestamp(5,
                TableUtil.setTimestampAttribute(obx.getStartDate()));
        ps.setString(6, Long.toString(ctr++));								//	used for admitting, primary, secondary on ICD9Diag

        Value value = obx.getValue();
        if (value == null) {
            ps.setString(7, ValTypeCode.NO_VALUE.getCode());
            ps.setString(8, null);
            ps.setString(9, null);
        } else if (value instanceof NumericalValue) {
            ps.setString(7, ValTypeCode.NUMERIC.getCode());
            if (value instanceof NumberValue) {
                ps.setString(8, TValCharWhenNumberCode.EQUAL.getCode());
            } else {
                InequalityNumberValue inv = (InequalityNumberValue) value;
                TValCharWhenNumberCode tvalCode =
                        TValCharWhenNumberCode.codeFor(inv.getComparator());
                ps.setString(8, tvalCode.getCode());
            }
            ps.setObject(9, ((NumericalValue) value).getNumber());
        } else {
            ps.setString(7, ValTypeCode.TEXT.getCode());
            ps.setString(8, value.getFormatted());
            ps.setString(9, null);
        }
        ps.setString(10, obx.getValueFlagCode().getCode());
        ps.setObject(11, null);
        ps.setObject(12, null);
        ps.setString(13, obx.getUnits());
        ps.setDate(14, TableUtil.setDateAttribute(obx.getEndDate()));
        ps.setString(15, null);
        ps.setObject(16, null);
        ps.setObject(17, obx.getObservationBlob());
        ps.setTimestamp(18, null);
        ps.setTimestamp(19, null);
        if (this.importTimestamp == null) {
            this.importTimestamp = new Timestamp(System.currentTimeMillis());
        }
        ps.setTimestamp(20, this.importTimestamp);
        ps.setString(21, obx.getSourceSystem());
        ps.setObject(22, null);
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
                null, concept.getDisplayName(),
                units,
                prop.getDataSourceType().getStringRepresentation(),
                start == null);
        concept.setInUse(true);
        return derivedObx;
    }
}
