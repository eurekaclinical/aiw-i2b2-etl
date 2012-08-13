package edu.emory.cci.aiw.i2b2etl.table;

import edu.emory.cci.aiw.i2b2etl.I2b2ETLUtil;
import edu.emory.cci.aiw.i2b2etl.metadata.Metadata;
import edu.emory.cci.aiw.i2b2etl.metadata.Concept;
import edu.emory.cci.aiw.i2b2etl.metadata.InvalidConceptCodeException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.protempa.KnowledgeSource;
import org.protempa.KnowledgeSourceReadException;
import org.protempa.proposition.*;
import org.protempa.proposition.value.*;
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
    private boolean useBatch = true;
    private Metadata ontologyModel;
    private PreparedStatement ps;
    private final String startConfig;
    private final String finishConfig;
    private final String unitsPropertyName;

    public FactHandler(Link[] links, String propertyName, String start, 
            String finish, String unitsPropertyName,
            Metadata ontologyModel) {
        if (ontologyModel == null) {
            throw new IllegalArgumentException("ontologyModel cannot be null");
        }
        this.ontologyModel = ontologyModel;
        useBatch = ontologyModel.isUseBatchObxInsert();
        this.linkTraverser = new LinkTraverser();
        this.links = links;
        this.propertyName = propertyName;
        this.startConfig = start;
        this.finishConfig = finish;
        this.unitsPropertyName = unitsPropertyName;
    }

    public void handleRecord(PatientDimension patient, VisitDimension visit,
            ProviderDimension provider,
            Proposition encounterProp,
            Map<Proposition, List<Proposition>> forwardDerivations,
            Map<Proposition, List<Proposition>> backwardDerivations,
            Map<UniqueId, Proposition> references,
            KnowledgeSource knowledgeSource, Connection cn) throws InvalidFactException {
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

        Logger logger = TableUtil.logger();
        for (Proposition prop : props) {
            Value propertyVal = this.propertyName != null ? prop.getProperty(this.propertyName) : null;
            Concept concept =
                    this.ontologyModel.getFromIdCache(prop.getId(),
                    this.propertyName, propertyVal);
            if (concept != null) {
                Date start = handleStartDate(prop, encounterProp, propertyVal);

                Date finish = handleFinishDate(prop, encounterProp, propertyVal);

                Value value = handleValue(prop);

                String observationBlob = null;
                
                ValueFlagCode valueFlagCode = ValueFlagCode.NO_VALUE;
                
                String units = handleUnits(prop);

                ObservationFact obx = new ObservationFact(start, finish, patient,
                        visit, provider, concept, value, valueFlagCode, observationBlob,
                        concept.getDisplayName(), units,
                        prop.getDataSourceType().getStringRepresentation());
                concept.setInUse(true);
                try {
                    insert(obx, cn);
                } catch (SQLException ex) {
                    String msg = "Observation fact not created for " + prop.getId() + "." + this.propertyName + "=" + propertyVal;
                    throw new InvalidFactException(msg, ex);
                } catch (InvalidConceptCodeException ex) {
                    String msg = "Observation fact not created for " + prop.getId() + "." + this.propertyName + "=" + propertyVal;
                    throw new InvalidFactException(msg, ex);
                }
            } else {
                logger.log(Level.FINE, "Discarded fact " + prop.getId() + "." + this.propertyName + "=" + propertyVal);
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
        if (start == null) {
            throw new InvalidFactException("Fact " + prop.getId() + ";" + this.propertyName + ";" + propertyVal + " does not have a time");
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
            if (!useBatch) {
                return;
            }
            if (ps != null) {
                batchNumber++;
                ps.executeBatch();
                logger.log(Level.FINEST, "DB_OBX_BATCH={0}", batchNumber);
                ps.close();
                ps = null;
            }

        } finally {
            logger.log(Level.INFO, "loaded obx {0}:{1}", new Object[]{plus, minus});
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException sqle) {
                }
            }
        }
    }

    private void insert(ObservationFact obx, Connection cn) throws SQLException, InvalidConceptCodeException {
        if (useBatch) {
            insertBatch(obx, cn);
        } else {
            insertNoBatch(obx, cn);
        }
    }

    private void insertBatch(ObservationFact obx, Connection cn) throws SQLException, InvalidConceptCodeException {
        Logger logger = TableUtil.logger();
        try {
            setParameters(cn, obx);

            ps.addBatch();

            if ((++idx % 8192) == 0) {
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

    private void insertNoBatch(ObservationFact obx, Connection cn) throws SQLException, InvalidConceptCodeException {
        Logger logger = TableUtil.logger();
        try {
            setParameters(cn, obx);

            ps.execute();
            plus++;
            logger.log(Level.FINEST, "DB_OBX_INSERT");
            ps.clearParameters();
            logger.log(Level.FINEST, "loaded obx {0}:{1}", new Object[]{plus, minus});
        } catch (SQLException e) {
            logger.log(Level.FINEST, "DB_OBX_INSERT_FAIL");
            logger.log(Level.SEVERE, "Insert failed on ObservationFact. I2B2 will not be correct.", e);
            try {
                ps.close();
            } catch (SQLException sqle) {
            }
            throw e;
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
        ps.setString(4, TableUtil.setStringAttribute(obx.getProvider().getId()));							//	seems coupled to 'reports'
        ps.setTimestamp(5, TableUtil.setTimestampAttribute(obx.getStartDate()));
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
        ps.setTimestamp(20, new java.sql.Timestamp(System.currentTimeMillis()));
        ps.setString(21, obx.getSourceSystem());
        ps.setObject(22, null);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
