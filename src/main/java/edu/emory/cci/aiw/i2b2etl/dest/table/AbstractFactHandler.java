package edu.emory.cci.aiw.i2b2etl.dest.table;

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

import edu.emory.cci.aiw.i2b2etl.dest.metadata.Concept;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.logging.Level;
import org.arp.javautil.sql.ConnectionSpec;
import org.protempa.proposition.value.InequalityNumberValue;
import org.protempa.proposition.value.NumberValue;
import org.protempa.proposition.value.NumericalValue;
import org.protempa.proposition.value.Value;

/**
 *
 * @author Andrew Post
 */
public abstract class AbstractFactHandler extends RecordHandler<ObservationFact> {
    private Timestamp importTimestamp;
    private final ObservationFact obx;

    AbstractFactHandler(ConnectionSpec connSpec, String statement) throws SQLException {
        super(connSpec, statement);
        this.obx = new ObservationFact();
    }

    ObservationFact getObx() {
        return obx;
    }

    Timestamp getImportTimestamp() {
        return importTimestamp;
    }
    
    @Override
    protected void setParameters(PreparedStatement ps, ObservationFact record) throws SQLException {
        assert obx != null : "obx cannot be null";
        assert obx.getVisit() != null : "obx.getVisit() cannot be null";
        ps.setString(1, obx.getVisit().getEncryptedVisitId());
        ps.setString(2, obx.getVisit().getEncryptedVisitIdSourceSystem());
        Concept concept = obx.getConcept();
        ps.setString(3, concept != null ? concept.getConceptCode() : null);
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
                TableUtil.logger().log(Level.WARNING, "Truncated text result to 255 characters: {0}", tval);
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
