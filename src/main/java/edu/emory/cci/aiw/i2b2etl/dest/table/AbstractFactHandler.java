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
import edu.emory.cci.aiw.i2b2etl.util.RecordHandler;
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

    AbstractFactHandler(ConnectionSpec connSpec, String statement) throws SQLException {
        super(connSpec, statement);
    }

    Timestamp getImportTimestamp() {
        return importTimestamp;
    }
    
    @Override
    protected void setParameters(PreparedStatement ps, ObservationFact record) throws SQLException {
        assert record != null : "record cannot be null";
        VisitDimension visit = record.getVisit();
        if (visit != null) {
            ps.setString(1, visit.getVisitId());
            ps.setString(2, visit.getVisitIdSource());
        } else {
            ps.setString(1, null);
            ps.setString(2, null);
        }
        Concept concept = record.getConcept();
        ps.setString(3, concept != null ? concept.getConceptCode() : null);
        ps.setString(4, record.getPatient().getEncryptedPatientId());
        ps.setString(5, record.getPatient().getEncryptedPatientIdSource());
        ps.setString(6, TableUtil.setStringAttribute(record.getProvider().getConcept().getConceptCode()));
        ps.setTimestamp(7, record.getStartDate());
        ps.setString(8, record.getModifierCd());
        ps.setLong(9, record.getInstanceNum());

        Value value = record.getValue();
        if (value == null) {
            ps.setString(10, ValTypeCode.NO_VALUE.getCode());
            ps.setString(11, null);
            ps.setObject(12, null);
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
            ps.setObject(12, null);
        }

        ps.setString(13, record.getValueFlagCode().getCode());
        ps.setObject(14, null);
        ps.setObject(15, null);
        ps.setObject(16, null);
        ps.setString(17, record.getUnits());
        ps.setTimestamp(18, record.getEndDate());
        ps.setString(19, null);
        ps.setTimestamp(20, null);
        ps.setTimestamp(21, record.getDownloadDate());
        if (this.importTimestamp == null) {
            this.importTimestamp = new Timestamp(System.currentTimeMillis());
        }
        ps.setTimestamp(22, this.importTimestamp);
        ps.setString(23, record.getSourceSystem());
        ps.setInt(24, 0);
        ps.setTimestamp(25, record.getDeletedDate());
    }
    
}
