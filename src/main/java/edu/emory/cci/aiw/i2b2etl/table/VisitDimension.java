package edu.emory.cci.aiw.i2b2etl.table;

import edu.emory.cci.aiw.i2b2etl.metadata.Concept;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.builder.ToStringBuilder;

public class VisitDimension {

    //	there should be one instance for each visit.
    //	cached on encounter_num.
    //    	CREATE TABLE  "VISIT_DIMENSION" 
    // 	    (
    // 		"ENCOUNTER_NUM"		NUMBER(38,0) NOT NULL ENABLE, 
    // 		"PATIENT_NUM"		NUMBER(38,0) NOT NULL ENABLE, 
    // 		"ACTIVE_STATUS_CD"	VARCHAR2(50), 
    // 		"START_DATE"		DATE, 
    // 		"END_DATE"			DATE, 
    // 		"INOUT_CD"			VARCHAR2(50), 
    // 		"LOCATION_CD"		VARCHAR2(50), 
    // 		"LOCATION_PATH"		VARCHAR2(900), 
    // 		"VISIT_BLOB"		CLOB, 
    // 		"UPDATE_DATE"		DATE, 
    // 		"DOWNLOAD_DATE"		DATE, 
    // 		"IMPORT_DATE"		DATE, 
    // 		"SOURCESYSTEM_CD"	VARCHAR2(50), 
    // 		"UPLOAD_ID"			NUMBER(38,0), 
    // 		 CONSTRAINT "VISIT_DIMENSION_PK" PRIMARY KEY ("ENCOUNTER_NUM", "PATIENT_NUM") ENABLE
    // 	    )
    private final Long visitId;
    private final String decipheredVisitId;
    private final long mrn;
    private final Date startDate;
    private final Date endDate;
    private static final Logger logger = Logger.getLogger(VisitDimension.class.getName());
    private static long VISIT_ID = 0;

    public VisitDimension(long mrn,
            java.util.Date startDate, java.util.Date endDate,
            String decipheredVisitId) {
        this.visitId = VISIT_ID++;
        this.decipheredVisitId = TableUtil.setStringAttribute(decipheredVisitId);
        this.mrn = mrn;
        this.startDate = TableUtil.setDateAttribute(startDate);
        this.endDate = TableUtil.setDateAttribute(endDate);
    }
    
    public long getVisitId() {
        return this.visitId;
    }

    public static void insertAll(Collection<VisitDimension> visits, Connection cn) throws SQLException {

        PreparedStatement ps = null;
        try {

            ps = cn.prepareStatement("insert into VISIT_DIMENSION values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            for (VisitDimension visit : visits) {

                try {

                    ps.setLong(1, visit.visitId);
                    ps.setLong(2, visit.mrn);
                    ps.setString(3, null);
                    ps.setDate(4, visit.startDate);
                    ps.setDate(5, visit.endDate);
                    ps.setString(6, null);
                    ps.setString(7, null);
                    ps.setString(8, null);
                    ps.setObject(9, "deciphered encounterID=" + visit.decipheredVisitId);
                    ps.setDate(10, null);
                    ps.setDate(11, null);
                    ps.setDate(12, new java.sql.Date(System.currentTimeMillis()));
                    ps.setString(13, null);
                    ps.setObject(14, null);

                    ps.execute();
                    logger.log(Level.FINEST, "DB_VD_INSERT {0}", visit);
                } catch (SQLException e) {
                    logger.log(Level.SEVERE, "DB_VD_INSERT_FAIL {0}", visit);
                }
                ps.clearParameters();
            }
            ps.close();
            ps = null;
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                }
            }
        }
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
