package edu.emory.cci.aiw.i2b2etl.table;

import java.sql.*;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * Represents records in the patient dimension.
 * 
 * The concept dimension has the following DDL:
 * <pre>
 * CREATE TABLE  "PATIENT_DIMENSION" 
 *    	(
 *    	"PATIENT_NUM" 		NUMBER(38,0) NOT NULL ENABLE, 
 *    	"VITAL_STATUS_CD" 	VARCHAR2(50), 
 *    	"BIRTH_DATE" 		DATE, 
 *    	"DEATH_DATE" 		DATE, 
 *    	"SEX_CD" 			VARCHAR2(50), 
 *    	"AGE_IN_YEARS_NUM" 	NUMBER(38,0), 
 *    	"LANGUAGE_CD" 		VARCHAR2(50), 
 *    	"RACE_CD" 			VARCHAR2(50), 
 *    	"MARITAL_STATUS_CD" VARCHAR2(50), 
 *    	"RELIGION_CD" 		VARCHAR2(50), 
 *    	"ZIP_CD" 			VARCHAR2(10), 
 *    	"STATECITYZIP_PATH" VARCHAR2(700), 
 *    	"PATIENT_BLOB" 		CLOB, 
 *    	"UPDATE_DATE" 		DATE, 
 *    	"DOWNLOAD_DATE" 	DATE, 
 *    	"IMPORT_DATE" 		DATE, 
 *    	"SOURCESYSTEM_CD" 	VARCHAR2(50), 
 *    	"UPLOAD_ID" 		NUMBER(38,0), 
 *    	CONSTRAINT "PATIENT_DIMENSION_PK" PRIMARY KEY ("PATIENT_NUM")
 *    	)
 * </pre>
 * 
 * @author Andrew Post
 */
public class PatientDimension {

    //	This is a related table that we should leverage.
    //
    //  CREATE TABLE PATIENT_MAPPING
    //  (
    //  "PATIENT_IDE"          VARCHAR2(200 BYTE) NOT NULL ENABLE,
    //  "PATIENT_IDE_SOURCE"   VARCHAR2(50 BYTE)  NOT NULL ENABLE,
    //  "PATIENT_NUM"          NUMBER(38,0)       NOT NULL ENABLE,
    //  "PATIENT_IDE_STATUS"   VARCHAR2(50 BYTE),
    //  "UPLOAD_DATE"          DATE,
    //  "UPDATE_DATE"          DATE,
    //  "DOWNLOAD_DATE"        DATE,
    //  "IMPORT_DATE"          DATE,
    //  "SOURCESYSTEM_CD"      VARCHAR2(50 BYTE),
    //  "UPLOAD_ID"            NUMBER(38,0),
    //  CONSTRAINT "PATIENT_MAPPING_PK" PRIMARY KEY ("PATIENT_IDE", "PATIENT_IDE_SOURCE")
    //  )
    private final long patientNum;
    private final String encryptedPatientId;
    private Integer ageInYears;
    private final String zip;
    private final String race;
    private final String gender;
    private final String language;
    private final String maritalStatus;
    private final String religion;
    private final VitalStatusCode vital;
    private final Date birthDate;
    private final Date deathDate;
    private final String sourceSystem;
    private static final NumFactory NUM_FACTORY = new IncrNumFactory();
    private static final Logger logger = Logger.getLogger(PatientDimension.class.getName());

    public PatientDimension(String encryptedPatientId, String zipCode, 
            Integer ageInYears,
            String gender, String language, String religion,
            java.util.Date birthDate, java.util.Date deathDate,
            String maritalStatus, String race, String sourceSystem) {
        //Required attributes
        this.patientNum = NUM_FACTORY.getInstance();
        this.zip = zipCode;
        this.birthDate = TableUtil.setDateAttribute(birthDate);
        this.deathDate = TableUtil.setDateAttribute(deathDate);
        this.vital = VitalStatusCode.getInstance(deathDate);
        
        //Optional attributes
        this.ageInYears = ageInYears;
        this.maritalStatus = maritalStatus;
        this.race = race;
        this.gender = gender;
        this.language = language;
        this.religion = religion;
        this.sourceSystem = sourceSystem;
        this.encryptedPatientId = encryptedPatientId;
    }
    
    public long getPatientNum() {
        return this.patientNum;
    }
    
    public String getEncryptedPatientId() {
        return this.encryptedPatientId;
    }
    
    public String getEncryptedPatientIdSourceSystem() {
        return NUM_FACTORY.getSourceSystem();
    }

    public static void insertAll(Collection<PatientDimension> patients, Connection cn) throws SQLException {
        PreparedStatement ps = null;
        PreparedStatement ps2 = null;
        try {
            ps = cn.prepareStatement("insert into PATIENT_DIMENSION values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            ps2 = cn.prepareStatement("insert into PATIENT_MAPPING values (?,?,?,?,?,?,?,?,?,?)");
            for (PatientDimension patient : patients) {
                try {
                    ps.setLong(1, patient.patientNum);
                    ps.setString(2, patient.vital.getCode());
                    ps.setDate(3, patient.birthDate);
                    ps.setDate(4, patient.deathDate);
                    ps.setString(5, patient.gender);
                    ps.setObject(6, patient.ageInYears);
                    ps.setString(7, patient.language);
                    ps.setString(8, patient.race);
                    ps.setString(9, patient.maritalStatus);
                    ps.setString(10, patient.religion);
                    ps.setString(11, patient.zip);
                    ps.setString(12, TableUtil.setStringAttribute(null));
                    ps.setObject(13, null);
                    ps.setTimestamp(14, null);
                    ps.setTimestamp(15, null);
                    ps.setTimestamp(16, new java.sql.Timestamp(System.currentTimeMillis()));
                    ps.setString(17, patient.sourceSystem);
                    ps.setObject(18, null);
                    ps.execute();
                    
                    ps2.setString(1, patient.encryptedPatientId);
                    ps2.setString(2, NUM_FACTORY.getSourceSystem());
                    ps2.setLong(3, patient.patientNum);
                    ps2.setString(4, null);
                    ps2.setDate(5, null);
                    ps2.setDate(6, null);
                    ps2.setDate(7, null);
                    ps2.setTimestamp(8, new java.sql.Timestamp(System.currentTimeMillis()));
                    ps2.setString(9, patient.sourceSystem);
                    ps2.setNull(10, Types.NUMERIC);
                    ps2.execute();
                    
                    logger.log(Level.FINEST, "DB_PD_INSERT {0}", patient);
                } catch (SQLException e) {
                    logger.log(Level.SEVERE, "DB_PD_INSERT_FAIL {0}", patient);
                    throw e;
                }
                ps.clearParameters();
                ps2.clearParameters();
            }
            ps.close();
            ps = null;
            ps2.close();
            ps2 = null;
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                }
            }
            if (ps2 != null) {
                try {
                    ps2.close();
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
