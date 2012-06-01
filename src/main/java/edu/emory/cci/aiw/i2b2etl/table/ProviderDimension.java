package edu.emory.cci.aiw.i2b2etl.table;

import java.sql.*;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * Represents records in the i2b2 provider dimension.
 * 
 * The provider dimension has the following DDL:
 * <pre>
 *   CREATE TABLE  "PROVIDER_DIMENSION" 
 *    	(
 *   	"PROVIDER_ID"		VARCHAR2(50) NOT NULL ENABLE, 
 *    	"PROVIDER_PATH"		VARCHAR2(700) NOT NULL ENABLE, 
 *    	"NAME_CHAR"			VARCHAR2(850), 
 *    	"PROVIDER_BLOB"		CLOB, 
 *    	"UPDATE_DATE"		DATE, 
 *    	"DOWNLOAD_DATE"		DATE, 
 *    	"IMPORT_DATE"		DATE, 
 *    	"SOURCESYSTEM_CD"	VARCHAR2(50), 
 *    	"UPLOAD_ID"			NUMBER(38,0), 
 *    	CONSTRAINT "PROVIDER_DIMENSION_PK" PRIMARY KEY ("PROVIDER_PATH", "PROVIDER_ID") ENABLE
 *    	)
 * </pre>
 * 
 * 
 * @author Andrew Post
 */
public class ProviderDimension {
    
    private final String id;
    private String i2b2Path;
    private final String fullName;
    private final String sourceSystem;
    
    /**
     * Constructs a provider dimension record. The provider path, which also
     * is in this dimension, is added later on with the 
     * {@link #setI2b2Path(java.lang.String) } after the ontology hierarchy is
     * created.
     * 
     * @param id the provider unique id, or <code>null</code> if the provider 
     * is not recorded or unknown.
     * @param firstName the provider's first name, if known.
     * @param middleName the provider's middle name, if known.
     * @param lastName the provider's last name, if known.
     * @param fullName the provider's full name, if known.
     */
    public ProviderDimension(String id, String fullName, String sourceSystem) {
        this.id = id;
        this.fullName = fullName;
        this.sourceSystem = sourceSystem;
    }
    
    /**
     * Returns the provider's unique id, or <code>null</code> if the provider 
     * is not recorded or unknown.
     * 
     * @return a {@link String}.
     */
    public String getId() {
        return this.id;
    }
    
    /**
     * Returns the provider's full name, or <code>null</code> if the provider 
     * is not recorded or unknown.
     * 
     * @return a {@link String}.
     */
    public String getFullName() {
        return this.fullName;
    }
    
    /**
     * Returns the source system of this provider, or <code>null</code> if it
     * is not recorded.
     * 
     * @return a {@link String}. 
     */
    public String getSourceSystem() {
        return this.sourceSystem;
    }
    
    public static void insertFacts(Connection dataSchemaConnection) throws SQLException {
        Statement stmt = dataSchemaConnection.createStatement();
        try {
            stmt.execute("INSERT INTO OBSERVATION_FACT (ENCOUNTER_NUM, PATIENT_NUM, CONCEPT_CD, PROVIDER_ID, START_DATE, END_DATE, MODIFIER_CD, IMPORT_DATE) SELECT DISTINCT a1.ENCOUNTER_NUM, a1.PATIENT_NUM, a1.PROVIDER_ID as CONCEPT_CD, a1.PROVIDER_ID, a2.START_DATE, a2.END_DATE, 0 as MODIFIER_CD, a1.IMPORT_DATE FROM OBSERVATION_FACT a1 JOIN VISIT_DIMENSION a2 on (a1.ENCOUNTER_NUM=a2.ENCOUNTER_NUM)");
            stmt.close();
            stmt = null;
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ignored) {
                    
                }
            }
        }
    }

    public static void insertAll(Collection<ProviderDimension> providers, 
            Connection cn) throws SQLException {
        Logger logger = TableUtil.logger();
        PreparedStatement ps = cn.prepareStatement("insert into PROVIDER_DIMENSION values (?,?,?,?,?,?,?,?,?)");
        try {
            for (ProviderDimension provider : providers) {
                if (provider.i2b2Path == null) {
                    throw new AssertionError("i2b2path cannot be null: " + provider);
                }
                try {
                    ps.setString(1, TableUtil.setStringAttribute(provider.id));
                    ps.setString(2, provider.i2b2Path);
                    ps.setString(3, provider.fullName);
                    ps.setObject(4, null);
                    ps.setTimestamp(5, null);
                    ps.setTimestamp(6, null);
                    ps.setTimestamp(7, new java.sql.Timestamp(System.currentTimeMillis()));
                    ps.setString(8, provider.sourceSystem);
                    ps.setObject(9, null);

                    ps.execute();
                    logger.log(Level.FINEST, "DB_RD_INSERT {0}", provider);
                } catch (SQLException e) {
                    logger.log(Level.SEVERE, "DB_RD_INSERT_FAIL {0}", provider);
                    throw e;
                }
                ps.clearParameters();
            }
            ps.close();
            ps = null;
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException sqle) {
                    
                }
            }
        }
    }

    /**
     * Sets the provider i2b2 path.
     * 
     * @param i2b2Path a {@link String} path separated by <code>/</code>.
     */
    public void setI2b2Path(String i2b2Path) {
        this.i2b2Path = i2b2Path;
    }
    
    /**
     * Returns the provider i2b2 path, or <code>null</code> if the provider 
     * is not recorded or unknown.
     * 
     * @return a {@link String}.
     */
    public String getI2b2Path() {
        return this.i2b2Path;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
    
    
}
