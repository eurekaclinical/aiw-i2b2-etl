package edu.emory.cci.aiw.i2b2etl;

import edu.emory.cci.aiw.i2b2etl.table.InvalidFactException;
import edu.emory.cci.aiw.i2b2etl.table.FactHandler;
import edu.emory.cci.aiw.i2b2etl.configuration.*;
import edu.emory.cci.aiw.i2b2etl.metadata.Metadata;
import edu.emory.cci.aiw.i2b2etl.metadata.InvalidConceptCodeException;
import edu.emory.cci.aiw.i2b2etl.metadata.Concept;
import edu.emory.cci.aiw.i2b2etl.metadata.OntologyBuildException;
import edu.emory.cci.aiw.i2b2etl.configuration.DataSection.DataSpec;
import edu.emory.cci.aiw.i2b2etl.metadata.*;
import edu.emory.cci.aiw.i2b2etl.table.ConceptDimension;
import edu.emory.cci.aiw.i2b2etl.table.PatientDimension;
import edu.emory.cci.aiw.i2b2etl.table.ProviderDimension;
import edu.emory.cci.aiw.i2b2etl.table.VisitDimension;
import java.io.File;
import java.sql.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;


import org.protempa.FinderException;
import org.protempa.KnowledgeSource;
import org.protempa.proposition.Proposition;
import org.protempa.proposition.TemporalProposition;
import org.protempa.proposition.UniqueId;
import org.protempa.query.handler.QueryResultsHandler;
import org.protempa.query.handler.table.Link;
import org.protempa.query.handler.table.Reference;

/**
 *
 * @author Andrew Post
 */
public final class I2B2QueryResultsHandler implements QueryResultsHandler {

    private static final long serialVersionUID = -1503401944818776787L;
    private static final String PROTEMPA_DEFAULT_CONFIG = "erat-diagnoses-direct";
    private final File confFile;
    private KnowledgeSource knowledgeSource;
    private ConfigurationReader configurationReader;
    private Metadata ontologyModel;
    private Connection dataSchemaConnection;
    private List<FactHandler> factHandlers;

    public I2B2QueryResultsHandler(String confXML) {
        String protempaConfig = System.getProperty("i2b2.protempa.config", PROTEMPA_DEFAULT_CONFIG);
        Logger logger = I2b2ETLUtil.logger();
        logger.log(Level.INFO, String.format("Using protempa config: %s", protempaConfig));
        this.confFile = new File(confXML);
        logger.log(Level.INFO, String.format("Using conf.xml: %s", this.confFile.getAbsolutePath()));
    }

    @Override
    public void init(KnowledgeSource knowledgeSource) throws FinderException {
        Logger logger = I2b2ETLUtil.logger();
        logger.log(Level.INFO, "handler init. interpret ontology.");
        this.knowledgeSource = knowledgeSource;
        try {
            readConfiguration();
            mostlyBuildOntology();
            truncateDataTables();
            this.dataSchemaConnection = openDatabaseConnection("dataschema");
            assembleFactHandlers();
        } catch (InstantiationException ex) {
            throw new FinderException(ex);
        } catch (IllegalAccessException ex) {
            throw new FinderException(ex);
        } catch (ConfigurationReadException ex) {
            throw new FinderException(ex);
        } catch (OntologyBuildException ex) {
            throw new FinderException(ex);
        } catch (SQLException ex) {
            throw new FinderException(ex);
        }
        
        
    }

    private void assembleFactHandlers() throws IllegalAccessException, InstantiationException {
        this.factHandlers = new ArrayList<FactHandler>();
        for (DataSection.DataSpec obx : this.configurationReader.getDataSection().getAll()) {
            Link[] links;
            if (obx.referenceName != null) {
                links = new Link[]{new Reference(obx.referenceName)};
            } else {
                links = null;
            }
            FactHandler factHandler = new FactHandler(links, obx.propertyName, 
                    obx.start, obx.finish, obx.units, this.ontologyModel);
            this.factHandlers.add(factHandler);
        }
    }

    @Override
    public void handleQueryResult(String keyId, List<Proposition> propositions,
            Map<Proposition, List<Proposition>> forwardDerivations,
            Map<Proposition, List<Proposition>> backwardDerivations,
            Map<UniqueId, Proposition> references) throws FinderException {
        DictionarySection dictSection = this.configurationReader.getDictionarySection();
        String visitPropId = dictSection.get("visitDimension");
        try {
            Logger logger = I2b2ETLUtil.logger();
            logger.log(Level.FINE, "STEP: create and persist all Observations");
            for (Proposition prop : propositions) {
                if (prop.getId().equals(visitPropId)) {
                    DataSection obxSection =
                            this.configurationReader.getDataSection();
                    DataSpec providerFullNameSpec =
                            obxSection.get(dictSection.get("providerFullName"));
                    DataSpec providerFirstNameSpec =
                            obxSection.get(dictSection.get("providerFirstName"));
                    DataSpec providerMiddleNameSpec =
                            obxSection.get(dictSection.get("providerMiddleName"));
                    DataSpec providerLastNameSpec =
                            obxSection.get(dictSection.get("providerLastName"));
                    
                    ProviderDimension provider = 
                            this.ontologyModel.addProviderIfNeeded(prop, 
                            providerFullNameSpec.referenceName,
                            providerFullNameSpec.propertyName, 
                            providerFirstNameSpec.referenceName,
                            providerFirstNameSpec.propertyName,
                            providerMiddleNameSpec.referenceName,
                            providerMiddleNameSpec.propertyName,
                            providerLastNameSpec.referenceName,
                            providerLastNameSpec.propertyName,
                            references);
                    PatientDimension pd;
                    if ((pd = this.ontologyModel.getPatient(keyId)) == null) {
                        pd = this.ontologyModel.addPatient(keyId, prop,
                                this.configurationReader.getDictionarySection(),
                                this.configurationReader.getDataSection(),
                                references);
                    }
                    VisitDimension vd = this.ontologyModel.addVisit(pd.getMRN(), 
                            (TemporalProposition) prop,
                            this.configurationReader.getDictionarySection(),
                            this.configurationReader.getDataSection(),
                            references);
                    for (FactHandler factHandler : this.factHandlers) {
                        factHandler.handleRecord(pd, vd, provider, prop,
                                forwardDerivations, backwardDerivations,
                                references, this.knowledgeSource,
                                this.dataSchemaConnection);
                    }
                }
            }
        } catch (InvalidPatientRecordException ex) {
            throw new FinderException("Load into i2b2 failed", ex);
        } catch (InvalidFactException ioe) {
            throw new FinderException("Load into i2b2 failed", ioe);
        }
    }

    @Override
    public void finish() throws FinderException {
        
        try {
            Logger logger = I2b2ETLUtil.logger();
            for (FactHandler factHandler : this.factHandlers) {
                factHandler.clearOut(this.dataSchemaConnection);
            }
            this.ontologyModel.buildProviderHierarchy();
            
            
            // persist Patients & Visits.

            logger.log(Level.FINE, "STEP: persist all PatientDimension and VisitDimension");
            PatientDimension.insertAll(this.ontologyModel.getPatients(), this.dataSchemaConnection);

            VisitDimension.insertAll(this.ontologyModel.getVisits(), this.dataSchemaConnection);

            // find Provider root. gather its leaf nodes. persist Providers.

            logger.log(Level.FINE, "STEP: persist all ProviderDimension");
            ProviderDimension.insertAll(this.ontologyModel.getProviders(), this.dataSchemaConnection);
            ProviderDimension.insertFacts(this.dataSchemaConnection);

            // flush hot concepts out of the tree. persist Concepts.

            logger.log(Level.FINE, "STEP: persist all ConceptDimension");
            ConceptDimension.insertAll(this.ontologyModel.getRoot(), this.dataSchemaConnection);
            this.dataSchemaConnection.close();
            this.dataSchemaConnection = null;
            persistMetadata();
        } catch (OntologyBuildException ex) {
            throw new FinderException("Load into i2b2 failed", ex);
        } catch (InvalidConceptCodeException ex) {
            throw new FinderException("Load into i2b2 failed", ex);
        } catch (SQLException ex) {
            throw new FinderException("Load into i2b2 failed", ex);
        } finally {
            if (this.dataSchemaConnection != null) {
                try {
                    this.dataSchemaConnection.close();
                } catch (SQLException ex) {
                }
            }
        }
    }

    @Override
    public void validate(KnowledgeSource knowledgeSource) {
    }

    private void readConfiguration() throws ConfigurationReadException {
        Logger logger = I2b2ETLUtil.logger();
        logger.log(Level.FINE, "STEP: read conf.xml");
        this.configurationReader = new ConfigurationReader(this.confFile);
        this.configurationReader.read();
    }

    private void mostlyBuildOntology() throws OntologyBuildException {
        DictionarySection dictionarySection = this.configurationReader.getDictionarySection();
        String rootNodeName = dictionarySection.get("rootNodeName");
        String obsFact = dictionarySection.get("observationFact");
        boolean useBatchInsert = obsFact != null && obsFact.equals("batchInsert");
        this.ontologyModel = new Metadata(this.knowledgeSource,
                rootNodeName,
                useBatchInsert,
                this.configurationReader.getConceptsSection().getFolderSpecs(),
                this.configurationReader.getDictionarySection(),
                this.configurationReader.getDataSection());
    }

    private void truncateDataTables() throws SQLException {
        // Truncate the data tables
        // This is controlled by 'truncateTables' in conf.xml
        String truncateTables = this.configurationReader.getDictionarySection().get("truncateTables");
        if (truncateTables == null || truncateTables.equalsIgnoreCase("true")) {
            // To do: table names should be parameterized in conf.xml and related to other data
            String[] dataschemaTables = {"OBSERVATION_FACT", "CONCEPT_DIMENSION", "PATIENT_DIMENSION", "PROVIDER_DIMENSION", "VISIT_DIMENSION"};

            Connection conn = openDatabaseConnection("dataschema");
            try {
                for (String tableName : dataschemaTables) {
                    truncateTable(conn, tableName);
                }
                conn.close();
                conn = null;
            } finally {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException sqle) {
                    }
                }
            }

            conn = openDatabaseConnection("metaschema");
            try {
                truncateTable(conn, this.configurationReader.getDictionarySection().get("metaTableName"));  // metaTableName in conf.xml
                conn.close();
                conn = null;
            } finally {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException sqle) {
                    }
                }
            }
        }
    }

    private void truncateTable(Connection conn, String tableName) throws SQLException {
        Logger logger = I2b2ETLUtil.logger();
        try {
            String query = "TRUNCATE TABLE " + tableName;
            logger.info(query);
            Statement st = conn.createStatement();
            try {
                st.execute(query);
                st.close();
                st = null;
            } finally {
                if (st != null) {
                    try {
                        st.close();
                    } catch (SQLException sqle) {
                    }
                }
            }
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, "An error occurred truncating the tables", ex);
            throw ex;
        }
    }

    public Connection openDatabaseConnection(String schema) throws SQLException {
        DatabaseSection.DatabaseSpec db = this.configurationReader.getDatabaseSection().get(schema);
        Logger logger = I2b2ETLUtil.logger();
        logger.log(Level.INFO, "STEP: connecting to: {0} as user {1}", new Object[]{db.connect, db.user});
        return DriverManager.getConnection(db.connect, db.user, db.passwd);
    }

    private void persistMetadata() throws SQLException {
        //
        // metadata schema
        //

        // persist entire tree.
        Logger logger = I2b2ETLUtil.logger();
        logger.log(Level.FINE, "STEP: persist all metadata from ontology graph");
        Connection cn = openDatabaseConnection("metaschema");
        try {
            persistOntologyIntoI2B2Batch(this.ontologyModel, cn);
            cn.close();
            cn = null;
        } finally {
            if (cn != null) {
                try {
                    cn.close();
                } catch (SQLException sqle) {
                }
            }
        }
    }

    private void persistOntologyIntoI2B2Batch(Metadata model, Connection cn) throws SQLException {

        // CREATE TABLE "I2B2"
        // (
        // 1 "C_HLEVEL" NUMBER(22,0) NOT NULL ENABLE,
        // 2 "C_FULLNAME" VARCHAR2(700) NOT NULL ENABLE,
        // 3 "C_NAME" VARCHAR2(2000) NOT NULL ENABLE,
        // 4 "C_SYNONYM_CD" CHAR(1) NOT NULL ENABLE,
        // 5 "C_VISUALATTRIBUTES" CHAR(3) NOT NULL ENABLE,
        // 6 "C_TOTALNUM" NUMBER(22,0),
        // 7 "C_BASECODE" VARCHAR2(50),
        // 8 "C_METADATAXML" CLOB,
        // 9 "C_FACTTABLECOLUMN" VARCHAR2(50) NOT NULL ENABLE,
        // 10 "C_TABLENAME" VARCHAR2(50) NOT NULL ENABLE,
        // 11 "C_COLUMNNAME" VARCHAR2(50) NOT NULL ENABLE,
        // 12 "C_COLUMNDATATYPE" VARCHAR2(50) NOT NULL ENABLE,
        // 13 "C_OPERATOR" VARCHAR2(10) NOT NULL ENABLE,
        // 14 "C_DIMCODE" VARCHAR2(700) NOT NULL ENABLE,
        // 15 "C_COMMENT" CLOB,
        // 16 "C_TOOLTIP" VARCHAR2(900),
        // 17 "UPDATE_DATE" DATE NOT NULL ENABLE,
        // 18 "DOWNLOAD_DATE" DATE,
        // 19 "IMPORT_DATE" DATE,
        // 20 "SOURCESYSTEM_CD" VARCHAR2(50),
        // 21 "VALUETYPE_CD" VARCHAR2(50)
        // )


        int idx = 0;
        int plus = 0;
        int minus = 0;
        String tableName = this.configurationReader.getDictionarySection().get("metaTableName");
        int batchNumber = 0;
        Logger logger = I2b2ETLUtil.logger();
        try {
            logger.log(Level.INFO, "batch inserting on table {0}", tableName);
            PreparedStatement ps;
            ps = cn.prepareStatement("insert into " + tableName + " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            try {
                @SuppressWarnings("unchecked")
                Enumeration<Concept> emu = model.getRoot().depthFirstEnumeration();
                while (emu.hasMoreElements()) {

                    Concept concept = emu.nextElement();

                    ps.setLong(1, concept.getLevel());
                    ps.setString(2, concept.getI2B2Path());
                    assert concept.getDisplayName() != null && concept.getDisplayName().length() > 0 : "concept " + concept.getConceptCode() + " (" + concept.getI2B2Path() + ") " + " has an invalid display name '" + concept.getDisplayName() + "'";
                    ps.setString(3, concept.getDisplayName());
                    ps.setString(4, "N");
                    ps.setString(5, concept.getCVisualAttributes());
                    ps.setObject(6, null);

                    ps.setString(7, concept.getConceptCode());

                    // put labParmXml here
                    //
                    ps.setObject(8, null);

                    ps.setString(9, "concept_cd");
                    ps.setString(10, "concept_dimension");
                    ps.setString(11, "concept_path");
                    ps.setString(12, "T");
                    ps.setString(13, "LIKE");
                    ps.setString(14, concept.getI2B2Path());
                    ps.setObject(15, null);
                    ps.setString(16, null);
                    ps.setDate(17, new java.sql.Date(System.currentTimeMillis()));
                    ps.setDate(18, null);
                    ps.setDate(19, null);
                    ps.setString(20, concept.getSourceSystemCode());
                    ps.setString(21, null);

                    ps.addBatch();

                    if ((++idx % 8192) == 0) {
                        batchNumber++;
                        ps.executeBatch();
                        cn.commit();
                        idx = 0;
                        plus += 8192;
                        logBatch(tableName, batchNumber);
                        ps.clearBatch();
                        logger.log(Level.INFO, "loaded ontology {0}:{1}", new Object[]{plus, minus});
                    }
                }
                batchNumber++;
                ps.executeBatch();
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
            cn.commit();
            cn.close();
            cn = null;
            logBatch(tableName, batchNumber);
            logger.log(Level.INFO, "TALLY_META_{0}_PM: {1}:{2}", new Object[]{tableName, plus, minus});
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Batch failed on OntologyTable " + tableName + ". I2B2 will not be correct.", e);
            throw e;
        } finally {
            if (cn != null) {
                try {
                    cn.close();
                } catch (SQLException sqle) {
                }
            }
        }
    }

    private static void logBatch(String tableName, int batchNumber) {
        Logger logger = I2b2ETLUtil.logger();
        if (logger.isLoggable(Level.FINEST)) {
            Object[] args = new Object[]{tableName, batchNumber};
            logger.log(Level.FINEST, "DB_{0}_BATCH={1}", args);
        }
    }
}
