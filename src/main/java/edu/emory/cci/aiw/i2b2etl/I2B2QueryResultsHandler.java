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
package edu.emory.cci.aiw.i2b2etl;

import edu.emory.cci.aiw.i2b2etl.table.InvalidFactException;
import edu.emory.cci.aiw.i2b2etl.table.FactHandler;
import edu.emory.cci.aiw.i2b2etl.configuration.*;
import edu.emory.cci.aiw.i2b2etl.configuration.ConceptsSection.FolderSpec;
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
import java.io.StringReader;
import java.sql.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.ArrayUtils;


import org.protempa.KnowledgeSource;
import org.protempa.KnowledgeSourceReadException;
import org.protempa.PropositionDefinition;
import org.protempa.ReferenceDefinition;
import org.protempa.proposition.Proposition;
import org.protempa.proposition.TemporalProposition;
import org.protempa.proposition.UniqueId;
import org.protempa.query.Query;
import org.protempa.query.handler.QueryResultsHandler;
import org.protempa.query.handler.QueryResultsHandlerInitException;
import org.protempa.query.handler.QueryResultsHandlerProcessingException;
import org.protempa.query.handler.table.Link;
import org.protempa.query.handler.table.Reference;

/**
 *
 * @author Andrew Post
 */
public final class I2B2QueryResultsHandler implements QueryResultsHandler {
    
    private static final long serialVersionUID = -1503401944818776787L;
    private final File confFile;
    private final boolean inferPropositionIdsNeeded;
    private KnowledgeSource knowledgeSource;
    private ConfigurationReader configurationReader;
    private Metadata ontologyModel;
    private Connection dataSchemaConnection;
    private List<FactHandler> factHandlers;
    private Query query;
    
    /**
     * Creates a new query results handler that will use the provided 
     * configuration file. It is the same as calling the two-argument
     * constructor with <code>inferPropositionIdsNeeded</code> set to
     * <code>true</code>.
     * 
     * @param confXML an i2b2 query results handler configuration file. Cannot
     * be <code>null</code>.
     */
    public I2B2QueryResultsHandler(File confXML) {
        this(confXML, true);
    }
    
    /**
     * Creates a new query results handler that will use the provided
     * configuration file. This constructor, through the 
     * <code>inferPropositionIdsNeeded</code> parameter, lets you control
     * whether proposition ids to be returned from the Protempa processing
     * run should be inferred from the i2b2 configuration file.
     * 
     * @param confXML an i2b2 query results handler configuration file. Cannot
     * be <code>null</code>.
     * @param inferPropositionIdsNeeded <code>true</code> if proposition ids
     * to be returned from the Protempa processing run should include all of
     * those specified in the i2b2 configuration file, <code>false</code> if
     * the proposition ids returned should be only those specified in the
     * Protempa {@link Query}.
     */
    public I2B2QueryResultsHandler(File confXML,
            boolean inferPropositionIdsNeeded) {
        if (confXML == null) {
            throw new IllegalArgumentException("confXML cannot be null");
        }
        
        Logger logger = I2b2ETLUtil.logger();
        this.confFile = confXML;
        logger.log(Level.FINE, String.format("Using configuration file: %s",
                this.confFile.getAbsolutePath()));
        this.inferPropositionIdsNeeded = inferPropositionIdsNeeded;
    }

    /**
     * Reads the configuration file passed into the constructor, in addition to
     * setting the knowledge source and query. This method is intended to be
     * called internally by Protempa.
     *
     * @param knowledgeSource the {@link KnowledgeSource}. Cannot be
     * <code>null</code>.
     * @param query the {@link Query}. Cannot be <code>null</code>.
     * @throws QueryResultsHandlerInitException if an error occurs reading the
     * configuration file.
     */
    @Override
    public void init(KnowledgeSource knowledgeSource, Query query)
            throws QueryResultsHandlerInitException {
        assert knowledgeSource != null : "knowledgeSource cannot be null";
        assert query != null : "query cannot be null";
        this.knowledgeSource = knowledgeSource;
        this.query = query;
        try {
            readConfiguration();
        } catch (ConfigurationReadException ex) {
            throw new QueryResultsHandlerInitException(
                    "Could not initialize query results handler for query "
                    + query.getId(), ex);
        }
    }

    /**
     * Builds most of the concept tree, truncates the data tables, opens a
     * connection to the i2b2 project database, and does some other prep. This
     * method is called before the first call to{@link #handleQueryResult()}.
     *
     * @throws QueryResultsHandlerProcessingException
     */
    @Override
    public void start() throws QueryResultsHandlerProcessingException {
        Logger logger = I2b2ETLUtil.logger();
        try {
            mostlyBuildOntology();
            truncateDataTables();
            this.dataSchemaConnection = openDatabaseConnection("dataschema");
            assembleFactHandlers();
            logger.log(Level.INFO,
                    "Populating observation facts table for query {0}",
                    this.query.getId());
        } catch (KnowledgeSourceReadException ex) {
            throw new QueryResultsHandlerProcessingException(
                    "Error during i2b2 load", ex);
        } catch (InstantiationException ex) {
            throw new QueryResultsHandlerProcessingException(
                    "Error during i2b2 load", ex);
        } catch (IllegalAccessException ex) {
            throw new QueryResultsHandlerProcessingException(
                    "Error during i2b2 load", ex);
        } catch (OntologyBuildException ex) {
            throw new QueryResultsHandlerProcessingException(
                    "Error during i2b2 load", ex);
        } catch (SQLException ex) {
            throw new QueryResultsHandlerProcessingException(
                    "Error during i2b2 load", ex);
        }
    }
    
    private void assembleFactHandlers() throws IllegalAccessException,
            InstantiationException, KnowledgeSourceReadException {
        this.factHandlers = new ArrayList<FactHandler>();
        DictionarySection dictSection =
                this.configurationReader.getDictionarySection();
        String visitPropId = dictSection.get("visitDimension");
        PropositionDefinition visitPropDef =
                this.knowledgeSource.readPropositionDefinition(visitPropId);
        DataSection dataSection = this.configurationReader.getDataSection();
        for (DataSection.DataSpec obx : dataSection.getAll()) {
            PropositionDefinition[] propDefs;
            Link[] links;
            if (obx.referenceName != null) {
                links = new Link[]{new Reference(obx.referenceName)};
                ReferenceDefinition refDef =
                        visitPropDef.referenceDefinition(obx.referenceName);
                String[] propIds = refDef.getPropositionIds();
                propDefs = new PropositionDefinition[propIds.length + 1];
                propDefs[0] = visitPropDef;
                for (int i = 1; i < propDefs.length; i++) {
                    propDefs[i] =
                            this.knowledgeSource.readPropositionDefinition(
                            propIds[i - 1]);
                    assert propDefs[i] != null : "Invalid proposition id "
                            + propIds[i - 1];
                }
            } else {
                links = null;
                propDefs = new PropositionDefinition[]{visitPropDef};
            }
            
            String[] potentialDerivedPropIdsArr =
                    this.ontologyModel.extractDerived(propDefs);
            
            FactHandler factHandler = new FactHandler(links, obx.propertyName,
                    obx.start, obx.finish, obx.units,
                    potentialDerivedPropIdsArr, this.ontologyModel);
            this.factHandlers.add(factHandler);
        }
    }
    
    @Override
    public void handleQueryResult(String keyId, List<Proposition> propositions,
            Map<Proposition, List<Proposition>> forwardDerivations,
            Map<Proposition, List<Proposition>> backwardDerivations,
            Map<UniqueId, Proposition> references)
            throws QueryResultsHandlerProcessingException {
        DictionarySection dictSection =
                this.configurationReader.getDictionarySection();
        String visitPropId = dictSection.get("visitDimension");
        try {
            Set<Proposition> derivedPropositions = new HashSet<Proposition>();
            List<Proposition> props = new ArrayList<Proposition>();
            for (Proposition prop : propositions) {
                if (prop.getId().equals(visitPropId)) {
                    props.add(prop);
                }
            }
            
            for (Proposition prop : props) {
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
                VisitDimension vd = this.ontologyModel.addVisit(
                        pd.getPatientNum(), pd.getEncryptedPatientId(),
                        pd.getEncryptedPatientIdSourceSystem(),
                        (TemporalProposition) prop,
                        this.configurationReader.getDictionarySection(),
                        this.configurationReader.getDataSection(),
                        references);
                for (FactHandler factHandler : this.factHandlers) {
                    factHandler.handleRecord(pd, vd, provider, prop,
                            forwardDerivations, backwardDerivations,
                            references, this.knowledgeSource,
                            derivedPropositions,
                            this.dataSchemaConnection);
                }
            }
        } catch (InvalidPatientRecordException ex) {
            throw new QueryResultsHandlerProcessingException(
                    "Load into i2b2 failed for query " + this.query.getId(),
                    ex);
        } catch (InvalidFactException ioe) {
            throw new QueryResultsHandlerProcessingException(
                    "Load into i2b2 failed for query " + this.query.getId(),
                    ioe);
        }
    }
    
    @Override
    public void finish() throws QueryResultsHandlerProcessingException {
        Logger logger = I2b2ETLUtil.logger();
        String queryId = this.query.getId();
        logger.log(Level.INFO,
                "Done populating observation facts table for query {0}",
                queryId);
        try {
            for (FactHandler factHandler : this.factHandlers) {
                factHandler.clearOut(this.dataSchemaConnection);
            }
            this.ontologyModel.buildProviderHierarchy();


            // persist Patients & Visits.
            logger.log(Level.INFO, "Populating dimensions for query {0}",
                    queryId);
            
            logger.log(Level.FINE,
                    "Populating patient dimension for query {0}", queryId);
            PatientDimension.insertAll(this.ontologyModel.getPatients(),
                    this.dataSchemaConnection);
            
            logger.log(Level.FINE,
                    "Populating visit dimension for query {0}", queryId);
            VisitDimension.insertAll(this.ontologyModel.getVisits(),
                    this.dataSchemaConnection);
            
            logger.log(Level.FINE,
                    "Inserting ages into observation fact table for query {0}",
                    queryId);
            PatientDimension.insertAges(this.ontologyModel.getPatients(),
                    this.dataSchemaConnection,
                    this.configurationReader.getDictionarySection().get("ageConceptCodePrefix"));

            // find Provider root. gather its leaf nodes. persist Providers.

            logger.log(Level.FINE,
                    "Populating provider dimension for query {0}", queryId);
            ProviderDimension.insertAll(this.ontologyModel.getProviders(),
                    this.dataSchemaConnection);
            logger.log(Level.FINE,
                    "Inserting providers into observation fact table for query {0}",
                    queryId);
            ProviderDimension.insertFacts(this.dataSchemaConnection);

            // flush hot concepts out of the tree. persist Concepts.

            logger.log(Level.FINE, "Populating concept dimension for query {0}",
                    this.query.getId());
            ConceptDimension.insertAll(this.ontologyModel.getRoot(), this.dataSchemaConnection);
            this.dataSchemaConnection.close();
            this.dataSchemaConnection = null;
            logger.log(Level.INFO, "Done populating dimensions for query {0}",
                    queryId);
            logger.log(Level.INFO,
                    "Done populating observation fact table for query {0}",
                    queryId);
            persistMetadata();
        } catch (OntologyBuildException ex) {
            throw new QueryResultsHandlerProcessingException(
                    "Load into i2b2 failed for query " + queryId, ex);
        } catch (InvalidConceptCodeException ex) {
            throw new QueryResultsHandlerProcessingException(
                    "Load into i2b2 failed for query " + queryId, ex);
        } catch (SQLException ex) {
            throw new QueryResultsHandlerProcessingException(
                    "Load into i2b2 failed for query " + queryId, ex);
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
    public void validate() {
    }
    
    private void readConfiguration() throws ConfigurationReadException {
        Logger logger = I2b2ETLUtil.logger();
        logger.log(Level.FINER, "STEP: read conf.xml");
        this.configurationReader = new ConfigurationReader(this.confFile);
        this.configurationReader.read();
    }
    
    private void mostlyBuildOntology() throws OntologyBuildException {
        DictionarySection dictionarySection =
                this.configurationReader.getDictionarySection();
        String rootNodeName = dictionarySection.get("rootNodeName");
        
        this.ontologyModel = new Metadata(this.knowledgeSource,
                collectUserPropositionDefinitions(),
                rootNodeName,
                this.configurationReader.getConceptsSection().getFolderSpecs(),
                this.configurationReader.getDictionarySection(),
                this.configurationReader.getDataSection());
    }
    
    private PropositionDefinition[] collectUserPropositionDefinitions() {
        PropositionDefinition[] allUserPropDefs = 
                this.query.getPropositionDefinitions();
        List<PropositionDefinition> result = 
                new ArrayList<PropositionDefinition>();
        Set<String> propIds = 
                org.arp.javautil.arrays.Arrays.asSet(
                this.query.getPropositionIds());
        for (PropositionDefinition propDef : allUserPropDefs) {
            if (propIds.contains(propDef.getId())) {
                result.add(propDef);
            }
        }
        return result.toArray(new PropositionDefinition[result.size()]);
    }
    
    private void truncateDataTables() throws SQLException {
        // Truncate the data tables
        // This is controlled by 'truncateTables' in conf.xml
        String truncateTables = this.configurationReader.getDictionarySection().get("truncateTables");
        if (truncateTables == null || truncateTables.equalsIgnoreCase("true")) {
            // To do: table names should be parameterized in conf.xml and related to other data
            String queryId = this.query.getId();
            Logger logger = I2b2ETLUtil.logger();
            logger.log(Level.INFO, "Truncating data tables for query {0}",
                    queryId);
            String[] dataschemaTables = {"OBSERVATION_FACT", "CONCEPT_DIMENSION", "PATIENT_DIMENSION", "PATIENT_MAPPING", "PROVIDER_DIMENSION", "VISIT_DIMENSION", "ENCOUNTER_MAPPING"};
            
            Connection conn = openDatabaseConnection("dataschema");
            try {
                for (String tableName : dataschemaTables) {
                    truncateTable(conn, tableName);
                }
                conn.close();
                conn = null;
                logger.log(Level.INFO,
                        "Done truncating data tables for query {0}", queryId);
            } finally {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException sqle) {
                    }
                }
            }
            
            logger.log(Level.INFO, "Truncating metadata tables for query {0}",
                    queryId);
            conn = openDatabaseConnection("metaschema");
            try {
                truncateTable(conn, this.configurationReader.getDictionarySection().get("metaTableName"));  // metaTableName in conf.xml
                conn.close();
                conn = null;
                logger.log(Level.INFO,
                        "Done truncating metadata tables for query {0}",
                        queryId);
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
    
    private void truncateTable(Connection conn, String tableName)
            throws SQLException {
        Logger logger = I2b2ETLUtil.logger();
        String queryId = this.query.getId();
        try {
            String sql = "TRUNCATE TABLE " + tableName;
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE,
                        "Executing the following SQL for query {0}: {1}",
                        new Object[]{queryId, sql});
            }
            Statement st = conn.createStatement();
            try {
                st.execute(sql);
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
            logger.log(Level.FINE, "Done executing SQL for query {0}",
                    queryId);
        } catch (SQLException ex) {
            logger.log(Level.SEVERE,
                    "An error occurred truncating the tables for query "
                    + queryId, ex);
            throw ex;
        }
    }
    
    public Connection openDatabaseConnection(String schema) throws SQLException {
        DatabaseSection.DatabaseSpec db =
                this.configurationReader.getDatabaseSection().get(schema);
        Logger logger = I2b2ETLUtil.logger();
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, 
                    "Connecting to {0} as user {1} for query {2}",
                    new Object[]{db.connect, db.user, this.query.getId()});
        }
        return DriverManager.getConnection(db.connect, db.user, db.passwd);
    }
    
    private void persistMetadata() throws SQLException {
        String queryId = this.query.getId();
        Logger logger = I2b2ETLUtil.logger();
        logger.log(Level.INFO, "Populating metadata tables for query {0}",
                queryId);
        Connection cn = openDatabaseConnection("metaschema");
        try {
            persistOntologyIntoI2B2Batch(this.ontologyModel, cn);
            cn.close();
            cn = null;
            logger.log(Level.INFO,
                    "Done populating metadata tables for query {0}", queryId);
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
            logger.log(Level.FINE, "batch inserting on table {0}", tableName);
            PreparedStatement ps;
            ps = cn.prepareStatement("insert into " + tableName + " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            try {
                @SuppressWarnings("unchecked")
                Enumeration<Concept> emu = model.getRoot().depthFirstEnumeration();
                /*
                 * A depth-first enumeration should traverse the hierarchies
                 * in the order in which they were created.
                 */
                Timestamp importTimestamp =
                        new Timestamp(System.currentTimeMillis());
                Set<String> conceptCodes = new HashSet<String>();
                while (emu.hasMoreElements()) {
                    
                    Concept concept = emu.nextElement();
                    
                    ps.setLong(1, concept.getLevel());
                    ps.setString(2, concept.getI2B2Path());
                    assert concept.getDisplayName() != null && concept.getDisplayName().length() > 0 : "concept " + concept.getConceptCode() + " (" + concept.getI2B2Path() + ") " + " has an invalid display name '" + concept.getDisplayName() + "'";
                    ps.setString(3, concept.getDisplayName());
                    String conceptCode = concept.getConceptCode();
                    if (conceptCodes.add(conceptCode)) {
                        ps.setString(4, SynonymCode.NOT_SYNONYM.getCode());
                    } else {
                        ps.setString(4, SynonymCode.SYNONYM.getCode());
                    }
                    ps.setString(5, concept.getCVisualAttributes());
                    ps.setObject(6, null);
                    
                    ps.setString(7, conceptCode);

                    // put labParmXml here
                    //
					if (null == concept.getMetadataXml() || concept
							.getMetadataXml().isEmpty() || concept
							.getMetadataXml().equals("")) {
						ps.setObject(8, null);
					} else {
						ps.setClob(8, new StringReader(concept.getMetadataXml
								()));
					}
                    
                    ps.setString(9, "concept_cd");
                    ps.setString(10, "concept_dimension");
                    ps.setString(11, "concept_path");
                    ps.setString(12, concept.getDataType().getCode());
                    ps.setString(13, concept.getOperator().getSQLOperator());
                    ps.setString(14, concept.getDimCode());
                    ps.setObject(15, null);
                    ps.setString(16, null);
                    ps.setTimestamp(17, importTimestamp);
                    ps.setDate(18, null);
                    ps.setTimestamp(19, importTimestamp);
                    ps.setString(20,
                            MetadataUtil.toSourceSystemCode(
                            concept.getSourceSystemCode()));
                    ps.setString(21, concept.getValueTypeCode().getCode());
                    
                    ps.addBatch();
                    
                    if ((++idx % 8192) == 0) {
                        importTimestamp =
                                new Timestamp(System.currentTimeMillis());
                        batchNumber++;
                        ps.executeBatch();
                        cn.commit();
                        idx = 0;
                        plus += 8192;
                        logBatch(tableName, batchNumber);
                        ps.clearBatch();
                        if (logger.isLoggable(Level.FINE)) {
                            logger.log(Level.FINE, "loaded ontology {0}:{1}",
                                    new Object[]{plus, minus});
                        }
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
            logger.log(Level.FINE, "TALLY_META_{0}_PM: {1}:{2}", new Object[]{tableName, plus, minus});
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
    
    @Override
    public String[] getPropositionIdsNeeded()
            throws KnowledgeSourceReadException {
        if (!this.inferPropositionIdsNeeded) {
            return ArrayUtils.EMPTY_STRING_ARRAY;
        } else {
            Set<String> result = new HashSet<String>();
            
            DictionarySection dictionarySection =
                    this.configurationReader.getDictionarySection();
            String visitPropId = dictionarySection.get("visitDimension");
            result.add(visitPropId);
            PropositionDefinition visitProp =
                    this.knowledgeSource.readPropositionDefinition(visitPropId);
            
            DataSection dataSection =
                    this.configurationReader.getDataSection();
            for (DataSpec dataSpec : dataSection.getAll()) {
                if (dataSpec.referenceName != null) {
                    ReferenceDefinition refDef =
                            visitProp.referenceDefinition(dataSpec.referenceName);
                    if (refDef == null) {
                        throw new KnowledgeSourceReadException(
                                "missing reference "
                                + dataSpec.referenceName
                                + " for proposition definition "
                                + visitPropId + " for query " 
                                + this.query.getId());
                    }
                    org.arp.javautil.arrays.Arrays.addAll(result,
                            refDef.getPropositionIds());
                }
                
            }
            
            ConceptsSection conceptsSection =
                    this.configurationReader.getConceptsSection();
            for (FolderSpec folderSpec : conceptsSection.getFolderSpecs()) {
                for (String proposition : folderSpec.propositions) {
                    result.add(proposition);
                }
            }
            
            for (PropositionDefinition pd :
                    this.query.getPropositionDefinitions()) {
                result.add(pd.getId());
            }
            
            return result.toArray(new String[result.size()]);
        }
    }
}
