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

import edu.emory.cci.aiw.i2b2etl.configuration.Concepts;
import edu.emory.cci.aiw.i2b2etl.configuration.Configuration;
import edu.emory.cci.aiw.i2b2etl.configuration.ConfigurationReadException;
import edu.emory.cci.aiw.i2b2etl.configuration.Data;
import edu.emory.cci.aiw.i2b2etl.configuration.DataSpec;
import edu.emory.cci.aiw.i2b2etl.configuration.Database;
import edu.emory.cci.aiw.i2b2etl.configuration.DatabaseSpec;
import edu.emory.cci.aiw.i2b2etl.configuration.FolderSpec;
import edu.emory.cci.aiw.i2b2etl.configuration.Settings;
import edu.emory.cci.aiw.i2b2etl.metadata.Concept;
import edu.emory.cci.aiw.i2b2etl.metadata.InvalidConceptCodeException;
import edu.emory.cci.aiw.i2b2etl.table.InvalidPatientRecordException;
import edu.emory.cci.aiw.i2b2etl.metadata.Metadata;
import edu.emory.cci.aiw.i2b2etl.metadata.OntologyBuildException;
import edu.emory.cci.aiw.i2b2etl.table.ConceptDimensionHandler;
import edu.emory.cci.aiw.i2b2etl.table.ConceptDimensionLoader;
import edu.emory.cci.aiw.i2b2etl.table.EncounterMappingHandler;
import edu.emory.cci.aiw.i2b2etl.table.FactHandler;
import edu.emory.cci.aiw.i2b2etl.table.InvalidFactException;
import edu.emory.cci.aiw.i2b2etl.table.MetaTableConceptLoader;
import edu.emory.cci.aiw.i2b2etl.table.MetaTableConceptHandler;
import edu.emory.cci.aiw.i2b2etl.table.ModifierDimensionHandler;
import edu.emory.cci.aiw.i2b2etl.table.ModifierDimensionLoader;
import edu.emory.cci.aiw.i2b2etl.table.PatientDimension;
import edu.emory.cci.aiw.i2b2etl.table.PatientDimensionHandler;
import edu.emory.cci.aiw.i2b2etl.table.PatientDimensionFactory;
import edu.emory.cci.aiw.i2b2etl.table.PatientMappingHandler;
import edu.emory.cci.aiw.i2b2etl.table.PropositionFactHandler;
import edu.emory.cci.aiw.i2b2etl.table.ProviderDimension;
import edu.emory.cci.aiw.i2b2etl.table.ProviderDimensionHandler;
import edu.emory.cci.aiw.i2b2etl.table.ProviderDimensionFactory;
import edu.emory.cci.aiw.i2b2etl.table.VisitDimension;
import edu.emory.cci.aiw.i2b2etl.table.VisitDimensionHandler;
import edu.emory.cci.aiw.i2b2etl.table.VisitDimensionFactory;
import org.apache.commons.lang3.ArrayUtils;
import org.arp.javautil.sql.ConnectionSpec;
import org.arp.javautil.sql.DatabaseAPI;
import org.arp.javautil.sql.InvalidConnectionSpecArguments;
import org.protempa.KnowledgeSource;
import org.protempa.KnowledgeSourceReadException;
import org.protempa.PropositionDefinition;
import org.protempa.ReferenceDefinition;
import org.protempa.dest.AbstractQueryResultsHandler;
import org.protempa.dest.QueryResultsHandlerInitException;
import org.protempa.dest.QueryResultsHandlerProcessingException;
import org.protempa.dest.table.Link;
import org.protempa.dest.table.Reference;
import org.protempa.proposition.Proposition;
import org.protempa.proposition.TemporalProposition;
import org.protempa.proposition.UniqueId;
import org.protempa.query.Query;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.StringUtils;
import org.protempa.DataSource;
import org.protempa.backend.dsb.DataSourceBackend;
import org.protempa.backend.ksb.KnowledgeSourceBackend;

import org.protempa.dest.QueryResultsHandlerCloseException;

/**
 * @author Andrew Post
 */
public final class I2b2QueryResultsHandler extends AbstractQueryResultsHandler {
    private static final String[] OBX_FACT_IDXS = new String[]{"FACT_NOLOB", "FACT_PATCON_DATE_PRVD_IDX", "FACT_CNPT_PAT_ENCT_IDX"};

    private final Query query;
    private final KnowledgeSource knowledgeSource;
    private final I2b2Destination.DataInsertMode dataInsertMode;
    private final boolean inferPropositionIdsNeeded;
    private final Settings settings;
    private final Data data;
    private final Database database;
    private final ConnectionSpec dataConnectionSpec;
    private final Concepts conceptsSection;
    private List<FactHandler> factHandlers;
    private ConceptDimensionHandler conceptDimensionHandler;
    private ModifierDimensionHandler modifierDimensionHandler;
    private Metadata ontologyModel;
    private HashSet<Object> propIdsFromKnowledgeSource;
    private final DataSpec providerFullNameSpec;
    private final DataSpec providerFirstNameSpec;
    private final DataSpec providerMiddleNameSpec;
    private final DataSpec providerLastNameSpec;
    private final ConnectionSpec metadataConnectionSpec;
    private final String visitPropId;
    private boolean skipProviderHierarchy;
    private Connection dataSchemaConnection;
    private final Set<String> dataSourceBackendIds;
    private final RemoveMethod dataRemoveMethod;
    private RemoveMethod metaRemoveMethod;
    private final Set<String> knowledgeSourceBackendIds;
    private final String qrhId;
    private ProviderDimensionFactory providerDimensionFactory;
    private PatientDimensionFactory patientDimensionFactory;
    private VisitDimensionFactory visitDimensionFactory;
    private final Configuration configuration;

    /**
     * Creates a new query results handler that will use the provided
     * configuration file. This constructor, through the
     * <code>inferPropositionIdsNeeded</code> parameter, lets you control
     * whether proposition ids to be returned from the Protempa processing run
     * should be inferred from the i2b2 configuration file.
     *
     * @param confXML                   an i2b2 query results handler configuration file. Cannot
     *                                  be <code>null</code>.
     * @param inferPropositionIdsNeeded <code>true</code> if proposition ids to
     *                                  be returned from the Protempa processing run should include all of those
     *                                  specified in the i2b2 configuration file, <code>false</code> if the
     *                                  proposition ids returned should be only those specified in the Protempa
     *                                  {@link Query}.
     * @param dataInsertMode            whether to truncate existing data or append to it
     */
    I2b2QueryResultsHandler(Query query, DataSource dataSource, KnowledgeSource knowledgeSource, Configuration configuration,
                            boolean inferPropositionIdsNeeded, I2b2Destination.DataInsertMode dataInsertMode)
            throws QueryResultsHandlerInitException {
        Logger logger = I2b2ETLUtil.logger();
        this.query = query;
        this.knowledgeSource = knowledgeSource;
        this.dataInsertMode = dataInsertMode;
        this.configuration = configuration;
        try {
            this.configuration.init();
        } catch (ConfigurationReadException ex) {
            throw new QueryResultsHandlerInitException("Could not initialize query results handler", ex);
        }
        logger.log(Level.FINE, String.format("Using configuration: %s",
                this.configuration.getName()));
        this.inferPropositionIdsNeeded = inferPropositionIdsNeeded;
        logger.log(Level.FINER, "STEP: read conf.xml");
        this.settings = this.configuration.getSettings();

        this.data = this.configuration.getData();
        this.conceptsSection = this.configuration.getConcepts();
        this.database = this.configuration.getDatabase();
        DatabaseSpec dataSchemaSpec = this.database.getDataSpec();
        DatabaseSpec metadataSchemaSpec = this.database.getMetadataSpec();
        try {
            this.dataConnectionSpec = DatabaseAPI.DRIVERMANAGER.newConnectionSpecInstance(dataSchemaSpec.getConnect(), dataSchemaSpec.getUser(), dataSchemaSpec.getPasswd());
            this.metadataConnectionSpec = DatabaseAPI.DRIVERMANAGER.newConnectionSpecInstance(metadataSchemaSpec.getConnect(), metadataSchemaSpec.getUser(), metadataSchemaSpec.getPasswd());
        } catch (InvalidConnectionSpecArguments ex) {
            throw new QueryResultsHandlerInitException("Could not initialize query results handler", ex);
        }
        
        this.providerFullNameSpec = this.data.get(this.settings.getProviderFullName());
        this.providerFirstNameSpec = this.data.get(this.settings.getProviderFirstName());
        this.providerMiddleNameSpec = this.data.get(this.settings.getProviderMiddleName());
        this.providerLastNameSpec = this.data.get(this.settings.getProviderLastName());
        this.visitPropId = this.settings.getVisitDimension();
        if (this.inferPropositionIdsNeeded) {
            try {
                readPropIdsFromKnowledgeSource();
            } catch (KnowledgeSourceReadException ex) {
                throw new QueryResultsHandlerInitException("Could not initialize query results handler", ex);
            }
        }

        this.skipProviderHierarchy = this.settings.getSkipProviderHierarchy();
        this.dataRemoveMethod = this.settings.getDataRemoveMethod();
        this.metaRemoveMethod = this.settings.getMetaRemoveMethod();
        
        DataSourceBackend[] dsBackends = dataSource.getBackends();
        this.dataSourceBackendIds = new HashSet<>();
        for (int i = 0; i < dsBackends.length; i++) {
            String id = dsBackends[i].getId();
            if (id != null) {
                this.dataSourceBackendIds.add(id);
            }
        }
        this.qrhId = this.settings.getSourceSystemCode();
        this.dataSourceBackendIds.add(this.qrhId);

        KnowledgeSourceBackend[] ksBackends = knowledgeSource.getBackends();
        this.knowledgeSourceBackendIds = new HashSet<>();
        for (int i = 0; i < ksBackends.length; i++) {
            String id = ksBackends[i].getId();
            if (id != null) {
                this.knowledgeSourceBackendIds.add(id);
            }
        }
        this.knowledgeSourceBackendIds.add(this.qrhId);
        
        try {
            this.conceptDimensionHandler = new ConceptDimensionHandler(dataConnectionSpec);
        } catch (SQLException ex) {
            throw new QueryResultsHandlerInitException(ex);
        }
        
        try {
            this.modifierDimensionHandler = new ModifierDimensionHandler(dataConnectionSpec);
        } catch (SQLException ex) {
            throw new QueryResultsHandlerInitException(ex);
        }
    }

    /**
     * Builds most of the concept tree, truncates the data tables, opens a
     * connection to the i2b2 project database, and does some other prep. This
     * method is called before the first call to
     * {@link #handleQueryResult(String, java.util.List, java.util.Map, java.util.Map, java.util.Map)}.
     *
     * @throws QueryResultsHandlerProcessingException
     */
    @Override
    public void start() throws QueryResultsHandlerProcessingException {
        Logger logger = I2b2ETLUtil.logger();
        try {
            mostlyBuildOntology();
            this.providerDimensionFactory = new ProviderDimensionFactory(this.qrhId, this.ontologyModel, this.dataConnectionSpec, this.skipProviderHierarchy);
            this.patientDimensionFactory = new PatientDimensionFactory(this.ontologyModel, this.settings, this.data, this.dataConnectionSpec);
            this.visitDimensionFactory = new VisitDimensionFactory(this.qrhId, this.settings, this.data, this.dataConnectionSpec);
            if (this.dataInsertMode == I2b2Destination.DataInsertMode.TRUNCATE) {
                DataRemoverFactory f = new DataRemoverFactory();
                f.getInstance(this.dataRemoveMethod).doRemoveData();
                f.getInstance(this.metaRemoveMethod).doRemoveMetadata();
            }
            assembleFactHandlers();
            // disable indexes on observation_fact to speed up inserts
            disableObservationFactIndexes();
            // create i2b2 temporary tables using stored procedures
            dropTempTables();
            createTempTables();
            this.dataSchemaConnection = openDataDatabaseConnection();
            logger.log(Level.INFO, "Populating observation facts table for query {0}", this.query.getId());
        } catch (KnowledgeSourceReadException | SQLException | OntologyBuildException | IllegalAccessException | InstantiationException ex) {
            throw new QueryResultsHandlerProcessingException("Error during i2b2 load", ex);
        }
    }

    private String tempPatientTableName() {
        return PatientDimensionHandler.TEMP_PATIENT_TABLE;
    }

    private String tempPatientMappingTableName() {
        return PatientMappingHandler.TEMP_PATIENT_MAPPING_TABLE;
    }

    private String tempVisitTableName() {
        return VisitDimensionHandler.TEMP_VISIT_TABLE;
    }

    private String tempEncounterMappingTableName() {
        return EncounterMappingHandler.TEMP_ENC_MAPPING_TABLE;
    }

    private String tempProviderTableName() {
        return ProviderDimensionHandler.TEMP_PROVIDER_TABLE;
    }

    private String tempConceptTableName() {
        return ConceptDimensionHandler.TEMP_CONCEPT_TABLE;
    }

    private String tempObservationFactTableName() {
        return PropositionFactHandler.TEMP_OBSERVATION_TABLE;
    }
    
    private String tempModifierTableName() {
        return ModifierDimensionHandler.TEMP_MODIFIER_TABLE;
    }

    /**
     * Calls stored procedures in the i2b2 data schema to create temporary
     * tables for patient, visit, provider, and concept dimension, and for
     * observation fact tables. These are the tables that will be populated
     * by the query results handler. At that point, more i2b2 stored procedures
     * will be called to "upsert" the temporary data into the permanent tables.
     *
     * @throws SQLException if an error occurs while interacting with the database
     */
    private void createTempTables() throws SQLException {
        I2b2ETLUtil.logger().log(Level.INFO, "Creating temporary tables");
        try (final Connection conn = openDataDatabaseConnection()) {
            // for all of the procedures, the first parameter is an IN parameter
            // specifying the table name, and the second is an OUT paremeter
            // that contains an error message
            // create the patient table
            try (CallableStatement call = conn.prepareCall("{ call CREATE_TEMP_PATIENT_TABLE(?, ?) }")) {
                call.setString(1, tempPatientTableName());
                call.registerOutParameter(2, Types.VARCHAR);
                call.execute();
            }
            // create the patient mapping table
            try (CallableStatement call = conn.prepareCall("{ call CREATE_TEMP_PID_TABLE(?, ?) }")) {
                call.setString(1, tempPatientMappingTableName());
                call.registerOutParameter(2, Types.VARCHAR);
                call.execute();
            }
            // create the visit table
            try (CallableStatement call = conn.prepareCall("{ call CREATE_TEMP_VISIT_TABLE(?, ?) }")) {
                call.setString(1, tempVisitTableName());
                call.registerOutParameter(2, Types.VARCHAR);
                call.execute();
            }
            // create the encounter mapping table
            try (CallableStatement call = conn.prepareCall("{ call CREATE_TEMP_EID_TABLE(?, ?) }")) {
                call.setString(1, tempEncounterMappingTableName());
                call.registerOutParameter(2, Types.VARCHAR);
                call.execute();
            }
            // create the provider table
            try (CallableStatement call = conn.prepareCall("{ call CREATE_TEMP_PROVIDER_TABLE(?, ?) }")) {
                call.setString(1, tempProviderTableName());
                call.registerOutParameter(2, Types.VARCHAR);
                call.execute();
            }
            // create the concept table
            try (CallableStatement call = conn.prepareCall("{ call CREATE_TEMP_CONCEPT_TABLE(?, ?) }")) {
                call.setString(1, tempConceptTableName());
                call.registerOutParameter(2, Types.VARCHAR);
                call.execute();
            }
            // create the modifier table
            try (CallableStatement call = conn.prepareCall("{ call CREATE_TEMP_MODIFIER_TABLE(?, ?) }")) {
                call.setString(1, tempModifierTableName());
                call.registerOutParameter(2, Types.VARCHAR);
                call.execute();
            }
            // create the observation fact table
            try (CallableStatement call = conn.prepareCall("{ call CREATE_TEMP_TABLE(?, ?) }")) {
                call.setString(1, tempObservationFactTableName());
                call.registerOutParameter(2, Types.VARCHAR);
                call.execute();
            }

            I2b2ETLUtil.logger().log(Level.INFO, "Created temporary tables");
        }
    }

    /**
     * Calls stored procedures to drop all of the temp tables created.
     *
     * @throws SQLException if an error occurs while interacting with the database
     */
    private void dropTempTables() throws SQLException {
        try (final Connection conn = openDataDatabaseConnection();
             CallableStatement call = conn.prepareCall("{ call REMOVE_TEMP_TABLE(?) }")) {
            call.setString(1, tempPatientTableName());
            call.execute();

            call.setString(1, tempPatientMappingTableName());
            call.execute();

            call.setString(1, tempVisitTableName());
            call.execute();

            call.setString(1, tempEncounterMappingTableName());
            call.execute();

            call.setString(1, tempProviderTableName());
            call.execute();

            call.setString(1, tempConceptTableName());
            call.execute();
            
            call.setString(1, tempModifierTableName());
            call.execute();

            call.setString(1, tempObservationFactTableName());
            call.execute();
        }
    }

    private void assembleFactHandlers() throws IllegalAccessException, InstantiationException, KnowledgeSourceReadException, SQLException {
        this.factHandlers = new ArrayList<>();
        addPropositionFactHandlers();
    }

    @Override
    public void handleQueryResult(String keyId, List<Proposition> propositions, Map<Proposition, List<Proposition>> forwardDerivations, Map<Proposition, List<Proposition>> backwardDerivations, Map<UniqueId, Proposition> references) throws QueryResultsHandlerProcessingException {
        try {
            Set<Proposition> derivedPropositions = new HashSet<>();
            PatientDimension pd = null;
            for (Proposition prop : propositions) {
                if (prop.getId().equals(this.visitPropId)) {
                    if (pd == null) {
                        pd = this.patientDimensionFactory.getInstance(keyId, prop, references);
                    }
                    ProviderDimension providerDimension = this.providerDimensionFactory.getInstance(
                                    prop,
                                    this.providerFullNameSpec != null ? this.providerFullNameSpec.getReferenceName() : null,
                                    this.providerFullNameSpec != null ? this.providerFullNameSpec.getPropertyName() : null,
                                    this.providerFirstNameSpec != null ? this.providerFirstNameSpec.getReferenceName() : null,
                                    this.providerFirstNameSpec != null ? this.providerFirstNameSpec.getPropertyName() : null,
                                    this.providerMiddleNameSpec != null ? this.providerMiddleNameSpec.getReferenceName() : null,
                                    this.providerMiddleNameSpec != null ? this.providerMiddleNameSpec.getPropertyName() : null,
                                    this.providerLastNameSpec != null ? this.providerLastNameSpec.getReferenceName() : null,
                                    this.providerLastNameSpec != null ? this.providerLastNameSpec.getPropertyName() : null,
                                    references);
                    VisitDimension vd = this.visitDimensionFactory.getInstance(pd.getEncryptedPatientId(), pd.getEncryptedPatientIdSourceSystem(), (TemporalProposition) prop, references);
                    for (FactHandler factHandler : this.factHandlers) {
                        factHandler.handleRecord(pd, vd, providerDimension, prop, forwardDerivations, backwardDerivations, references, this.knowledgeSource, derivedPropositions, this.dataSchemaConnection);
                    }
                }
            }
        } catch (InvalidConceptCodeException | InvalidFactException | InvalidPatientRecordException | SQLException ex) {
            throw new QueryResultsHandlerProcessingException("Load into i2b2 failed for query " + this.query.getId(), ex);
        }
    }

    @Override
    public void finish() throws QueryResultsHandlerProcessingException {
        // upload_id for all the dimension table stored procedures
        final int UPLOAD_ID = 0;

        Logger logger = I2b2ETLUtil.logger();
        String queryId = this.query.getId();

        try {
            for (FactHandler factHandler : this.factHandlers) {
                factHandler.close();
            }

            logger.log(Level.INFO, "Inserting ages into observation fact table for query {0}", queryId);

            this.dataSchemaConnection.close();
            this.dataSchemaConnection = null;

            // persist Patients & Visits.
            this.patientDimensionFactory.close();
            this.patientDimensionFactory = null;
            logger.log(Level.INFO, "Populating dimensions for query {0}", queryId);
            logger.log(Level.INFO, "Populating patient dimension for query {0}", queryId);
            try (Connection conn = openDataDatabaseConnection()) {
                try (CallableStatement mappingCall = conn.prepareCall("{ call EUREKA.EK_INSERT_PID_MAP_FROMTEMP(?, ?, ?) }")) {
                    mappingCall.setString(1, tempPatientMappingTableName());
                    mappingCall.setInt(2, UPLOAD_ID);
                    mappingCall.registerOutParameter(3, Types.VARCHAR);
                    mappingCall.execute();
                    logger.log(Level.INFO, "EUREKA.EK_INSERT_PID_MAP_FROMTEMP errmsg: {0}", mappingCall.getString(3));
                    //commit and rollback are called by stored procedure.
                }
            }

            this.visitDimensionFactory.close();
            this.visitDimensionFactory = null;
            try (Connection conn = openDataDatabaseConnection()) {
                try (CallableStatement mappingCall = conn.prepareCall("{ call EUREKA.EK_INSERT_EID_MAP_FROMTEMP(?, ?, ?) }")) {
                    mappingCall.setString(1, tempEncounterMappingTableName());
                    mappingCall.setInt(2, UPLOAD_ID);
                    mappingCall.registerOutParameter(3, Types.VARCHAR);
                    mappingCall.execute();
                    logger.log(Level.INFO, "EUREKA.EK_INSERT_EID_MAP_FROMTEMP errmsg: {0}", mappingCall.getString(3));
                    //commit and rollback are called by the stored procedure.
                }
            }
            try (Connection conn = openDataDatabaseConnection()) {
                try (CallableStatement call = conn.prepareCall("{ call INSERT_PATIENT_FROMTEMP(?, ?, ?) }")) {
                    call.setString(1, tempPatientTableName());
                    call.setInt(2, UPLOAD_ID);
                    call.registerOutParameter(3, Types.VARCHAR);
                    call.execute();
                    logger.log(Level.INFO, "INSERT_PATIENT_FROMTEMP errmsg: {0}", call.getString(3));
                    conn.commit();
                } catch (SQLException ex) {
                    try {
                        conn.rollback();
                    } catch (SQLException ignore) {
                    }
                    throw ex;
                }
            }
            logger.log(Level.INFO, "Populating visit dimension for query {0}", queryId);

            try (Connection conn = openDataDatabaseConnection()) {
                try (CallableStatement call = conn.prepareCall("{ call INSERT_ENCOUNTERVISIT_FROMTEMP(?, ?, ?) }")) {
                    call.setString(1, tempVisitTableName());
                    call.setInt(2, UPLOAD_ID);
                    call.registerOutParameter(3, Types.VARCHAR);
                    call.execute();
                    logger.log(Level.INFO, "INSERT_ENCOUNTERVISIT_FROMTEMP errmsg: {0}", call.getString(3));
                    //commit and rollback are called by the stored procedure.
                }
            }

            // find Provider root. gather its leaf nodes. persist Providers.
            this.providerDimensionFactory.close();
            this.providerDimensionFactory = null;
            if (!this.skipProviderHierarchy) {
                logger.log(Level.INFO, "Populating provider dimension for query {0}", queryId);
                try (Connection conn = openDataDatabaseConnection()) {
                    try (CallableStatement call = conn.prepareCall("{ call INSERT_PROVIDER_FROMTEMP(?, ?, ?) }")) {
                        call.setString(1, tempProviderTableName());
                        call.setInt(2, UPLOAD_ID);
                        call.registerOutParameter(3, Types.VARCHAR);
                        call.execute();
                        logger.log(Level.INFO, "INSERT_PROVIDER_FROMTEMP errmsg: {0}", call.getString(3));
                        conn.commit();
                    } catch (SQLException ex) {
                        try {
                            conn.rollback();
                        } catch (SQLException ignore) {
                        }
                        throw ex;
                    }
                }
            }
            
            // flush hot concepts out of the tree. persist Concepts.
            logger.log(Level.INFO, "Populating concept dimension for query {0}", this.query.getId());
            new ConceptDimensionLoader(this.conceptDimensionHandler).execute(this.ontologyModel.getRoot());
            this.conceptDimensionHandler.close();
            this.conceptDimensionHandler = null;
            try (Connection conn = openDataDatabaseConnection()) {
                try (CallableStatement call = conn.prepareCall("{ call INSERT_CONCEPT_FROMTEMP(?, ?, ?) }")) {
                    call.setString(1, tempConceptTableName());
                    call.setInt(2, UPLOAD_ID);
                    call.registerOutParameter(3, Types.VARCHAR);
                    call.execute();
                    logger.log(Level.INFO, "INSERT_CONCEPT_FROMTEMP errmsg: {0}", call.getString(3));
                    conn.commit();
                } catch (SQLException ex) {
                    try {
                        conn.rollback();
                    } catch (SQLException ignore) {
                    }
                    throw ex;
                }
            }
            
            logger.log(Level.INFO, "Populating modifier dimension for query {0}", this.query.getId());
            new ModifierDimensionLoader(this.modifierDimensionHandler).execute(this.ontologyModel.getRoot());
            this.modifierDimensionHandler.close();
            this.modifierDimensionHandler = null;
            try (Connection conn = openDataDatabaseConnection()) {
                try (CallableStatement call = conn.prepareCall("{ call INSERT_MODIFIER_FROMTEMP(?, ?, ?) }")) {
                    call.setString(1, tempModifierTableName());
                    call.setInt(2, UPLOAD_ID);
                    call.registerOutParameter(3, Types.VARCHAR);
                    call.execute();
                    logger.log(Level.INFO, "INSERT_MODIFIER_FROMTEMP errmsg: {0}", call.getString(3));
                    conn.commit();
                } catch (SQLException ex) {
                    try {
                        conn.rollback();
                    } catch (SQLException ignore) {
                    }
                    throw ex;
                }
            }
            
            logger.log(Level.INFO, "Done populating dimensions for query {0}", queryId);

            try (Connection conn = openDataDatabaseConnection()) {
                logger.log(Level.INFO, "Populating observation_fact from temporary table");
                try (CallableStatement call = conn.prepareCall("{ call EUREKA.EK_UPDATE_OBSERVATION_FACT(?, ?, ?, ?) }")) {
                    call.setString(1, tempObservationFactTableName());
                    call.setLong(2, UPLOAD_ID);
                    call.setLong(3, 1); // appendFlag
                    call.registerOutParameter(4, Types.VARCHAR);
                    call.execute();
                    logger.log(Level.INFO, "EUREKA.EK_UPDATE_OBSERVATION_FACT errmsg: {0}", call.getString(4));
                    //commit and rollback are called by the stored procedure.
                }
            }

            // re-enable the indexes now that we're done populating the table
            enableObservationFactIndexes();
            gatherStatisticsOnObservationFact();
            logger.log(Level.INFO, "Done populating observation fact table for query {0}", queryId);
            persistMetadata();
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, "Load into i2b2 failed for query " + queryId, ex);
            throw new QueryResultsHandlerProcessingException("Load into i2b2 failed for query " + queryId, ex);
        }
    }

    @Override
    public void close() throws QueryResultsHandlerCloseException {
        if (this.visitDimensionFactory != null) {
            try {
                this.visitDimensionFactory.close();
            } catch (SQLException ignore) {
            }
        }
        if (this.patientDimensionFactory != null) {
            try {
                this.patientDimensionFactory.close();
            } catch (SQLException ignore) {
            }
        }
        if (this.conceptDimensionHandler != null) {
            try {
                this.conceptDimensionHandler.close();
            } catch (SQLException ignore) {
            }
        }
        if (this.modifierDimensionHandler != null) {
            try {
                this.modifierDimensionHandler.close();
            } catch (SQLException ignore) {
            }
        }
        if (this.dataSchemaConnection != null) {
            try {
                this.dataSchemaConnection.close();
            } catch (SQLException ignore) {
            }
        }
    }

    @Override
    public String[] getPropositionIdsNeeded() {
        if (!inferPropositionIdsNeeded) {
            return ArrayUtils.EMPTY_STRING_ARRAY;
        } else {
            for (PropositionDefinition pd : this.query.getPropositionDefinitions()) {
                this.propIdsFromKnowledgeSource.add(pd.getId());
            }
            return this.propIdsFromKnowledgeSource.toArray(new String[this.propIdsFromKnowledgeSource.size()]);
        }
    }

    private void mostlyBuildOntology() throws OntologyBuildException {
        this.ontologyModel = new Metadata(this.qrhId, knowledgeSource, collectUserPropositionDefinitions(), this.settings.getRootNodeName(), this.conceptsSection.getFolderSpecs(), settings, this.data, this.skipProviderHierarchy);
        setI2B2PathsToConcepts();
    }

    private void setI2B2PathsToConcepts() {
        Enumeration<Concept> emu = this.ontologyModel.getRoot().breadthFirstEnumeration();
        while (emu.hasMoreElements()) {
            Concept concept = emu.nextElement();
            Concept conceptFromCache = this.ontologyModel.getFromIdCache(concept.getId());
            if (conceptFromCache != null) {
                conceptFromCache.setHierarchyPath(concept.getFullName());
            }
        }
    }


    private PropositionDefinition[] collectUserPropositionDefinitions() {
        PropositionDefinition[] allUserPropDefs = this.query.getPropositionDefinitions();
        List<PropositionDefinition> result = new ArrayList<>();
        Set<String> propIds = org.arp.javautil.arrays.Arrays.asSet(this.query.getPropositionIds());
        for (PropositionDefinition propDef : allUserPropDefs) {
            if (propIds.contains(propDef.getId())) {
                result.add(propDef);
            }
        }
        return result.toArray(new PropositionDefinition[result.size()]);
    }

    private void readPropIdsFromKnowledgeSource() throws KnowledgeSourceReadException {
        this.propIdsFromKnowledgeSource = new HashSet<>();
        this.propIdsFromKnowledgeSource.add(this.visitPropId);
        PropositionDefinition visitProp = this.knowledgeSource.readPropositionDefinition(this.visitPropId);
        for (DataSpec dataSpec : this.data.getAll()) {
            if (dataSpec.getReferenceName() != null) {
                ReferenceDefinition refDef = visitProp.referenceDefinition(dataSpec.getReferenceName());
                if (refDef == null) {
                    throw new KnowledgeSourceReadException("missing reference " + dataSpec.getReferenceName() + " for proposition definition " + visitPropId + " for query " + this.query.getId());
                }
                org.arp.javautil.arrays.Arrays.addAll(this.propIdsFromKnowledgeSource, refDef.getPropositionIds());
            }
        }
        for (FolderSpec folderSpec : this.conceptsSection.getFolderSpecs()) {
            for (String proposition : folderSpec.getPropositions()) {
                this.propIdsFromKnowledgeSource.add(proposition);
            }
        }
    }

    private void addPropositionFactHandlers() throws KnowledgeSourceReadException, SQLException {
        String[] potentialDerivedPropIdsArr = this.ontologyModel.extractDerived();
        Set<String> patDimDataTypes = this.settings.getPatientDimensionDataTypes();
        for (DataSpec obx : this.data.getAll()) {
            if (!patDimDataTypes.contains(obx.getKey())) {
                Link[] links;
                if (obx.getReferenceName() != null) {
                    links = new Link[]{new Reference(obx.getReferenceName())};
                } else {
                    links = null;
                }

                PropositionFactHandler propFactHandler =
                        new PropositionFactHandler(this.dataConnectionSpec, links, obx.getPropertyName(),
                                obx.getStart(), obx.getFinish(), obx.getUnits(),
                                potentialDerivedPropIdsArr, this.ontologyModel);
                this.factHandlers.add(propFactHandler);
            }
        }
    }

    private void persistMetadata() throws SQLException {
        String queryId = this.query.getId();
        Logger logger = I2b2ETLUtil.logger();
        logger.log(Level.INFO, "Populating metadata tables for query {0}", queryId);
        persistOntologyIntoI2B2Batch(this.ontologyModel);
        logger.log(Level.INFO, "Done populating metadata tables for query {0}", queryId);
    }

    private void persistOntologyIntoI2B2Batch(Metadata model) throws SQLException {
        String tableName = this.settings.getMetaTableName();
        MetaTableConceptHandler metaTableHandler = new MetaTableConceptHandler(this.metadataConnectionSpec, tableName);
        MetaTableConceptLoader metaTableConceptLoader = new MetaTableConceptLoader(metaTableHandler);
        metaTableConceptLoader.execute(model.getRoot());
    }
    
    private abstract class DataRemover {
        abstract void doRemoveData() throws SQLException;
        abstract void doRemoveMetadata() throws SQLException;
    }
    
    private class DataRemoverFactory {
        DataRemover getInstance(RemoveMethod removeMethod) {
            switch (removeMethod) {
                case TRUNCATE:
                    return new TableTruncater();
                case DELETE:
                    return new TableDeleter();
                default:
                    throw new AssertionError("Unexpected remove method " + removeMethod);
            }
        }
    }

    private class TableTruncater extends DataRemover {

        @Override
        void doRemoveData() throws SQLException {
            // Truncate the data tables
            // To do: table names should be parameterized in conf.xml and related to other data
            String queryId = query.getId();
            Logger logger = I2b2ETLUtil.logger();
            logger.log(Level.INFO, "Truncating data tables for query {0}", queryId);
            String[] dataschemaTables = {"OBSERVATION_FACT", "CONCEPT_DIMENSION", "PATIENT_DIMENSION", "PATIENT_MAPPING", "PROVIDER_DIMENSION", "VISIT_DIMENSION", "ENCOUNTER_MAPPING", "MODIFIER_DIMENSION"};
            try (final Connection conn = openDataDatabaseConnection()) {
                for (String tableName : dataschemaTables) {
                    truncateTable(conn, tableName);
                }
                logger.log(Level.INFO, "Done truncating data tables for query {0}", queryId);
            }
        }
        
        @Override
        void doRemoveMetadata() throws SQLException {
            // Truncate the data tables
            // To do: table names should be parameterized in conf.xml and related to other data
            String queryId = query.getId();
            Logger logger = I2b2ETLUtil.logger();
            logger.log(Level.INFO, "Truncating metadata tables for query {0}", queryId);
            try (final Connection conn = openMetadataDatabaseConnection()) {
                truncateTable(conn, settings.getMetaTableName()); // metaTableName in conf.xml
                logger.log(Level.INFO, "Done truncating metadata tables for query {0}", queryId);
            }
        }

        private void truncateTable(Connection conn, String tableName) throws SQLException {
            Logger logger = I2b2ETLUtil.logger();
            String queryId = query.getId();
            String sql = "TRUNCATE TABLE " + tableName;
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, "Executing the following SQL for query {0}: {1}", new Object[]{queryId, sql});
            }
            try (final Statement st = conn.createStatement()) {
                st.execute(sql);
                logger.log(Level.FINE, "Done executing SQL for query {0}", queryId);
            } catch (SQLException ex) {
                logger.log(Level.SEVERE, "An error occurred truncating the tables for query " + queryId, ex);
                throw ex;
            }
        }
    }
    
    private class TableDeleter extends DataRemover {

        @Override
        void doRemoveData() throws SQLException {
            String queryId = query.getId();
            Logger logger = I2b2ETLUtil.logger();
            logger.log(Level.INFO, "Deleting data tables for query {0}", queryId);
            String[] dataschemaTables = {"OBSERVATION_FACT", "CONCEPT_DIMENSION", "PATIENT_DIMENSION", "PATIENT_MAPPING", "PROVIDER_DIMENSION", "VISIT_DIMENSION", "ENCOUNTER_MAPPING", "MODIFIER_DIMENSION"};
            try (final Connection conn = openDataDatabaseConnection()) {
                for (String tableName : dataschemaTables) {
                    deleteTable(conn, tableName, dataSourceBackendIds);
                }
                logger.log(Level.INFO, "Done deleting data for query {0}", queryId);
            }
        }
        
        @Override
        void doRemoveMetadata() throws SQLException {
            String queryId = query.getId();
            Logger logger = I2b2ETLUtil.logger();
            logger.log(Level.INFO, "Deleting metadata for query {0}", queryId);
            try (final Connection conn = openMetadataDatabaseConnection()) {
                deleteTable(conn, settings.getMetaTableName(), knowledgeSourceBackendIds); // metaTableName in conf.xml
                logger.log(Level.INFO, "Done deleting metadata for query {0}", queryId);
            }
        }

        private void deleteTable(Connection conn, String tableName, Set<String> sourceSystemCodes) throws SQLException {
            Logger logger = I2b2ETLUtil.logger();
            String queryId = query.getId();
            String sql = "DELETE FROM " + tableName;
            if (sourceSystemCodes != null && !sourceSystemCodes.isEmpty()) {
                sql += " WHERE SOURCESYSTEM_CD IN ('" + StringUtils.join(sourceSystemCodes, "','") + "')";
            }
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, "Executing the following SQL for query {0}: {1}", new Object[]{queryId, sql});
            }
            try (final Statement st = conn.createStatement()) {
                st.execute(sql);
                logger.log(Level.FINE, "Done executing SQL for query {0}", queryId);
            } catch (SQLException ex) {
                logger.log(Level.SEVERE, "An error occurred deleting for query " + queryId, ex);
                throw ex;
            }
        }
    }

    private void disableObservationFactIndexes() throws SQLException {
        Logger logger = I2b2ETLUtil.logger();
        logger.log(Level.INFO, "Disabling indices on observation_fact");
        try (Connection conn = openDataDatabaseConnection();
             Statement stmt = conn.createStatement()) {
            for (String idx : OBX_FACT_IDXS) {
                if (logger.isLoggable(Level.FINE)) {
                    logger.log(Level.FINE, "Disabling index: {0}", idx);
                }
                stmt.executeUpdate("ALTER INDEX " + idx + " UNUSABLE");
            }
            stmt.executeUpdate("ALTER SESSION SET skip_unusable_indexes = true");
            logger.log(Level.INFO, "Disabled indices on observation_fact");
        }
    }

    private void enableObservationFactIndexes() throws SQLException {
        Logger logger = I2b2ETLUtil.logger();
        logger.log(Level.INFO, "Enabling indices on observation_fact");
        try (Connection conn = openDataDatabaseConnection();
             Statement stmt = conn.createStatement()) {
            for (String idx : OBX_FACT_IDXS) {
                if (logger.isLoggable(Level.FINE)) {
                    logger.log(Level.FINE, "Enabling index: {0}", idx);
                }
                stmt.executeUpdate("ALTER INDEX " + idx + " REBUILD");
            }
            stmt.executeUpdate("ALTER SESSION SET skip_unusable_indexes = false");
            logger.log(Level.INFO, "Enabled indices on observation_fact");
        }
    }

    private void gatherStatisticsOnObservationFact() throws SQLException {
        Logger logger = I2b2ETLUtil.logger();
        logger.log(Level.INFO, "Gathering statistics on observation_fact");
        try (Connection conn = openDataDatabaseConnection();
             CallableStatement stmt = conn.prepareCall("{call dbms_stats.gather_table_stats(?, ?)}")) {
            stmt.setString(1, this.database.getDataSpec().getUser());
            stmt.setString(2, "observation_fact");
            stmt.execute();
            logger.log(Level.INFO, "Finished gathering statistics on observation_fact");
        }
    }

    private Connection openDataDatabaseConnection() throws SQLException {
        Connection con = this.dataConnectionSpec.getOrCreate();
        con.setAutoCommit(false);
        return con;
    }

    private Connection openMetadataDatabaseConnection() throws SQLException {
        Connection con = this.metadataConnectionSpec.getOrCreate();
        con.setAutoCommit(false);
        return con;
    }

    private static void logBatch(String tableName, int batchNumber) {
        Logger logger = I2b2ETLUtil.logger();
        if (logger.isLoggable(Level.FINEST)) {
            Object[] args = new Object[]{tableName, batchNumber};
            logger.log(Level.FINEST, "DB_{0}_BATCH={1}", args);
        }
    }

}
