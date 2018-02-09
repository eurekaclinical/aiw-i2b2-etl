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
package edu.emory.cci.aiw.i2b2etl.dest;

import org.protempa.query.QueryMode;
import edu.emory.cci.aiw.i2b2etl.dest.config.Concepts;
import edu.emory.cci.aiw.i2b2etl.dest.config.Configuration;
import edu.emory.cci.aiw.i2b2etl.dest.config.Data;
import edu.emory.cci.aiw.i2b2etl.dest.config.DataSpec;
import edu.emory.cci.aiw.i2b2etl.dest.config.Database;
import edu.emory.cci.aiw.i2b2etl.dest.config.DatabaseSpec;
import edu.emory.cci.aiw.i2b2etl.dest.config.Settings;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.conceptid.InvalidConceptCodeException;
import edu.emory.cci.aiw.i2b2etl.dest.table.InvalidPatientRecordException;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.Metadata;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.MetadataFactory;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.OntologyBuildException;
import edu.emory.cci.aiw.i2b2etl.dest.table.ConceptDimensionHandler;
import edu.emory.cci.aiw.i2b2etl.dest.table.ConceptDimensionLoader;
import edu.emory.cci.aiw.i2b2etl.dest.table.EncounterMappingHandler;
import edu.emory.cci.aiw.i2b2etl.dest.table.FactHandler;
import edu.emory.cci.aiw.i2b2etl.dest.table.InvalidFactException;
import edu.emory.cci.aiw.i2b2etl.dest.table.MetaTableConceptLoader;
import edu.emory.cci.aiw.i2b2etl.dest.table.MetaTableConceptHandler;
import edu.emory.cci.aiw.i2b2etl.dest.table.ModifierDimensionHandler;
import edu.emory.cci.aiw.i2b2etl.dest.table.ModifierDimensionLoader;
import edu.emory.cci.aiw.i2b2etl.dest.table.PatientDimension;
import edu.emory.cci.aiw.i2b2etl.dest.table.PatientDimensionHandler;
import edu.emory.cci.aiw.i2b2etl.dest.table.PatientDimensionFactory;
import edu.emory.cci.aiw.i2b2etl.dest.table.PatientMappingHandler;
import edu.emory.cci.aiw.i2b2etl.dest.table.PropositionFactHandler;
import edu.emory.cci.aiw.i2b2etl.dest.table.ProviderDimension;
import edu.emory.cci.aiw.i2b2etl.dest.table.ProviderDimensionHandler;
import edu.emory.cci.aiw.i2b2etl.dest.table.ProviderDimensionFactory;
import edu.emory.cci.aiw.i2b2etl.dest.table.RejectedFactHandler;
import edu.emory.cci.aiw.i2b2etl.dest.table.RejectedFactHandlerFactory;
import edu.emory.cci.aiw.i2b2etl.dest.table.VisitDimension;
import edu.emory.cci.aiw.i2b2etl.dest.table.VisitDimensionHandler;
import edu.emory.cci.aiw.i2b2etl.dest.table.VisitDimensionFactory;
import org.arp.javautil.sql.ConnectionSpec;
import org.protempa.KnowledgeSource;
import org.protempa.KnowledgeSourceReadException;
import org.protempa.PropositionDefinition;
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
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.StringUtils;
import org.protempa.DataSource;
import org.protempa.KnowledgeSourceCache;
import org.protempa.KnowledgeSourceCacheFactory;
import org.protempa.ProtempaEvent;
import org.protempa.ProtempaEventListener;
import org.protempa.backend.dsb.DataSourceBackend;
import org.protempa.backend.ksb.KnowledgeSourceBackend;

import org.protempa.dest.QueryResultsHandlerCloseException;

/**
 * After loading data intp the temporary tables, a series of stored procedures
 * are called to do the final stage of the data load into i2b2's data schema.
 * There currently are Oracle and PostgreSQL implementations of the stored
 * procedures. An implementation of the stored procedures must implement the
 * following API. The procedures below are executed in order:
 *
 * <dl>
 * <dt>EUREKA.EK_PRE_HOOK()</dt>
 * <dd>Called first after the database session is created. Sets up the
 * session.</dd>
 * <dt>EUREKA.EK_DISABLE_INDEXES()</dt>
 * <dd>Statements to disable indexes that might slow down ETL can be put here.
 * This procedure is called optionally. It gets called second, after
 * EK_PRE_HOOK.</dd>
 * <dt>EUREKA.EK_INSERT_PID_MAP_FROMTEMP(?, ?)</dt>
 * <dd>Populates the PATIENT_MAPPING table. First argument is the name of the
 * temporary table for new, changed, and logically deleted patient mapping
 * records. Second argument is the upload id. It is not called if there was a
 * previous error.</dd>
 * <dt>EUREKA.EK_INSERT_EID_MAP_FROMTEMP(?, ?)</dt>
 * <dd>Populates the ENCOUNTER_MAPPING table. First argument is the name of the
 * temporary table for new, changed, and logically deleted encounter mapping
 * records. Second argument is the upload id. It is not called if there was a
 * previous error.</dd>
 * <dt>EUREKA.EK_INS_PATIENT_FROMTEMP(?, ?)</dt>
 * <dd>Populates the PATIENT_DIMENSION table. First argument is the name of the
 * temporary table for new, changed, and logically deleted patient records.
 * Second argument is the upload id. It is not called if there was a previous
 * error.</dd>
 * <dt>EUREKA.EK_INS_ENC_VISIT_FROMTEMP(?, ?)</dt>
 * <dd>Populates the VISIT_DIMENSION table. First argument is the name of the
 * temporary table for new, changed, and logically deleted visit records. Second
 * argument is the upload id. It is not called if there was a previous
 * error.</dd>
 * <dt>EUREKA.EK_INS_PROVIDER_FROMTEMP(?, ?)</dt>
 * <dd>Populates the PROVIDER_DIMENSION table. First argument is the name of the
 * temporary table for new, changed, and logically deleted provider records.
 * Second argument is the upload id. It is not called if there was a previous
 * error.</dd>
 * <dt>EUREKA.EK_INS_CONCEPT_FROMTEMP(?, ?)</dt>
 * <dd>Populates the CONCEPT_DIMENSION table. First argument is the name of the
 * temporary table for new, changed, and logically deleted concept records.
 * Second argument is the upload id. It is not called if there was a previous
 * error.</dd>
 * <dt>EUREKA.EK_INS_MODIFIER_FROMTEMP(?, ?)</dt>
 * <dd>Populates the MODIFIER_DIMENSION table. First argument is the name of the
 * temporary table for new, changed, and logically deleted modifier records.
 * Second argument is the upload id. It is not called if there was a previous
 * error.</dd>
 * <dt>EUREKA.EK_UPDATE_OBSERVATION_FACT(?, ?, ?, ?)</dt>
 * <dd>Populates the OBSERVATION_FACT table. First argument is the name of the
 * temporary table for new, changed, and logically deleted observation facts.
 * Second argument is the name of the intermediate temporary table for
 * processing observation facts. Third argument is the upload id. Fourth
 * argument is 1 or 0 depending on whether to merge on update (1) or append on
 * update (0). It is not called if there was a previous error.</dd>
 * <dt>EUREKA.EK_ENABLE_INDEXES()</dt>
 * <dd>Statements to disable indexes that might slow down ETL can be put here.
 * This procedure gets called only if EUREKA.EK_DISABLE_INDEXES was called. It
 * is called even if there was a previous error.</dd>
 * <dt>EUREKA.EK_POST_HOOK()</dt>
 * <dd>It is always called, even if there was a previous error. Performs
 * database-specific session cleanup.</dd>
 * </dl>
 *
 * @author Andrew Post
 */
public final class I2b2QueryResultsHandler extends AbstractQueryResultsHandler {

    private static final String[] OBX_FACT_IDXS = new String[]{"FACT_NOLOB", "FACT_PATCON_DATE_PRVD_IDX", "FACT_CNPT_PAT_ENCT_IDX"};

    // upload_id for all the dimension table stored procedures
    private final static int UPLOAD_ID = 0;

    private final Query query;
    private final KnowledgeSource knowledgeSource;
    private final Settings settings;
    private final Data data;
    private final Database database;
    private final ConnectionSpec dataConnectionSpec;
    private final Concepts conceptsSection;
    private List<FactHandler> factHandlers;
    private ConceptDimensionHandler conceptDimensionHandler;
    private ModifierDimensionHandler modifierDimensionHandler;
    private Metadata metadata;
    private final DataSpec providerFullNameSpec;
    private final DataSpec providerFirstNameSpec;
    private final DataSpec providerMiddleNameSpec;
    private final DataSpec providerLastNameSpec;
    private final ConnectionSpec metadataConnectionSpec;
    private final String visitPropId;
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
    private KnowledgeSourceCache cache;
    private List<? extends ProtempaEventListener> eventListeners;
    private String dataSchemaName;

    /**
     * Creates a new query results handler that will use the provided
     * configuration file. This constructor, through the
     * <code>inferPropositionIdsNeeded</code> parameter, lets you control
     * whether proposition ids to be returned from the Protempa processing run
     * should be inferred from the i2b2 configuration file.
     *
     * @param confXML an i2b2 query results handler configuration file. Cannot
     * be <code>null</code>.
     * @param inferPropositionIdsNeeded <code>true</code> if proposition ids to
     * be returned from the Protempa processing run should include all of those
     * specified in the i2b2 configuration file, <code>false</code> if the
     * proposition ids returned should be only those specified in the Protempa
     * {@link Query}.
     * @param dataInsertMode whether to truncate existing data or append to it
     */
    I2b2QueryResultsHandler(Query query, DataSource dataSource, KnowledgeSource knowledgeSource, Configuration configuration, List<? extends ProtempaEventListener> eventListeners)
            throws QueryResultsHandlerInitException {
        if (dataSource == null) {
            throw new IllegalArgumentException("dataSource cannot be null");
        }
        if (knowledgeSource == null) {
            throw new IllegalArgumentException("knowledgeSource cannot be null");
        }
        Logger logger = I2b2ETLUtil.logger();
        this.query = query;
        this.knowledgeSource = knowledgeSource;
        this.configuration = configuration;
        logger.log(Level.FINE, String.format("Using configuration: %s",
                this.configuration.getName()));
        logger.log(Level.FINER, "STEP: read conf.xml");
        this.settings = this.configuration.getSettings();

        this.data = this.configuration.getData();
        this.conceptsSection = this.configuration.getConcepts();
        this.database = this.configuration.getDatabase();
        DatabaseSpec dataSchemaSpec = this.database.getDataSpec();
        if (dataSchemaSpec != null) {
            this.dataConnectionSpec = dataSchemaSpec.toConnectionSpec();
        } else {
            this.dataConnectionSpec = null;
        }

        DatabaseSpec metadataSchemaSpec = this.database.getMetadataSpec();
        if (metadataSchemaSpec != null) {
            this.metadataConnectionSpec = metadataSchemaSpec.toConnectionSpec();
        } else {
            this.metadataConnectionSpec = null;
        }

        this.providerFullNameSpec = this.data.get(this.settings.getProviderFullName());
        this.providerFirstNameSpec = this.data.get(this.settings.getProviderFirstName());
        this.providerMiddleNameSpec = this.data.get(this.settings.getProviderMiddleName());
        this.providerLastNameSpec = this.data.get(this.settings.getProviderLastName());
        this.visitPropId = this.settings.getVisitDimension();

        RemoveMethod removeMethod = this.settings.getDataRemoveMethod();
        if (removeMethod != null) {
            this.dataRemoveMethod = removeMethod;
        } else {
            this.dataRemoveMethod = RemoveMethod.TRUNCATE;
        }
        RemoveMethod metaRemoveMethod2 = this.settings.getMetaRemoveMethod();
        if (metaRemoveMethod2 != null) {
            this.metaRemoveMethod = metaRemoveMethod2;
        } else {
            this.metaRemoveMethod = RemoveMethod.TRUNCATE;
        }

        DataSourceBackend[] dsBackends = dataSource.getBackends();
        this.dataSourceBackendIds = new HashSet<>();
        for (int i = 0; i < dsBackends.length; i++) {
            String id = dsBackends[i].getId();
            if (id != null) {
                this.dataSourceBackendIds.add(id);
            }
        }
        String sourceSystemCd = this.settings.getSourceSystemCode();
        if (sourceSystemCd != null) {
            this.qrhId = sourceSystemCd;
        } else {
            this.qrhId = I2B2QueryResultsHandlerSourceId.getInstance().getStringRepresentation();
        }
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

        this.eventListeners = eventListeners;
    }

    @Override
    public String getId() {
        return this.qrhId;
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
    public void start(Collection<PropositionDefinition> propDefs) throws QueryResultsHandlerProcessingException {
        Logger logger = I2b2ETLUtil.logger();
        try {
            this.conceptDimensionHandler = new ConceptDimensionHandler(dataConnectionSpec);
            this.modifierDimensionHandler = new ModifierDimensionHandler(dataConnectionSpec);
            this.cache = new KnowledgeSourceCacheFactory().getInstance(this.knowledgeSource, propDefs, true);
            this.metadata = new MetadataFactory().getInstance(propDefs, this.qrhId, this.cache, collectUserPropositionDefinitions(), this.conceptsSection.getFolderSpecs(), settings, this.data, this.metadataConnectionSpec);
            this.providerDimensionFactory = new ProviderDimensionFactory(this.metadata, this.settings, this.dataConnectionSpec);
            this.patientDimensionFactory = new PatientDimensionFactory(this.metadata, this.settings, this.data, this.dataConnectionSpec);
            this.visitDimensionFactory = new VisitDimensionFactory(this.metadata, this.settings, this.data, this.dataConnectionSpec);
            DataRemoverFactory f = new DataRemoverFactory();
            if (this.query.getQueryMode() == QueryMode.REPLACE) {
                f.getInstance(this.dataRemoveMethod).doRemoveData();
            }
            f.getInstance(this.metaRemoveMethod).doRemoveMetadata();
            this.factHandlers = new ArrayList<>();
            addPropositionFactHandlers();
            executePreHook();
            // disable indexes on observation_fact to speed up inserts
            disableObservationFactIndexes();
            // create i2b2 temporary tables using stored procedures
            truncateTempTables();
            this.dataSchemaConnection = openDataDatabaseConnection();

            this.dataSchemaName = this.dataSchemaConnection.getSchema();

            if (this.settings.getManageCTotalNum()) {
                try (Connection conn = openMetadataDatabaseConnection()) {
                    try (CallableStatement mappingCall = conn.prepareCall("{ call ECMETA.EC_CLEAR_C_TOTALNUM() }")) {
                        logger.log(Level.INFO, "Clearing C_TOTALNUM for query {0}", this.query.getName());
                        mappingCall.execute();
                        //commit and rollback are called by stored procedure.
                    }
                }
            }
            logger.log(Level.INFO, "Populating observation facts table for query {0}", this.query.getName());
        } catch (KnowledgeSourceReadException | SQLException | OntologyBuildException ex) {
            throw new QueryResultsHandlerProcessingException("Error during i2b2 load", ex);
        }
    }

    private String rejectedObservationFactTable() {
        return RejectedFactHandler.REJECTED_FACT_TABLE;
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

    private String tempObservationFactCompleteTableName() {
        return PropositionFactHandler.TEMP_OBSERVATION_COMPLETE_TABLE;
    }

    private String tempModifierTableName() {
        return ModifierDimensionHandler.TEMP_MODIFIER_TABLE;
    }

    /**
     * Calls stored procedures to drop all of the temp tables created.
     *
     * @throws SQLException if an error occurs while interacting with the
     * database
     */
    private void truncateTempTables() throws SQLException {
        Logger logger = I2b2ETLUtil.logger();
        logger.log(Level.INFO, "Truncating temp data tables for query {0}", this.query.getName());
        try (final Connection conn = openDataDatabaseConnection()) {
            conn.setAutoCommit(true);
            String[] dataschemaTables = {tempPatientTableName(), tempPatientMappingTableName(), tempVisitTableName(), tempEncounterMappingTableName(), tempProviderTableName(), tempConceptTableName(), tempModifierTableName(), tempObservationFactTableName(), tempObservationFactCompleteTableName()};
            for (String tableName : dataschemaTables) {
                truncateTable(conn, tableName);
            }
            logger.log(Level.INFO, "Done truncating temp data tables for query {0}", this.query.getName());
        }
    }

    @Override
    public void handleQueryResult(String keyId, List<Proposition> propositions, Map<Proposition, List<Proposition>> forwardDerivations, Map<Proposition, List<Proposition>> backwardDerivations, Map<UniqueId, Proposition> references) throws QueryResultsHandlerProcessingException {
        Logger logger = I2b2ETLUtil.logger();
        logger.log(Level.FINER, "Loading patient into i2b2");
        try {
            Set<Proposition> derivedPropositions = new HashSet<>();
            PatientDimension pd = null;
            for (Proposition prop : propositions) {
                if (prop.getId().equals(this.visitPropId)) {
                    pd = handlePatient(pd, keyId, prop, references, forwardDerivations, backwardDerivations, derivedPropositions);
                }
            }
        } catch (InvalidConceptCodeException | InvalidFactException | InvalidPatientRecordException | SQLException ex) {
            throw new QueryResultsHandlerProcessingException("Load into i2b2 failed for query " + this.query.getName(), ex);
        }
        logger.log(Level.FINER, "Done loading patient into i2b2");
    }

    private PatientDimension handlePatient(PatientDimension pd, String keyId, Proposition prop, Map<UniqueId, Proposition> references, Map<Proposition, List<Proposition>> forwardDerivations, Map<Proposition, List<Proposition>> backwardDerivations, Set<Proposition> derivedPropositions) throws SQLException, InvalidConceptCodeException, InvalidFactException, InvalidPatientRecordException {
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
        VisitDimension vd = this.visitDimensionFactory.getInstance(pd.getEncryptedPatientId(), pd.getEncryptedPatientIdSource(), (TemporalProposition) prop, references);
        for (FactHandler factHandler : this.factHandlers) {
            factHandler.handleRecord(pd, vd, providerDimension, prop, forwardDerivations, backwardDerivations, references, derivedPropositions);
        }
        return pd;
    }

    @Override
    public void finish() throws QueryResultsHandlerProcessingException {

        Logger logger = I2b2ETLUtil.logger();
        logger.log(Level.FINE, "Beginning finish for query {0}", this.query.getName());
        String queryId = this.query.getName();

        SQLException exception = null;

        try {
            if (this.factHandlers != null) {
                for (Iterator<FactHandler> itr = this.factHandlers.iterator(); itr.hasNext();) {
                    FactHandler factHandler = itr.next();
                    factHandler.close();
                    itr.remove();
                }
            }
        } catch (SQLException ex) {
            exception = ex;
        }

        if (this.dataSchemaConnection != null) {
            try {
                this.dataSchemaConnection.close();
                this.dataSchemaConnection = null;
            } catch (SQLException ex) {
                if (exception == null) {
                    exception = ex;
                }
            }
        }

        if (this.patientDimensionFactory != null) {
            try {
                // persist Patients & Visits.
                this.patientDimensionFactory.close();
                this.patientDimensionFactory = null;
            } catch (SQLException ex) {
                if (exception == null) {
                    exception = ex;
                }
            }
        }

        logger.log(Level.INFO, "Populating dimensions for query {0}", queryId);

        if (exception == null) {
            try (Connection conn = openDataDatabaseConnection();
                    CallableStatement mappingCall = conn.prepareCall("{ call EUREKA.EK_INSERT_PID_MAP_FROMTEMP(?, ?) }")) {
                conn.setAutoCommit(true);
                logger.log(Level.INFO, "Populating patient dimension for query {0}", queryId);
                mappingCall.setString(1, tempPatientMappingTableName());
                mappingCall.setInt(2, UPLOAD_ID);
                mappingCall.execute();
            } catch (SQLException ex) {
                exception = ex;
            }
        }

        if (this.visitDimensionFactory != null) {
            try {
                this.visitDimensionFactory.close();
                this.visitDimensionFactory = null;
            } catch (SQLException ex) {
                if (exception == null) {
                    exception = ex;
                }
            }
        }

        if (exception == null) {
            try (Connection conn = openDataDatabaseConnection();
                    CallableStatement mappingCall = conn.prepareCall("{ call EUREKA.EK_INSERT_EID_MAP_FROMTEMP(?, ?) }")) {
                conn.setAutoCommit(true);
                mappingCall.setString(1, tempEncounterMappingTableName());
                mappingCall.setInt(2, UPLOAD_ID);
                mappingCall.execute();
            } catch (SQLException ex) {
                exception = ex;
            }
        }

        if (exception == null) {
            try (Connection conn = openDataDatabaseConnection()) {
                try (CallableStatement call = conn.prepareCall("{ call EUREKA.EK_INS_PATIENT_FROMTEMP(?, ?) }")) {
                    conn.setAutoCommit(true);
                    call.setString(1, tempPatientTableName());
                    call.setInt(2, UPLOAD_ID);
                    call.execute();
                }
            } catch (SQLException ex) {
                exception = ex;
            }
        }

        if (exception == null) {
            try (Connection conn = openDataDatabaseConnection();
                    CallableStatement call = conn.prepareCall("{ call EUREKA.EK_INS_ENC_VISIT_FROMTEMP(?, ?) }")) {
                conn.setAutoCommit(true);
                logger.log(Level.INFO, "Populating visit dimension for query {0}", queryId);
                call.setString(1, tempVisitTableName());
                call.setInt(2, UPLOAD_ID);
                call.execute();
                //commit and rollback are called by the stored procedure.
            } catch (SQLException ex) {
                exception = ex;
            }
        }

        if (this.providerDimensionFactory != null) {
            try {
                // find Provider root. gather its leaf nodes. persist Providers.
                this.providerDimensionFactory.close();
                this.providerDimensionFactory = null;
            } catch (SQLException ex) {
                if (exception == null) {
                    exception = ex;
                }
            }
        }

        if (exception == null) {
            try {
                logger.log(Level.INFO, "Populating provider dimension for query {0}", queryId);
                try (Connection conn = openDataDatabaseConnection()) {
                    conn.setAutoCommit(true);
                    try (CallableStatement call = conn.prepareCall("{ call EUREKA.EK_INS_PROVIDER_FROMTEMP(?, ?) }")) {
                        call.setString(1, tempProviderTableName());
                        call.setInt(2, UPLOAD_ID);
                        call.execute();
                    }
                }
            } catch (SQLException ex) {
                exception = ex;
            }
        }

        if (exception == null) {
            try {
                // flush hot concepts out of the tree. persist Concepts.
                logger.log(Level.INFO, "Populating concept dimension for query {0}", this.query.getName());
                new ConceptDimensionLoader(this.conceptDimensionHandler).execute(this.metadata.getAllRoots());
            } catch (SQLException ex) {
                exception = ex;
            }
        }
        if (this.conceptDimensionHandler != null) {
            try {
                this.conceptDimensionHandler.close();
                this.conceptDimensionHandler = null;
            } catch (SQLException ex) {
                if (exception == null) {
                    exception = ex;
                }
            }
        }

        if (exception == null) {
            try {
                try (Connection conn = openDataDatabaseConnection()) {
                    conn.setAutoCommit(true);
                    try (CallableStatement call = conn.prepareCall("{ call EUREKA.EK_INS_CONCEPT_FROMTEMP(?, ?) }")) {
                        call.setString(1, tempConceptTableName());
                        call.setInt(2, UPLOAD_ID);
                        call.execute();
                    }
                }
            } catch (SQLException ex) {
                exception = ex;
            }
        }

        if (exception == null) {
            try {
                logger.log(Level.INFO, "Populating modifier dimension for query {0}", this.query.getName());
                new ModifierDimensionLoader(this.modifierDimensionHandler).execute(this.metadata.getModifierRoots());
            } catch (SQLException ex) {
                exception = ex;
            }
        }
        if (this.modifierDimensionHandler != null) {
            try {
                this.modifierDimensionHandler.close();
                this.modifierDimensionHandler = null;
            } catch (SQLException ex) {
                if (exception == null) {
                    exception = ex;
                }
            }
        }

        if (exception == null) {
            try (Connection conn = openDataDatabaseConnection()) {
                conn.setAutoCommit(true);
                try (CallableStatement call = conn.prepareCall("{ call EUREKA.EK_INS_MODIFIER_FROMTEMP(?, ?) }")) {
                    call.setString(1, tempModifierTableName());
                    call.setInt(2, UPLOAD_ID);
                    call.execute();
                }
            } catch (SQLException ex) {
                exception = ex;
            }
        }

        if (exception == null) {
            try {
                logger.log(Level.INFO, "Done populating dimensions for query {0}", queryId);

                try (Connection conn = openDataDatabaseConnection()) {
                    conn.setAutoCommit(true);
                    logger.log(Level.INFO, "Populating observation_fact from temporary table");
                    try (CallableStatement call = conn.prepareCall("{ call EUREKA.EK_UPDATE_OBSERVATION_FACT(?, ?, ?, ?) }")) {
                        call.setString(1, tempObservationFactTableName());
                        call.setString(2, tempObservationFactCompleteTableName());
                        call.setLong(3, UPLOAD_ID);
                        call.setLong(4, (this.query.getQueryMode() == QueryMode.UPDATE && this.settings.getMergeOnUpdate()) ? 1 : 0); // appendFlag
                        call.execute();
                    }
                }
            } catch (SQLException ex) {
                exception = ex;
            }
        }

        try {
            // re-enable the indexes now that we're done populating the table
            enableObservationFactIndexes();
        } catch (SQLException ex) {
            if (exception == null) {
                exception = ex;
            }
        }

        try {
            // execute post-hook
            executePostHook();
        } catch (SQLException ex) {
            if (exception == null) {
                exception = ex;
            }
        }

        List<String> cFullNames = new ArrayList<>();
        if (exception == null && this.metadataConnectionSpec != null) {
            logger.log(Level.INFO, "Querying TABLE_ACCESS for full names");
            try (Connection conn = openMetadataDatabaseConnection()) {
                try (Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery("SELECT DISTINCT C_FULLNAME FROM TABLE_ACCESS")) {
                    while (rs.next()) {
                        cFullNames.add(rs.getString(1));
                    }
                }
            } catch (SQLException ex) {
                exception = ex;
            }
        }

        if (exception == null && !cFullNames.isEmpty()) {
            for (String cFullName : cFullNames) {
                logger.log(Level.INFO, "Getting number of records loaded for {0}", cFullName);
                fireProtempaEvent(new ProtempaEvent(ProtempaEvent.Level.INFO, ProtempaEvent.Type.QRH_STEP_START, getClass(), new Date(), "Count of " + cFullName));
                String countQuery = "SELECT count(*) FROM " + tempObservationFactCompleteTableName()
                        + " obx join "
                        + tempConceptTableName()
                        + " tc ON (obx.concept_cd=tc.concept_cd) WHERE tc.concept_path like '"
                        + cFullName
                        + "%' AND obx.modifier_cd='@' AND obx.patient_num IS NOT NULL AND obx.encounter_num IS NOT NULL";
                int count = -1;
                try (Connection conn = openDataDatabaseConnection();
                        Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery(countQuery)) {
                    if (rs.next()) {
                        count = rs.getInt(1);
                    }
                } catch (SQLException ex) {
                    exception = ex;
                    break;
                }
                fireProtempaEvent(new ProtempaEvent(ProtempaEvent.Level.INFO, ProtempaEvent.Type.QRH_STEP_STOP, getClass(), new Date(), "Count of " + cFullName));
                if (exception == null) {
                    logger.log(Level.INFO, "{0} {1} record(s) loaded", new Object[]{count, cFullName});
                    fireProtempaEvent(new ProtempaEvent(ProtempaEvent.Level.INFO, ProtempaEvent.Type.QRH_STEP_RESULT, getClass(), new Date(), "Count of " + cFullName + ": " + count));
                } else {
                    fireProtempaEvent(new ProtempaEvent(ProtempaEvent.Level.INFO, ProtempaEvent.Type.QRH_STEP_RESULT, getClass(), new Date(), "Count of " + cFullName + ": ERROR (" + exception.getMessage() + ")"));
                }
            }
        }

        if (exception == null) {
            logger.log(Level.INFO, "Getting number of patient records loaded");
            fireProtempaEvent(new ProtempaEvent(ProtempaEvent.Level.INFO, ProtempaEvent.Type.QRH_STEP_START, getClass(), new Date(), "Count of patient records"));
            int count = -1;
            try (Connection conn = openDataDatabaseConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tempPatientTableName())) {
                if (rs.next()) {
                    count = rs.getInt(1);
                }
            } catch (SQLException ex) {
                exception = ex;
            }
            fireProtempaEvent(new ProtempaEvent(ProtempaEvent.Level.INFO, ProtempaEvent.Type.QRH_STEP_STOP, getClass(), new Date(), "Count of patient records"));
            if (exception == null) {
                logger.log(Level.INFO, "{0} patient record(s) loaded", count);
                fireProtempaEvent(new ProtempaEvent(ProtempaEvent.Level.INFO, ProtempaEvent.Type.QRH_STEP_RESULT, getClass(), new Date(), "Count of patient records: " + count));
            } else {
                fireProtempaEvent(new ProtempaEvent(ProtempaEvent.Level.INFO, ProtempaEvent.Type.QRH_STEP_RESULT, getClass(), new Date(), "Count of patient records: ERROR (" + exception.getMessage() + ")"));
            }
        }

        if (exception == null) {
            logger.log(Level.INFO, "Getting number of visit records loaded");
            fireProtempaEvent(new ProtempaEvent(ProtempaEvent.Level.INFO, ProtempaEvent.Type.QRH_STEP_START, getClass(), new Date(), "Count of visit records"));
            int count = -1;
            try (Connection conn = openDataDatabaseConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tempVisitTableName())) {
                if (rs.next()) {
                    count = rs.getInt(1);
                }
            } catch (SQLException ex) {
                exception = ex;
            }
            fireProtempaEvent(new ProtempaEvent(ProtempaEvent.Level.INFO, ProtempaEvent.Type.QRH_STEP_STOP, getClass(), new Date(), "Count of visit records"));
            if (exception == null) {
                logger.log(Level.INFO, "{0} visit record(s) loaded", count);
                fireProtempaEvent(new ProtempaEvent(ProtempaEvent.Level.INFO, ProtempaEvent.Type.QRH_STEP_RESULT, getClass(), new Date(), "Count of visit records: " + count));
            } else {
                fireProtempaEvent(new ProtempaEvent(ProtempaEvent.Level.INFO, ProtempaEvent.Type.QRH_STEP_RESULT, getClass(), new Date(), "Count of visit records: ERROR (" + exception.getMessage() + ")"));
            }
        }

        if (exception == null) {
            logger.log(Level.INFO, "Getting number of provider records loaded");
            fireProtempaEvent(new ProtempaEvent(ProtempaEvent.Level.INFO, ProtempaEvent.Type.QRH_STEP_START, getClass(), new Date(), "Count of provider records"));
            int count = -1;
            try (Connection conn = openDataDatabaseConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tempProviderTableName())) {
                if (rs.next()) {
                    count = rs.getInt(1);
                }
            } catch (SQLException ex) {
                exception = ex;
            }
            fireProtempaEvent(new ProtempaEvent(ProtempaEvent.Level.INFO, ProtempaEvent.Type.QRH_STEP_STOP, getClass(), new Date(), "Count of provider records"));
            if (exception == null) {
                logger.log(Level.INFO, "{0} provider record(s) loaded", count);
                fireProtempaEvent(new ProtempaEvent(ProtempaEvent.Level.INFO, ProtempaEvent.Type.QRH_STEP_RESULT, getClass(), new Date(), "Count of provider records: " + count));
            } else {
                fireProtempaEvent(new ProtempaEvent(ProtempaEvent.Level.INFO, ProtempaEvent.Type.QRH_STEP_RESULT, getClass(), new Date(), "Count of provider records: ERROR (" + exception.getMessage() + ")"));
            }
        }

        if (exception == null) {
            logger.log(Level.INFO, "Getting number of concept records loaded");
            fireProtempaEvent(new ProtempaEvent(ProtempaEvent.Level.INFO, ProtempaEvent.Type.QRH_STEP_START, getClass(), new Date(), "Count of concept records"));
            int count = -1;
            try (Connection conn = openDataDatabaseConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tempConceptTableName())) {
                if (rs.next()) {
                    count = rs.getInt(1);
                }
            } catch (SQLException ex) {
                exception = ex;
            }
            fireProtempaEvent(new ProtempaEvent(ProtempaEvent.Level.INFO, ProtempaEvent.Type.QRH_STEP_STOP, getClass(), new Date(), "Count of concept records"));
            if (exception == null) {
                logger.log(Level.INFO, "{0} concept record(s) loaded", count);
                fireProtempaEvent(new ProtempaEvent(ProtempaEvent.Level.INFO, ProtempaEvent.Type.QRH_STEP_RESULT, getClass(), new Date(), "Count of concept records: " + count));
            } else {
                fireProtempaEvent(new ProtempaEvent(ProtempaEvent.Level.INFO, ProtempaEvent.Type.QRH_STEP_RESULT, getClass(), new Date(), "Count of concept records: ERROR (" + exception.getMessage() + ")"));
            }
        }

        if (exception == null) {
            try {
                logger.log(Level.INFO, "Done populating observation fact table for query {0}", queryId);
                if (this.metadataConnectionSpec != null) {
                    logger.log(Level.INFO, "Populating metadata tables for query {0}", queryId);
                    String tableName = this.settings.getMetaTableName();
                    try (MetaTableConceptHandler metaTableHandler = new MetaTableConceptHandler(this.metadataConnectionSpec, tableName)) {
                        MetaTableConceptLoader metaTableConceptLoader = new MetaTableConceptLoader(metaTableHandler);
                        metaTableConceptLoader.execute(this.metadata.getAllRoots());
                        logger.log(Level.INFO, "Done populating metadata tables for query {0}", queryId);
                    }
                } else {
                    logger.log(Level.INFO, "Skipping metadata tables for query {0}", queryId);
                }
            } catch (SQLException ex) {
                exception = ex;
            }
        }

        if (exception == null && this.settings.getManageCTotalNum()) {
            if (this.dataSchemaName != null) {
                try (Connection conn = openMetadataDatabaseConnection()) {
                    try (CallableStatement mappingCall = conn.prepareCall("{ call ECMETA.EC_UPDATE_C_TOTALNUM(?) }")) {
                        logger.log(Level.INFO, "Updating C_TOTALNUM for query {0}", this.query.getName());
                        mappingCall.setString(1, this.dataSchemaName);
                        mappingCall.execute();
                        //commit and rollback are called by stored procedure.
                    }
                } catch (SQLException ex) {
                    exception = ex;
                }
            } else {
                logger.log(Level.WARNING, "Data schema is unknown, so could not update C_TOTALNUM");
            }
        }

        if (exception
                != null) {
            logger.log(Level.SEVERE, "Load into i2b2 failed for query " + queryId, exception);
            throw new QueryResultsHandlerProcessingException("Load into i2b2 failed for query " + queryId, exception);
        }
    }

    private void fireProtempaEvent(ProtempaEvent evt) {
        for (ProtempaEventListener listener : this.eventListeners) {
            listener.eventFired(evt);
        }
    }

    @Override
    public void close() throws QueryResultsHandlerCloseException {

        if (this.factHandlers != null) {
            for (FactHandler factHandler : this.factHandlers) {
                try {
                    factHandler.close();
                } catch (SQLException ignore) {
                }
            }
            this.factHandlers = null;
        }

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

    private void addPropositionFactHandlers() throws KnowledgeSourceReadException, SQLException {
        String[] potentialDerivedPropIdsArr = this.metadata.extractDerived();
        Set<String> dimDataTypes = this.settings.getDimensionDataTypes();
        RejectedFactHandlerFactory rejectedFactHandlerFactory
                = new RejectedFactHandlerFactory(this.dataConnectionSpec, rejectedObservationFactTable());
        for (DataSpec dataSpec : this.data.getAll()) {
            if (!dimDataTypes.contains(dataSpec.getKey())) {
                Link[] links;
                if (dataSpec.getReferenceName() != null) {
                    links = new Link[]{new Reference(dataSpec.getReferenceName())};
                } else {
                    links = null;
                }
                PropositionFactHandler propFactHandler
                        = new PropositionFactHandler(this.dataConnectionSpec, links, dataSpec.getPropertyName(),
                                dataSpec.getStart(), dataSpec.getFinish(), dataSpec.getUnits(),
                                potentialDerivedPropIdsArr, this.metadata,
                                this.cache,
                                rejectedFactHandlerFactory);
                this.factHandlers.add(propFactHandler);

            }
        }
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
            String queryId = query.getName();
            Logger logger = I2b2ETLUtil.logger();
            logger.log(Level.INFO, "Truncating data tables for query {0}", queryId);
            String[] dataschemaTables = {"OBSERVATION_FACT", "CONCEPT_DIMENSION", "PATIENT_DIMENSION", "PATIENT_MAPPING", "PROVIDER_DIMENSION", "VISIT_DIMENSION", "ENCOUNTER_MAPPING", "MODIFIER_DIMENSION"};
            try (final Connection conn = openDataDatabaseConnection()) {
                conn.setAutoCommit(true);
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
            if (metadataConnectionSpec != null) {
                String queryId = query.getName();
                Logger logger = I2b2ETLUtil.logger();
                logger.log(Level.INFO, "Truncating metadata tables for query {0}", queryId);
                try (final Connection conn = openMetadataDatabaseConnection()) {
                    conn.setAutoCommit(true);
                    truncateTable(conn, settings.getMetaTableName()); // metaTableName in conf.xml
                    logger.log(Level.INFO, "Done truncating metadata tables for query {0}", queryId);
                }
            }
        }

    }

    private void truncateTable(Connection conn, String tableName) throws SQLException {
        Logger logger = I2b2ETLUtil.logger();
        String queryId = query.getName();
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

    private class TableDeleter extends DataRemover {

        @Override
        void doRemoveData() throws SQLException {
            String queryId = query.getName();
            Logger logger = I2b2ETLUtil.logger();
            logger.log(Level.INFO, "Deleting data tables for query {0}", queryId);
            String[] dataschemaTables = {"OBSERVATION_FACT", "CONCEPT_DIMENSION", "PATIENT_DIMENSION", "PATIENT_MAPPING", "PROVIDER_DIMENSION", "VISIT_DIMENSION", "ENCOUNTER_MAPPING", "MODIFIER_DIMENSION"};
            try (final Connection conn = openDataDatabaseConnection()) {
                conn.setAutoCommit(true);
                for (String tableName : dataschemaTables) {
                    deleteTable(conn, tableName, dataSourceBackendIds);
                }
                logger.log(Level.INFO, "Done deleting data for query {0}", queryId);
            }
        }

        @Override
        void doRemoveMetadata() throws SQLException {
            if (metadataConnectionSpec != null) {
                String queryId = query.getName();
                Logger logger = I2b2ETLUtil.logger();
                logger.log(Level.INFO, "Deleting metadata for query {0}", queryId);
                try (final Connection conn = openMetadataDatabaseConnection()) {
                    conn.setAutoCommit(true);
                    deleteTable(conn, settings.getMetaTableName(), knowledgeSourceBackendIds); // metaTableName in conf.xml
                    logger.log(Level.INFO, "Done deleting metadata for query {0}", queryId);
                }
            }
        }

        private void deleteTable(Connection conn, String tableName, Set<String> sourceSystemCodes) throws SQLException {
            Logger logger = I2b2ETLUtil.logger();
            String queryId = query.getName();
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

    private void executePreHook() throws SQLException {
        Logger logger = I2b2ETLUtil.logger();
        logger.log(Level.INFO, "Executing pre-hook");
        try (Connection conn = openDataDatabaseConnection();
                CallableStatement stmt = conn.prepareCall("{call EUREKA.EK_PRE_HOOK()}")) {
            stmt.execute();
            logger.log(Level.INFO, "Pre-hook executed successfully");
        }
    }

    private void executePostHook() throws SQLException {
        Logger logger = I2b2ETLUtil.logger();
        logger.log(Level.INFO, "Executing post-hook");
        try (Connection conn = openDataDatabaseConnection();
                CallableStatement stmt = conn.prepareCall("{call EUREKA.EK_POST_HOOK()}")) {
            stmt.execute();
            logger.log(Level.INFO, "Post-hook executed successfully");
        }
    }

    private void disableObservationFactIndexes() throws SQLException {
        if (this.query.getQueryMode() == QueryMode.REPLACE || !this.configuration.getSettings().getMergeOnUpdate()) {
            Logger logger = I2b2ETLUtil.logger();
            logger.log(Level.INFO, "Disabling indices on observation_fact");
            try (Connection conn = openDataDatabaseConnection();
                    CallableStatement stmt = conn.prepareCall("{call EUREKA.EK_DISABLE_INDEXES()}")) {
                //stmt.registerOutParameter(1, Types.VARCHAR);
                stmt.execute();
                logger.log(Level.INFO, "Disabled indices on observation_fact");
            }
        }
    }

    private void enableObservationFactIndexes() throws SQLException {
        if (this.query.getQueryMode() == QueryMode.REPLACE || !this.configuration.getSettings().getMergeOnUpdate()) {
            Logger logger = I2b2ETLUtil.logger();
            logger.log(Level.INFO, "Enabling indices on observation_fact");
            try (Connection conn = openDataDatabaseConnection();
                    CallableStatement stmt = conn.prepareCall("{call EUREKA.EK_ENABLE_INDEXES()}")) {
                stmt.execute();
                logger.log(Level.INFO, "Enabled indices on observation_fact");
            }
        }
    }

    private Connection openDataDatabaseConnection() throws SQLException {
        return this.dataConnectionSpec.getOrCreate();
    }

    private Connection openMetadataDatabaseConnection() throws SQLException {
        return this.metadataConnectionSpec.getOrCreate();
    }

}
