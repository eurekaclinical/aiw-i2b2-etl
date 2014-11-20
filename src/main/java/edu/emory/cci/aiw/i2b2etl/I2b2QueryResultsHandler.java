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

import edu.emory.cci.aiw.i2b2etl.configuration.ConceptsSection;
import edu.emory.cci.aiw.i2b2etl.configuration.ConfigurationReadException;
import edu.emory.cci.aiw.i2b2etl.configuration.ConfigurationReader;
import edu.emory.cci.aiw.i2b2etl.configuration.DataSection;
import edu.emory.cci.aiw.i2b2etl.configuration.DatabaseSection;
import edu.emory.cci.aiw.i2b2etl.configuration.DictionarySection;
import edu.emory.cci.aiw.i2b2etl.metadata.Concept;
import edu.emory.cci.aiw.i2b2etl.metadata.InvalidConceptCodeException;
import edu.emory.cci.aiw.i2b2etl.metadata.InvalidPatientRecordException;
import edu.emory.cci.aiw.i2b2etl.metadata.Metadata;
import edu.emory.cci.aiw.i2b2etl.metadata.MetadataUtil;
import edu.emory.cci.aiw.i2b2etl.metadata.OntologyBuildException;
import edu.emory.cci.aiw.i2b2etl.metadata.SynonymCode;
import edu.emory.cci.aiw.i2b2etl.table.ConceptDimension;
import edu.emory.cci.aiw.i2b2etl.table.FactHandler;
import edu.emory.cci.aiw.i2b2etl.table.InvalidFactException;
import edu.emory.cci.aiw.i2b2etl.table.ObservationFact;
import edu.emory.cci.aiw.i2b2etl.table.PatientDimension;
import edu.emory.cci.aiw.i2b2etl.table.PropositionFactHandler;
import edu.emory.cci.aiw.i2b2etl.table.ProviderDimension;
import edu.emory.cci.aiw.i2b2etl.table.ProviderFactHandler;
import edu.emory.cci.aiw.i2b2etl.table.VisitDimension;
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

import java.io.File;
import java.io.StringReader;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.protempa.dest.QueryResultsHandlerCloseException;

/**
 * @author Andrew Post
 */
public final class I2b2QueryResultsHandler extends AbstractQueryResultsHandler {
    private static final String[] OBX_FACT_IDXS = new String[]{"FACT_NOLOB", "FACT_PATCON_DATE_PRVD_IDX", "FACT_CNPT_PAT_ENCT_IDX"};

    private final Query query;
    private final KnowledgeSource knowledgeSource;
    private final I2b2Destination.DataInsertMode dataInsertMode;
    private final File confFile;
    private final boolean inferPropositionIdsNeeded;
    private final ConfigurationReader configurationReader;
    private final DictionarySection dictSection;
    private final DataSection obxSection;
    private final DatabaseSection databaseSection;
    private final ConnectionSpec dataConnectionSpec;
    private final ConceptsSection conceptsSection;
    private List<FactHandler> factHandlers;
    private Metadata ontologyModel;
    private HashSet<Object> propIdsFromKnowledgeSource;
    private final DataSection.DataSpec providerFullNameSpec;
    private final DataSection.DataSpec providerFirstNameSpec;
    private final DataSection.DataSpec providerMiddleNameSpec;
    private final DataSection.DataSpec providerLastNameSpec;
    private final ConnectionSpec metadataConnectionSpec;
    private final String visitPropId;
    private final String loadProviderHeirarchy;
    private Connection dataSchemaConnection;

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
    I2b2QueryResultsHandler(Query query, KnowledgeSource knowledgeSource, File confXML,
                            boolean inferPropositionIdsNeeded, I2b2Destination.DataInsertMode dataInsertMode)
            throws QueryResultsHandlerInitException {
        Logger logger = I2b2ETLUtil.logger();
        this.query = query;
        this.knowledgeSource = knowledgeSource;
        this.dataInsertMode = dataInsertMode;
        this.confFile = confXML;
        logger.log(Level.FINE, String.format("Using configuration file: %s",
                this.confFile.getAbsolutePath()));
        this.inferPropositionIdsNeeded = inferPropositionIdsNeeded;
        try {
            logger.log(Level.FINER, "STEP: read conf.xml");
            this.configurationReader = new ConfigurationReader(this.confFile);
            this.configurationReader.read();
            this.dictSection = this.configurationReader.getDictionarySection();

            this.obxSection = this.configurationReader.getDataSection();
            this.conceptsSection = this.configurationReader.getConceptsSection();
            this.databaseSection = this.configurationReader.getDatabaseSection();
        } catch (ConfigurationReadException ex) {
            throw new QueryResultsHandlerInitException(
                    "Could not initialize query results handler", ex);
        }
        DatabaseSection.DatabaseSpec dataSchemaSpec = this.databaseSection.get("dataschema");
        DatabaseSection.DatabaseSpec metadataSchemaSpec = this.databaseSection.get("metaschema");
        try {
            this.dataConnectionSpec = DatabaseAPI.DRIVERMANAGER.newConnectionSpecInstance(dataSchemaSpec.connect, dataSchemaSpec.user, dataSchemaSpec.passwd);
            this.metadataConnectionSpec = DatabaseAPI.DRIVERMANAGER.newConnectionSpecInstance(metadataSchemaSpec.connect, metadataSchemaSpec.user, metadataSchemaSpec.passwd);
        } catch (InvalidConnectionSpecArguments ex) {
            throw new QueryResultsHandlerInitException("Could not initialize query results handler", ex);
        }

        this.providerFullNameSpec = this.obxSection.get(this.dictSection.get("providerFullName"));
        this.providerFirstNameSpec = this.obxSection.get(this.dictSection.get("providerFirstName"));
        this.providerMiddleNameSpec = this.obxSection.get(this.dictSection.get("providerMiddleName"));
        this.providerLastNameSpec = this.obxSection.get(this.dictSection.get("providerLastName"));
        this.visitPropId = this.dictSection.get("visitDimension");
        if (this.inferPropositionIdsNeeded) {
            try {
                readPropIdsFromKnowledgeSource();
            } catch (KnowledgeSourceReadException ex) {
                throw new QueryResultsHandlerInitException("Could not initialize query results handler", ex);
            }
        }

        this.loadProviderHeirarchy = this.dictSection.get("loadProvidersTree");

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
            if (this.dataInsertMode == I2b2Destination.DataInsertMode.TRUNCATE) {
                new TableTruncater().doTruncate();
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
        return PatientDimension.TEMP_PATIENT_TABLE;
    }

    private String tempPatientMappingTableName() {
        return PatientDimension.TEMP_PATIENT_MAPPING_TABLE;
    }

    private String tempVisitTableName() {
        return VisitDimension.TEMP_VISIT_TABLE;
    }

    private String tempEncounterMappingTableName() {
        return VisitDimension.TEMP_ENC_MAPPING_TABLE;
    }

    private String tempProviderTableName() {
        return ProviderDimension.TEMP_PROVIDER_TABLE;
    }

    private String tempConceptTableName() {
        return ConceptDimension.TEMP_CONCEPT_TABLE;
    }

    private String tempObservationFactTableName() {
        return ObservationFact.TEMP_OBSERVATION_TABLE;
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

            call.setString(1, tempObservationFactTableName());
            call.execute();
        }
    }

    private void assembleFactHandlers() throws IllegalAccessException, InstantiationException, KnowledgeSourceReadException {
        this.factHandlers = new ArrayList<>();
        if (this.loadProviderHeirarchy == null || this.loadProviderHeirarchy.equals("true")) {
            addProviderFactHandler();
        }
        addPropositionFactHandlers();
    }

    @Override
    public void handleQueryResult(String keyId, List<Proposition> propositions, Map<Proposition, List<Proposition>> forwardDerivations, Map<Proposition, List<Proposition>> backwardDerivations, Map<UniqueId, Proposition> references) throws QueryResultsHandlerProcessingException {
        try {
            Set<Proposition> derivedPropositions = new HashSet<>();
            for (Proposition prop : propositions) {
                if (prop.getId().equals(this.visitPropId)) {
                    PatientDimension pd = this.ontologyModel.getPatient(keyId);
                    if (pd == null) {
                        pd = this.ontologyModel.addPatient(keyId, prop, this.dictSection, this.obxSection, references);
                    }
                    ProviderDimension provider =
                            this.ontologyModel.addProviderIfNeeded(
                                    prop,
                                    this.providerFullNameSpec != null ? this.providerFullNameSpec.referenceName : null,
                                    this.providerFullNameSpec != null ? this.providerFullNameSpec.propertyName : null,
                                    this.providerFirstNameSpec != null ? this.providerFirstNameSpec.referenceName : null,
                                    this.providerFirstNameSpec != null ? this.providerFirstNameSpec.propertyName : null,
                                    this.providerMiddleNameSpec != null ? this.providerMiddleNameSpec.referenceName : null,
                                    this.providerMiddleNameSpec != null ? this.providerMiddleNameSpec.propertyName : null,
                                    this.providerLastNameSpec != null ? this.providerLastNameSpec.referenceName : null,
                                    this.providerLastNameSpec != null ? this.providerLastNameSpec.propertyName : null,
                                    references);
                    VisitDimension vd = this.ontologyModel.addVisit(pd.getEncryptedPatientId(), pd.getEncryptedPatientIdSourceSystem(), (TemporalProposition) prop, this.dictSection, this.obxSection, references);
                    for (FactHandler factHandler : this.factHandlers) {
                        factHandler.handleRecord(pd, vd, provider, prop, forwardDerivations, backwardDerivations, references, this.knowledgeSource, derivedPropositions, this.dataSchemaConnection);
                    }
                }
            }
        } catch (InvalidConceptCodeException | InvalidFactException | InvalidPatientRecordException ex) {
            throw new QueryResultsHandlerProcessingException("Load into i2b2 failed for query " + this.query.getId(), ex);
        }
    }

    @Override
    public void finish() throws QueryResultsHandlerProcessingException {
        // upload_id for all the dimension table stored procedures
        final int UPLOAD_ID = 0;

        Logger logger = I2b2ETLUtil.logger();
        String queryId = this.query.getId();
        String projectName = this.dictSection.get("projectName");


        try {
            for (FactHandler factHandler : this.factHandlers) {
                factHandler.clearOut(this.dataSchemaConnection);
            }

            logger.log(Level.INFO, "Inserting ages into observation fact table for query {0}", queryId);

            this.dataSchemaConnection.close();
            this.dataSchemaConnection = null;

            if (this.loadProviderHeirarchy == null || this.loadProviderHeirarchy.equals("true")) {
                this.ontologyModel.buildProviderHierarchy();
            }
            // persist Patients & Visits.
            logger.log(Level.INFO, "Populating dimensions for query {0}", queryId);
            logger.log(Level.INFO, "Populating patient dimension for query {0}", queryId);
            try (Connection conn = openDataDatabaseConnection()) {
                PatientDimension.insertAll(this.ontologyModel.getPatients(),
                        conn, projectName);

                try (CallableStatement mappingCall = conn.prepareCall("{ call INSERT_PID_MAP_FROMTEMP(?, ?, ?) }")) {
                    mappingCall.setString(1, tempPatientMappingTableName());
                    mappingCall.setInt(2, UPLOAD_ID);
                    mappingCall.registerOutParameter(3, Types.VARCHAR);
                    mappingCall.execute();
                    logger.log(Level.INFO, "INSERT_PID_MAP_FROMTTEMP errmsg: {0}", mappingCall.getString(3));
                    //commit and rollback are called by stored procedure.
                }
            }

            try (Connection conn = openDataDatabaseConnection()) {
                VisitDimension.insertAll(this.ontologyModel.getVisits(),
                        conn, projectName);
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
            logger.log(Level.INFO, "Populating provider dimension for query {0}", queryId);
            try (Connection conn = openDataDatabaseConnection()) {
                ProviderDimension.insertAll(this.ontologyModel.getProviders(), conn);
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
            // flush hot concepts out of the tree. persist Concepts.
            logger.log(Level.INFO, "Populating concept dimension for query {0}", this.query.getId());
            try (Connection conn = openDataDatabaseConnection()) {
                ConceptDimension.insertAll(this.ontologyModel.getRoot(), conn);
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
        } catch (OntologyBuildException | SQLException | InvalidConceptCodeException ex) {
            logger.log(Level.SEVERE, "Load into i2b2 failed for query " + queryId, ex);
            throw new QueryResultsHandlerProcessingException("Load into i2b2 failed for query " + queryId, ex);
        }
    }

    @Override
    public void close() throws QueryResultsHandlerCloseException {
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
        this.ontologyModel = new Metadata(knowledgeSource, collectUserPropositionDefinitions(), this.dictSection.get("rootNodeName"), this.conceptsSection.getFolderSpecs(), dictSection, this.obxSection);
        setI2B2PathsToConcepts();
    }

    private void setI2B2PathsToConcepts() {
        Enumeration<Concept> emu = this.ontologyModel.getRoot().breadthFirstEnumeration();
        while (emu.hasMoreElements()) {
            Concept concept = emu.nextElement();
            Concept conceptFromCache = this.ontologyModel.getFromIdCache(concept.getId());
            if (conceptFromCache != null) {
                conceptFromCache.setHierarchyPath(concept.getI2B2Path());
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
        for (DataSection.DataSpec dataSpec : this.obxSection.getAll()) {
            if (dataSpec.referenceName != null) {
                ReferenceDefinition refDef = visitProp.referenceDefinition(dataSpec.referenceName);
                if (refDef == null) {
                    throw new KnowledgeSourceReadException("missing reference " + dataSpec.referenceName + " for proposition definition " + visitPropId + " for query " + this.query.getId());
                }
                org.arp.javautil.arrays.Arrays.addAll(this.propIdsFromKnowledgeSource, refDef.getPropositionIds());
            }
        }
        for (ConceptsSection.FolderSpec folderSpec : this.conceptsSection.getFolderSpecs()) {
            for (String proposition : folderSpec.propositions) {
                this.propIdsFromKnowledgeSource.add(proposition);
            }
        }
    }

    private void addProviderFactHandler() {
        ProviderFactHandler providerFactHandler = new ProviderFactHandler();
        this.factHandlers.add(providerFactHandler);
    }

    private void addPropositionFactHandlers() throws KnowledgeSourceReadException {
        String[] potentialDerivedPropIdsArr = this.ontologyModel.extractDerived();
        for (DataSection.DataSpec obx : this.obxSection.getAll()) {
            Link[] links;
            if (obx.referenceName != null) {
                links = new Link[]{new Reference(obx.referenceName)};
            } else {
                links = null;
            }

            PropositionFactHandler propFactHandler =
                    new PropositionFactHandler(links, obx.propertyName,
                            obx.start, obx.finish, obx.units,
                            potentialDerivedPropIdsArr, this.ontologyModel);
            this.factHandlers.add(propFactHandler);
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
        int counter = 0;
        int batchSize = 1000;
        int commitCounter = 0;
        int commitSize = 10000;
        String tableName = this.dictSection.get("metaTableName");
        int batchNumber = 0;
        Logger logger = I2b2ETLUtil.logger();
        try (Connection cn = openMetadataDatabaseConnection()) {
            logger.log(Level.FINE, "batch inserting on table {0}", tableName);
            try (PreparedStatement ps = cn.prepareStatement("insert into " + tableName + "(c_hlevel,c_fullname,c_name,c_synonym_cd,c_visualattributes,c_totalnum," +
                    "c_basecode,c_metadataxml,c_facttablecolumn,c_tablename,c_columnname,c_columndatatype,c_operator,c_dimcode,c_comment,c_tooltip," +
                    "update_date,download_Date,import_date,sourcesystem_cd,valuetype_cd,m_applied_path,m_exclusion_cd,c_path,c_symbol)" +
                    " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
                @SuppressWarnings(value = "unchecked")
                Enumeration<Concept> emu = model.getRoot().depthFirstEnumeration();
                /*
                 * A depth-first enumeration should traverse the hierarchies
                 * in the order in which they were created.
                 */
                Timestamp importTimestamp = new Timestamp(System.currentTimeMillis());
                Set<String> conceptCodes = new HashSet<>();
                while (emu.hasMoreElements()) {
                    Concept concept = emu.nextElement();
                    ps.setLong(1, concept.getLevel());
                    ps.setString(2, concept.getI2B2Path());
                    assert concept.getDisplayName() != null && concept.getDisplayName().length() > 0 : "concept " + concept.getConceptCode() + " (" + concept.getI2B2Path() + ") " + " has an invalid display name '" + concept.getDisplayName() + "'";
                    ps.setString(3, concept.getDisplayName());
                    String conceptCode = concept.getConceptCode();
                    ps.setString(4, SynonymCode.NOT_SYNONYM.getCode());
                    ps.setString(5, concept.getCVisualAttributes());
                    ps.setObject(6, null);
                    ps.setString(7, conceptCode);
                    // put labParmXml here
                    //
                    if (null == concept.getMetadataXml() || concept.getMetadataXml().isEmpty() || concept.getMetadataXml().equals("")) {
                        ps.setObject(8, null);
                    } else {
                        ps.setClob(8, new StringReader(concept.getMetadataXml()));
                    }
                    ps.setString(9, concept.getFactTableColumn()); //patient_num
                    ps.setString(10, concept.getTableName()); //patient_dimension
                    ps.setString(11, concept.getColumnName()); //birth_date
                    ps.setString(12, concept.getDataType().getCode());
                    ps.setString(13, concept.getOperator().getSQLOperator());// >, BETWEEN
                    ps.setString(14, concept.getDimCode()); //sysdate - (365.25*upperboundplusoneyear), sysdate - (365.25*upperboundplusoneyear) and sysdate - (365.25*lowerbound) 
                    ps.setObject(15, null);
                    ps.setString(16, concept.getDisplayName());
                    ps.setTimestamp(17, importTimestamp);
                    ps.setDate(18, null);
                    ps.setTimestamp(19, importTimestamp);
                    ps.setString(20, MetadataUtil.toSourceSystemCode(concept.getSourceSystemCode()));
                    ps.setString(21, concept.getValueTypeCode().getCode());
                    ps.setString(22, concept.getAppliedPath());
                    ps.setString(23, null);
                    ps.setString(24, null);
                    ps.setString(25, null);
                    ps.addBatch();
                    ps.clearParameters();
                    counter++;
                    commitCounter++;
                    if (counter >= batchSize) {
                        importTimestamp = new Timestamp(System.currentTimeMillis());
                        batchNumber++;
                        ps.executeBatch();
                        ps.clearBatch();
                        counter = 0;
                        logBatch(tableName, batchNumber);
                        if (logger.isLoggable(Level.FINE)) {
                            logger.log(Level.FINE, "loaded ontology batch {0}", batchNumber);
                        }
                    }
                    if (commitCounter >= commitSize) {
                        cn.commit();
                        commitCounter = 0;
                    }
                }
                if (counter > 0) {
                    batchNumber++;
                    ps.executeBatch();
                }
                if (commitCounter > 0) {
                    cn.commit();
                }
            }
            logBatch(tableName, batchNumber);
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Batch failed on OntologyTable " + tableName + ". I2B2 will not be correct.", e);
            throw e;
        }
    }

    private class TableTruncater {

        void doTruncate() throws SQLException {
            // Truncate the data tables
            // This is controlled by 'truncateTables' in conf.xml
            String truncateTables = dictSection.get("truncateTables");
            if (truncateTables == null || truncateTables.equalsIgnoreCase("true")) {
                // To do: table names should be parameterized in conf.xml and related to other data
                String queryId = query.getId();
                Logger logger = I2b2ETLUtil.logger();
                logger.log(Level.INFO, "Truncating data tables for query {0}", queryId);
                String[] dataschemaTables = {"OBSERVATION_FACT", "CONCEPT_DIMENSION", "PATIENT_DIMENSION", "PATIENT_MAPPING", "PROVIDER_DIMENSION", "VISIT_DIMENSION", "ENCOUNTER_MAPPING"};
                try (final Connection conn = openDataDatabaseConnection()) {
                    for (String tableName : dataschemaTables) {
                        truncateTable(conn, tableName);
                    }
                    logger.log(Level.INFO, "Done truncating data tables for query {0}", queryId);
                }
                logger.log(Level.INFO, "Truncating metadata tables for query {0}", queryId);
                try (final Connection conn = openMetadataDatabaseConnection()) {
                    truncateTable(conn, dictSection.get("metaTableName")); // metaTableName in conf.xml
                    logger.log(Level.INFO, "Done truncating metadata tables for query {0}", queryId);
                }
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
            stmt.setString(1, this.databaseSection.get("dataschema").user);
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
