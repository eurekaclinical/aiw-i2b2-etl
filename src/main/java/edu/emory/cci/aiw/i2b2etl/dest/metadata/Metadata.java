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
package edu.emory.cci.aiw.i2b2etl.dest.metadata;

import edu.emory.cci.aiw.i2b2etl.dest.metadata.conceptid.InvalidConceptCodeException;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.conceptid.ConceptId;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.conceptid.PropDefConceptId;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.conceptid.SimpleConceptId;
import edu.emory.cci.aiw.i2b2etl.dest.config.Data;
import edu.emory.cci.aiw.i2b2etl.dest.config.FolderSpec;
import edu.emory.cci.aiw.i2b2etl.dest.config.Settings;
import edu.emory.cci.aiw.i2b2etl.dest.table.ProviderDimension;
import edu.emory.cci.aiw.i2b2etl.ksb.TableAccessReader;
import edu.emory.cci.aiw.i2b2etl.ksb.QueryConstructor;
import edu.emory.cci.aiw.i2b2etl.ksb.QueryExecutor;
import edu.emory.cci.aiw.i2b2etl.ksb.ResultSetReader;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.tree.TreeNode;

import org.apache.commons.collections4.map.ReferenceMap;
import org.apache.commons.lang3.StringUtils;
import org.arp.javautil.arrays.Arrays;
import org.arp.javautil.collections.Collections;
import org.arp.javautil.sql.ConnectionSpec;
import org.protempa.KnowledgeSource;
import org.protempa.KnowledgeSourceCache;
import org.protempa.KnowledgeSourceReadException;
import org.protempa.PropositionDefinition;
import org.protempa.proposition.value.Value;

/**
 * Controls the etl process for extracting data from files and a
 * knowledgeSource(s) for transform and load into an i2b2 instance. It is
 * single-threaded and should be called only once per process.
 * <p/>
 * a data file has a corresponding edu.emory.cci.registry.i2b2datareader handler
 * class. the meta information about the data files also have handlers in
 * edu.emory.cci.registry.i2b2metareader.
 * <p/>
 * currently, meta information is found in both the knowledgeSource AND in the
 * data files. the data is found in the data files created by PROTEMPA and a
 * file created by an SQL that fetches demographics data.
 * <p/>
 * there is a conf.xml file that declares how some of this process works. the
 * database & filesystem nodes point to database and file resources. files, once
 * declared, can be grouped into sets of files. the concept dimensions used in
 * i2b2 are declared in the meta node. each branch under the meta node is
 * created from data from the knowledgeSource, file(s), or de novo (hard-coded
 * in a java class).
 * <p/>
 * the metadatatree node declares how to assemble branches into the final
 * product for i2b2 (the metadata schema).
 * <p/>
 * lastly, the observation node declares what to process into observation_fact
 * entities in i2b2. this, along with the implicit Patient, Provider, Visit &
 * Concept dimensions are laoded into the data schema.
 * <p/>
 * the 'resources' folder in this project contains 'conf.xml' and 'demmeta.zip'.
 * the demmeta file needs pointing to from within the conf.xml file; it holds
 * information necessary to build out the demographics portion of the ontology
 * tree.
 * <p/>
 * the method doETL() orchestrates the entire process. the steps are commented
 * within that method.
 * <p/>
 * as little information as is possible is kept in memory so that the
 * observations get streamed from flat files, matched with in-memory data, and
 * batch-loaded into the database.
 */
public final class Metadata {

    private static final PropositionDefinition[] EMPTY_PROPOSITION_DEFINITION_ARRAY
            = new PropositionDefinition[0];
    private static final Logger LOGGER = Logger.getLogger(Metadata.class.getName());

    private Concept conceptRoot;
    private final Map<ConceptId, Concept> conceptCache = new HashMap<>();
    private final Map<List<Object>, ConceptId> conceptIdCache = new ReferenceMap<>();
    private final Data dataSection;
    private final Settings settings;
    private final PropositionDefinition[] userDefinedPropositionDefinitions;
    private final String sourceSystemCode;
    private final ProviderConceptTreeBuilder providerConceptTreeBuilder;
    private final KnowledgeSourceCache cache;
    private final FolderSpec[] folderSpecs;
    private final List<Concept> modifierRoots;
    private final List<Concept> allRoots;
    private ConnectionSpec metaConnectionSpec;

    /**
     *
     * @param sourceSystemCode source system code {@link String} to use for
     * concepts created by this query results handler. It is truncated, if
     * necessary, to 50 characters to fit into the i2b2 database tables. Cannot
     * be <code>null</code>.
     * @param cache
     * @param knowledgeSource
     * @param userDefinedPropositionDefinitions
     * @param folderSpecs
     * @param settings
     * @param dataSection
     * @throws OntologyBuildException
     */
    public Metadata(String sourceSystemCode, KnowledgeSourceCache cache, KnowledgeSource knowledgeSource,
            PropositionDefinition[] userDefinedPropositionDefinitions,
            FolderSpec[] folderSpecs,
            Settings settings,
            Data dataSection, ConnectionSpec metaConnectionSpec) throws OntologyBuildException {
        if (knowledgeSource == null) {
            throw new IllegalArgumentException("knowledgeSource cannot be null");
        }
        if (dataSection == null) {
            throw new IllegalArgumentException("dataSource cannot be null");
        }
        if (folderSpecs == null) {
            throw new IllegalArgumentException("folderSpecs cannot be null");
        }
        if (sourceSystemCode == null) {
            throw new IllegalArgumentException("sourceSystemCode cannot be null");
        }
        if (cache == null) {
            throw new IllegalArgumentException("cache cannot be null");
        }
        this.modifierRoots = new ArrayList<>();
        this.sourceSystemCode = MetadataUtil.toSourceSystemCode(sourceSystemCode);
        if (userDefinedPropositionDefinitions == null) {
            this.userDefinedPropositionDefinitions
                    = EMPTY_PROPOSITION_DEFINITION_ARRAY;
        } else {
            this.userDefinedPropositionDefinitions
                    = userDefinedPropositionDefinitions.clone();
        }
        this.cache = cache;
        String rootNodeDisplayName = settings.getRootNodeName();
        if (rootNodeDisplayName != null) {
            try {
                this.conceptRoot = new Concept(
                        SimpleConceptId.getInstance(rootNodeDisplayName, this), null, this);
            } catch (InvalidConceptCodeException ex) {
                throw new OntologyBuildException("Could not build ontology", ex);
            }
            this.conceptRoot.setDisplayName(rootNodeDisplayName);
            this.conceptRoot.setDataType(DataType.TEXT);
            this.conceptRoot.setSourceSystemCode(this.sourceSystemCode);
        }
        this.settings = settings;
        this.dataSection = dataSection;
        this.folderSpecs = folderSpecs.clone();
        this.allRoots = new ArrayList<>();
        if (rootNodeDisplayName != null) {
            this.allRoots.add(this.conceptRoot);
        }
        this.providerConceptTreeBuilder = new ProviderConceptTreeBuilder(this);
        try {
            constructTreePre();

            SubtreeBuilder[] builders = {
                new PhenotypesBuilder(this.cache, this),
                new DemographicsBuilder(this.cache, this),
                this.providerConceptTreeBuilder
            };
            for (SubtreeBuilder builder : builders) {
                builder.build(this.conceptRoot);
                if (this.conceptRoot == null) {
                    Concept[] builderRoot = builder.getRoots();
                    Arrays.addAll(this.allRoots, builderRoot);
                }
            }
        } catch (InvalidPromoteArgumentException | SQLException | IOException | UnknownPropositionDefinitionException | KnowledgeSourceReadException | InvalidConceptCodeException ex) {
            throwOntologyBuildException(ex);
        }

        this.metaConnectionSpec = metaConnectionSpec;

        setI2B2PathsToConcepts();
        assert !this.allRoots.contains(null) : "Null root concepts! " + this.allRoots;
    }
    
    public ConnectionSpec getMetaConnectionSpec() {
        return this.metaConnectionSpec;
    }

    public Settings getSettings() {
        return settings;
    }

    public Data getDataSection() {
        return dataSection;
    }

    public Concept getConceptRoot() {
        return this.conceptRoot;
    }

    public void addModifierRoot(Concept concept) {
        if (concept != null) {
            this.modifierRoots.add(concept);
            this.allRoots.add(concept);
        }
    }

    public Concept[] getModifierRoots() {
        return this.modifierRoots.toArray(new Concept[this.modifierRoots.size()]);
    }

    public Concept[] getAllRoots() {
        return this.allRoots.toArray(new Concept[this.allRoots.size()]);
    }

    public PropositionDefinition[] getPhenotypeDefinitions() {
        return userDefinedPropositionDefinitions.clone();
    }

    /**
     * Returns the source system code used for concepts created by this query
     * results handler.
     *
     * @return a {@link String}, maximum 50 characters. Guaranteed not
     * <code>null</code>.
     */
    public String getSourceSystemCode() {
        return this.sourceSystemCode;
    }

    public FolderSpec[] getFolderSpecs() {
        return this.folderSpecs.clone();
    }

    public ProviderDimension addProvider(
            ProviderDimension providerDimension) throws InvalidConceptCodeException, SQLException {
        this.providerConceptTreeBuilder.add(providerDimension);
        return providerDimension;
    }

    Concept newContainerConcept(String displayName, String conceptCode) throws OntologyBuildException {
        ConceptId conceptId = SimpleConceptId.getInstance(displayName, this);
        Concept concept = newConcept(conceptId, conceptCode, getSourceSystemCode());
        concept.setCVisualAttributes("CAE");
        concept.setDisplayName(displayName);
        return concept;
    }

    Concept getOrCreateHardCodedFolder(String... conceptIdSuffixes) throws InvalidConceptCodeException {
        String conceptIdSuffix = StringUtils.join(conceptIdSuffixes, '|');
        ConceptId conceptId = SimpleConceptId.getInstance(MetadataUtil.DEFAULT_CONCEPT_ID_PREFIX_INTERNAL + "|" + conceptIdSuffix, this);
        Concept root = getFromIdCache(conceptId);
        if (root == null) {
            root = createHardCodedFolder(conceptIdSuffix, conceptIdSuffixes[conceptIdSuffixes.length - 1]);
        }
        return root;
    }

    private Concept createHardCodedFolder(String conceptIdSuffix, String displayName) throws InvalidConceptCodeException {
        ConceptId conceptId = SimpleConceptId.getInstance(MetadataUtil.DEFAULT_CONCEPT_ID_PREFIX_INTERNAL + "|" + conceptIdSuffix, this);
        Concept root = new Concept(conceptId, null, this);
        root.setSourceSystemCode(this.sourceSystemCode);
        root.setDisplayName(displayName);
        root.setDataType(DataType.TEXT);
        addToIdCache(root);
        return root;
    }

    public Concept getFromIdCache(ConceptId conceptId) {
        synchronized (this.conceptCache) {
            return this.conceptCache.get(conceptId);
        }
    }

    public Concept getFromIdCache(String propId, String propertyName, Value value) {
        return getFromIdCache(
                PropDefConceptId.getInstance(propId, propertyName, value, this));
    }

    public void addToIdCache(Concept concept) {
        synchronized (this.conceptCache) {
            if (!this.conceptCache.containsKey(concept.getId())) {
                this.conceptCache.put(concept.getId(), concept);
            } else {
                throw new IllegalArgumentException("concept already added!");
            }
        }
    }

    public void putInConceptIdCache(List<Object> key, ConceptId conceptId) {
        synchronized (this.conceptIdCache) {
            if (!this.conceptIdCache.containsKey(key)) {
                this.conceptIdCache.put(key, conceptId);
            } else {
                throw new IllegalArgumentException("concept id already added!");
            }
        }
    }

    public ConceptId getFromConceptIdCache(List<Object> key) {
        synchronized (this.conceptIdCache) {
            return this.conceptIdCache.get(key);
        }
    }

    public String[] extractDerived()
            throws KnowledgeSourceReadException {
        Set<String> potentialDerivedConceptCodes = new HashSet<>();

        for (Concept r : getAllRoots()) {
            Enumeration<Concept> emu = r.depthFirstEnumeration();
            while (emu.hasMoreElements()) {
                Concept concept = emu.nextElement();
                if (concept.isDerived()) {
                    potentialDerivedConceptCodes.add(concept.getId().getId());
                }
            }
        }

        return potentialDerivedConceptCodes.toArray(
                new String[potentialDerivedConceptCodes.size()]);
    }

    public Concept newConcept(ConceptId conceptId, String conceptCodePrefix, String sourceSystemCode) throws OntologyBuildException {
        Concept concept = getFromIdCache(conceptId);
        if (concept == null) {
            try {
                concept = new Concept(conceptId, conceptCodePrefix, this);
            } catch (InvalidConceptCodeException ex) {
                throw new OntologyBuildException("Error building ontology", ex);
            }
            concept.setSourceSystemCode(MetadataUtil.toSourceSystemCode(sourceSystemCode));
            concept.setDisplayName(concept.getConceptCode());
            addToIdCache(concept);
        } else {
            throw new OntologyBuildException(
                    "Duplicate concept: " + concept.getConceptCode());
        }
        return concept;
    }

    private void constructTreePre()
            throws IOException, SQLException, KnowledgeSourceReadException,
            UnknownPropositionDefinitionException, InvalidConceptCodeException,
            OntologyBuildException, InvalidPromoteArgumentException {
        for (FolderSpec folderSpec : this.folderSpecs) {
            processFolderSpec(folderSpec);
        }
    }

    private void processFolderSpec(FolderSpec folderSpec)
            throws InvalidConceptCodeException, KnowledgeSourceReadException,
            InvalidPromoteArgumentException,
            UnknownPropositionDefinitionException, OntologyBuildException {
        if (folderSpec.getProperty() == null) {
            PropositionConceptTreeBuilder propProxy
                    = new PropositionConceptTreeBuilder(this.cache,
                            folderSpec.getPropositions(), folderSpec.getConceptCodePrefix(),
                            folderSpec.getValueType(), folderSpec.getModifiers(),
                            folderSpec.isAlreadyLoaded(), this);
            propProxy.build(this.conceptRoot);
            if (this.conceptRoot == null) {
                Arrays.addAll(this.allRoots, propProxy.getRoots());
            }
        } else {
            for (String propId : folderSpec.getPropositions()) {
                ConceptId conceptId
                        = PropDefConceptId.getInstance(propId, null, this);
                Concept concept = getFromIdCache(conceptId);
                if (concept == null) {
                    concept
                            = new Concept(conceptId, folderSpec.getConceptCodePrefix(), this);
                    concept.setSourceSystemCode(this.sourceSystemCode);
                    PropositionDefinition propDef = this.cache.get(propId);
                    if (propDef != null) {
                        concept.setDisplayName(propDef.getDisplayName());
                    } else {
                        throw new UnknownPropositionDefinitionException(propId);
                    }
                    concept.setDataType(DataType.TEXT);
                    concept.setAlreadyLoaded(folderSpec.isAlreadyLoaded());
                    addToIdCache(concept);
                    if (this.conceptRoot != null) {
                        this.conceptRoot.add(concept);
                    } else {
                        this.allRoots.add(concept);
                    }
                }
                ValueSetConceptTreeBuilder vsProxy
                        = new ValueSetConceptTreeBuilder(this.cache,
                                folderSpec.getPropositions(), folderSpec.getProperty(),
                                folderSpec.getConceptCodePrefix(), this);
                vsProxy.build(concept);
            }
        }
    }
    
    private static final QueryConstructor ALL_CONCEPTS_QUERY = new QueryConstructor() {

        @Override
        public void appendStatement(StringBuilder sql, String table) {
            sql.append("SELECT DISTINCT EK_UNIQUE_ID, C_FULLNAME FROM ");
            sql.append(table);
        }
    };

    private void setI2B2PathsToConcepts() throws OntologyBuildException {
        Map<String, List<String>> result;
        if (this.metaConnectionSpec != null) {
            try (QueryExecutor qe = new QueryExecutor(this.metaConnectionSpec.getOrCreate(), ALL_CONCEPTS_QUERY, new TableAccessReader(this.settings.getMetaTableName()))) {
                result = qe.execute(new ResultSetReader<Map<String, List<String>>>() {

                    @Override
                    public Map<String, List<String>> read(ResultSet rs) throws KnowledgeSourceReadException {
                        Map<String, List<String>> result = new HashMap<>();
                        if (rs != null) {
                            try {
                                while (rs.next()) {
                                    Collections.putList(result, rs.getString(1), rs.getString(2));
                                }
                            } catch (SQLException ex) {
                                throw new KnowledgeSourceReadException(ex);
                            }
                        }
                        return result;
                    }
                });
            } catch (KnowledgeSourceReadException | SQLException ex) {
                throw new OntologyBuildException(ex);
            }
        } else {
            result = new HashMap<>();
        }
        for (Concept c : getAllRoots()) {
            Enumeration<Concept> emu = c.preorderEnumeration();
            boolean isInPhenotypes = false;
            while (emu.hasMoreElements()) {
                Concept concept = emu.nextElement();
                TreeNode parent = concept.getParent();
                if (parent != null && parent.equals(c)) {
                    isInPhenotypes = false;
                }
                if (concept.getSymbol().equals("AIW|Phenotypes")) {
                    isInPhenotypes = true;
                }
                Concept conceptFromCache = getFromIdCache(concept.getId());
                if (conceptFromCache != null && (isInPhenotypes || !result.containsKey(conceptFromCache.getSymbol()))) {
                    conceptFromCache.addHierarchyPath(concept.getFullName());
                }
                if (conceptFromCache != null) {
                    List<String> get = result.get(concept.getSymbol());
                    if (get != null) {
                        for (String hp : get) {
                            conceptFromCache.addHierarchyPath(hp);
                        }
                    }
                }
            }
        }
    }

    private static void throwOntologyBuildException(Throwable ex) throws OntologyBuildException {
        throw new OntologyBuildException("Error building ontology", ex);
    }
}
