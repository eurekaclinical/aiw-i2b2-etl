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
package edu.emory.cci.aiw.i2b2etl.metadata;

import edu.emory.cci.aiw.i2b2etl.configuration.Data;
import edu.emory.cci.aiw.i2b2etl.configuration.FolderSpec;
import edu.emory.cci.aiw.i2b2etl.configuration.Settings;
import edu.emory.cci.aiw.i2b2etl.table.ProviderDimension;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.collections4.map.ReferenceMap;
import org.apache.commons.lang3.StringUtils;
import org.protempa.KnowledgeSource;
import org.protempa.KnowledgeSourceReadException;
import org.protempa.PropositionDefinition;
import org.protempa.proposition.value.NumberValue;
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
    
    private final Concept rootConcept;
    private final Map<ConceptId, Concept> CACHE
            = new HashMap<>();
    private final Map<List<Object>, ConceptId> conceptIdCache = new ReferenceMap<>();
    private final Set<String> conceptCodeCache = new HashSet<>();
    private final KnowledgeSource knowledgeSource;
    private final Data dataSection;
    private final Settings dictSection;
    private final PropositionDefinition[] userDefinedPropositionDefinitions;
    private String qrhId;
    private final ProviderConceptTreeBuilder providerConceptTreeBuilder;

    public Metadata(String qrhId, KnowledgeSource knowledgeSource,
            PropositionDefinition[] userDefinedPropositionDefinitions,
            String rootNodeDisplayName,
            FolderSpec[] folderSpecs,
            Settings dictSection,
            Data dataSection, boolean skipProviderHierarchy) throws OntologyBuildException {
        if (knowledgeSource == null) {
            throw new IllegalArgumentException("knowledgeSource cannot be null");
        }
        if (dataSection == null) {
            throw new IllegalArgumentException("dataSource cannot be null");
        }
        if (folderSpecs == null) {
            throw new IllegalArgumentException("folderSpecs cannot be null");
        }
        if (qrhId == null) {
            throw new IllegalArgumentException("qrhId cannot be null");
        }
        this.qrhId = qrhId;
        if (userDefinedPropositionDefinitions == null) {
            this.userDefinedPropositionDefinitions
                    = EMPTY_PROPOSITION_DEFINITION_ARRAY;
        } else {
            this.userDefinedPropositionDefinitions
                    = userDefinedPropositionDefinitions.clone();
        }
        this.knowledgeSource = knowledgeSource;
        try {
            this.rootConcept = new Concept(
                    ConceptId.getInstance(rootNodeDisplayName, this), null, this);
        } catch (InvalidConceptCodeException ex) {
            throw new OntologyBuildException("Could not build ontology", ex);
        }
        this.rootConcept.setDisplayName(rootNodeDisplayName);
        this.rootConcept.setDataType(DataType.TEXT);
        this.rootConcept.setSourceSystemCode(
                MetadataUtil.toSourceSystemCode(this.qrhId));
        this.dictSection = dictSection;
        this.dataSection = dataSection;

        Logger logger = MetadataUtil.logger();

        try {
            /*
             * Produce the ontology tree.
             */
            logger.log(Level.FINE, "STEP: construct tree");
            constructTreePre(folderSpecs);

            boolean skipDemographicsHierarchy = this.dictSection.getSkipDemographicsHierarchy();
            if (!skipDemographicsHierarchy) {
                buildDemographicsHierarchy();
            }

            //
            // at this point, the in-memory representation of the ontology is
            // complete.
            // the only other subsequent modifications happen to fields in a
            // node:
            //
            // the setInUse boolean is set when observation data is encountered
            // that relates
            // to the node. this is so the ConceptDimension gets populated with
            // concepts that
            // actually appear in the observations (see
            // ConceptDimension.insertAll()).
            //
            // the cacheKey & conceptCode are set under two conditions. see the
            // cache step
            // immediately after.
            //
            // ////
            // //// cache certain nodes of the ontology tree
            // ////
            logger.log(Level.FINE, "STEP: cache the proper ontology nodes within the ontology graph");

            // kludge to incorporate parent-of-leaf nodes for icd9d only
//            logger.log(Level.FINE, "STEP: kludge... cache icd9d parent-of-leaf");
//            captureICD9DParentNodesIntoCache();
            // the cache should be complete now.
            // sample and build the basic Patient-Visit-Provider relation.
            // these related objects hang around and are matched with each
            // observation_fact created. then they are inserted into the
            // data schema. the provider objects are also metadata... so they
            // exist as conceptNodes in the ontology.
            logger.log(Level.FINE, "STEP: sample patient-provider-visit relations");
            // TODO: fix hard-coded symbol

            //samplePatientsVisitsProviders(numberSignFileSetZero);
        } catch (InvalidPromoteArgumentException | SQLException | IOException | UnknownPropositionDefinitionException | KnowledgeSourceReadException | InvalidConceptCodeException ex) {
            throwOntologyBuildException(ex);
        }
        
        if (!skipProviderHierarchy) {
            this.providerConceptTreeBuilder = new ProviderConceptTreeBuilder(this);
            this.rootConcept.add(this.providerConceptTreeBuilder.build());
        } else {
            this.providerConceptTreeBuilder = null;
        }
    }

    private static void throwOntologyBuildException(Throwable ex) throws OntologyBuildException {
        throw new OntologyBuildException("Error building ontology", ex);
    }

    public Concept getRoot() {
        return this.rootConcept;
    }

    public ProviderDimension addProvider(
            ProviderDimension providerDimension) throws InvalidConceptCodeException, SQLException {
        if (this.providerConceptTreeBuilder != null) {
            this.providerConceptTreeBuilder.add(providerDimension);
        }
        return providerDimension;
    }

    public void buildDemographicsHierarchy() throws OntologyBuildException {
        DemographicsConceptTreeBuilder builder = new DemographicsConceptTreeBuilder(this.qrhId, this.knowledgeSource, this.dictSection, this);
        this.rootConcept.add(builder.build());
    }

    Concept getOrCreateHardCodedFolder(String... conceptIdSuffixes) throws InvalidConceptCodeException {
        String conceptIdSuffix = StringUtils.join(conceptIdSuffixes, '|');
        ConceptId conceptId = ConceptId.getInstance(MetadataUtil.DEFAULT_CONCEPT_ID_PREFIX_INTERNAL + "|" + conceptIdSuffix, this);
        Concept root = getFromIdCache(conceptId);
        if (root == null) {
            root = createHardCodedFolder(conceptIdSuffix, conceptIdSuffixes[conceptIdSuffixes.length - 1]);
        }
        return root;
    }

    private Concept createHardCodedFolder(String conceptIdSuffix, String displayName) throws InvalidConceptCodeException {
        ConceptId conceptId = ConceptId.getInstance(MetadataUtil.DEFAULT_CONCEPT_ID_PREFIX_INTERNAL + "|" + conceptIdSuffix, this);
        Concept root = new Concept(conceptId, null, this);
        root.setSourceSystemCode(MetadataUtil.toSourceSystemCode(this.qrhId));
        root.setDisplayName(displayName);
        root.setDataType(DataType.TEXT);
        addToIdCache(root);
        return root;
    }
    
    public void markAge(Long ageInYears) {
        Concept ageConcept = getFromIdCache(null, null,
                NumberValue.getInstance(ageInYears));
        if (ageConcept != null) {
            ageConcept.setInUse(true);
        }
    }

    public Concept getFromIdCache(ConceptId conceptId) {
        return this.CACHE.get(conceptId);
    }

    public Concept getFromIdCache(String propId, String propertyName, Value value) {
        return getFromIdCache(
                ConceptId.getInstance(propId, propertyName, value, this));
    }

    public void addToIdCache(Concept concept) {
        this.CACHE.put(concept.getId(), concept);
    }

    void putInConceptIdCache(List<Object> key, ConceptId conceptId) {
        this.conceptIdCache.put(key, conceptId);
    }

    ConceptId getFromConceptIdCache(List<Object> key) {
        return this.conceptIdCache.get(key);
    }

    void addToConceptCodeCache(String conceptCode) {
        this.conceptCodeCache.add(conceptCode);
    }

    boolean isInConceptCodeCache(String conceptCode) {
        return this.conceptCodeCache.contains(conceptCode);
    }

    public String[] extractDerived()
            throws KnowledgeSourceReadException {
        Set<String> potentialDerivedConceptCodes = new HashSet<>();

        @SuppressWarnings("unchecked")
        Enumeration<Concept> emu = getRoot().depthFirstEnumeration();

        while (emu.hasMoreElements()) {
            Concept concept = emu.nextElement();
            if (concept.isDerived()) {
                potentialDerivedConceptCodes.add(concept.getId().getPropositionId());
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
    
    public DimensionValueSetFolderBuilder newDimensionValueSetFolderBuilder(Concept root, String factTableColumn, String tableName) throws OntologyBuildException {
        return new DimensionValueSetFolderBuilder(root, this.knowledgeSource, this.dictSection, this.dataSection, this, this.qrhId, factTableColumn, tableName);
    }
    
    private void constructTreePre(FolderSpec[] folderSpecs)
            throws IOException, SQLException, KnowledgeSourceReadException,
            UnknownPropositionDefinitionException, InvalidConceptCodeException,
            OntologyBuildException, InvalidPromoteArgumentException {
        for (FolderSpec folderSpec : folderSpecs) {
            processFolderSpec(folderSpec);
        }
        if (this.userDefinedPropositionDefinitions.length > 0) {
            String[] propIds
                    = new String[this.userDefinedPropositionDefinitions.length];
            for (int i = 0;
                    i < this.userDefinedPropositionDefinitions.length;
                    i++) {
                propIds[i] = this.userDefinedPropositionDefinitions[i].getId();
            }
            FolderSpec folderSpec = new FolderSpec(
                    0,
                    "User-defined Derived Variables",
                    propIds,
                    null,
                    null,
                    null,
                    true,
                    null
            );
            processFolderSpec(folderSpec);
        }
    }

    /**
     * Mechanism to get rid of redundant, root-proximal nodes that are artifacts
     * of Protege (mostly).
     *
     * @param concept the root {@link Concept} of the tree or subtree.
     * @param ctr the number of levels to remove.
     */
    private static void promote(Concept concept, int ctr) throws InvalidPromoteArgumentException {
        if (ctr > 0) {
            if (concept.isLeaf()) {
                throw new InvalidPromoteArgumentException(concept);
            }
            Concept c = (Concept) concept.getChildAt(0);
            if (c == null) {
                return;
            }
            concept.removeAllChildren();
            while (c.getChildCount() != 0) {
                concept.add((Concept) c.getChildAt(0));
            }
            c.removeAllChildren();
            promote(concept, ctr - 1);
        }
    }

    private void processFolderSpec(FolderSpec folderSpec)
            throws InvalidConceptCodeException, KnowledgeSourceReadException,
            InvalidPromoteArgumentException,
            UnknownPropositionDefinitionException, OntologyBuildException {
        ConceptId conceptId
                = ConceptId.getInstance(folderSpec.getDisplayName(), this);
        Concept concept = getFromIdCache(conceptId);
        if (concept == null) {
            concept
                    = new Concept(conceptId, folderSpec.getConceptCodePrefix(), this);
            concept.setSourceSystemCode(
                    MetadataUtil.toSourceSystemCode(this.qrhId));
            concept.setDisplayName(folderSpec.getDisplayName());
            concept.setDataType(DataType.TEXT);
            addToIdCache(concept);
            this.rootConcept.add(concept);
        }
        Concept[] concepts;
        if (folderSpec.getProperty() == null) {
            PropositionConceptTreeBuilder propProxy
                    = new PropositionConceptTreeBuilder(this.knowledgeSource,
                            folderSpec.getPropositions(), folderSpec.getConceptCodePrefix(),
                            folderSpec.getValueType(), folderSpec.getModifiers(), this);
            concepts = propProxy.build();

        } else {
            ValueSetConceptTreeBuilder vsProxy
                    = new ValueSetConceptTreeBuilder(this.knowledgeSource,
                            folderSpec.getPropositions(), folderSpec.getProperty(),
                            folderSpec.getConceptCodePrefix(), this);
            concepts = vsProxy.build();
        }
        for (Concept c : concepts) {
            concept.add(c);
        }
        promote(concept, folderSpec.getSkipGen());
    }
}
