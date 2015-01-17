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

import edu.emory.cci.aiw.i2b2etl.configuration.ConceptsSection.FolderSpec;
import edu.emory.cci.aiw.i2b2etl.configuration.DataSection;
import edu.emory.cci.aiw.i2b2etl.configuration.DataSection.DataSpec;
import edu.emory.cci.aiw.i2b2etl.configuration.DictionarySection;
import edu.emory.cci.aiw.i2b2etl.table.ActiveStatusCode;
import edu.emory.cci.aiw.i2b2etl.table.PatientDimension;
import edu.emory.cci.aiw.i2b2etl.table.ProviderDimension;
import edu.emory.cci.aiw.i2b2etl.table.ProviderDimensionHandler;
import edu.emory.cci.aiw.i2b2etl.table.TableUtil;
import edu.emory.cci.aiw.i2b2etl.table.VisitDimension;
import edu.emory.cci.aiw.i2b2etl.table.VitalStatusCode;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;
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
import org.protempa.proposition.Proposition;
import org.protempa.proposition.TemporalProposition;
import org.protempa.proposition.UniqueId;
import org.protempa.proposition.value.AbsoluteTimeGranularity;
import org.protempa.proposition.value.AbsoluteTimeGranularityUtil;
import org.protempa.proposition.value.AbsoluteTimeUnit;
import org.protempa.proposition.value.DateValue;
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
    private static final String PROVIDER_ID_PREFIX = MetadataUtil.DEFAULT_CONCEPT_ID_PREFIX_INTERNAL + "|Provider:";
    private static final String NOT_RECORDED_PROVIDER_ID = PROVIDER_ID_PREFIX + "NotRecorded";
    private final Concept rootConcept;
    private final Map<ConceptId, Concept> CACHE
            = new HashMap<>();
    private final Map<List<Object>, ConceptId> conceptIdCache = new ReferenceMap<>();
    private final Set<String> conceptCodeCache = new HashSet<>();
    private final KnowledgeSource knowledgeSource;
    private final DataSection dataSection;
    private final DictionarySection dictSection;
    private final PropositionDefinition[] userDefinedPropositionDefinitions;
    private String qrhId;
    private final ProviderConceptTreeBuilder providerConceptTreeBuilder;

    public Metadata(String qrhId, KnowledgeSource knowledgeSource,
            PropositionDefinition[] userDefinedPropositionDefinitions,
            String rootNodeDisplayName,
            FolderSpec[] folderSpecs,
            DictionarySection dictSection,
            DataSection dataSection, boolean skipProviderHierarchy) throws OntologyBuildException {
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

            boolean skipDemographicsHierarchy = Boolean.parseBoolean(this.dictSection.get("skipDemographicsHierarchy"));
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

    public ProviderDimension addProviderIfNeeded(Proposition encounterProp,
            ProviderDimensionHandler providerDimensionHandler,
            String fullNameReference, String fullNameProperty,
            String firstNameReference, String firstNameProperty,
            String middleNameReference, String middleNameProperty,
            String lastNameReference, String lastNameProperty,
            Map<UniqueId, Proposition> references, ProviderDimension providerDimension) throws InvalidConceptCodeException, SQLException {
        Set<String> sources = new HashSet<>(4);

        String firstName = extractNamePart(firstNameReference, firstNameProperty, encounterProp, references, sources);
        String middleName = extractNamePart(middleNameReference, middleNameProperty, encounterProp, references, sources);
        String lastName = extractNamePart(lastNameReference, lastNameProperty, encounterProp, references, sources);
        String fullName = extractNamePart(fullNameReference, fullNameProperty, encounterProp, references, sources);
        if (fullName == null) {
            fullName = constructFullName(firstName, middleName, lastName);
        }

        String id;
        String source;
        if (!sources.isEmpty()) {
            id = PROVIDER_ID_PREFIX + fullName;
            source = MetadataUtil.toSourceSystemCode(StringUtils.join(sources, " & "));
        } else {
            id = NOT_RECORDED_PROVIDER_ID;
            source = MetadataUtil.toSourceSystemCode(this.qrhId);
            fullName = "Not Recorded";
        }
        ConceptId cid = ConceptId.getInstance(id, this);
        Concept concept = getFromIdCache(cid);
        boolean found = concept != null;
        if (!found) {
            concept = new Concept(cid, null, this);
            concept.setSourceSystemCode(source);
            concept.setDisplayName(fullName);
            concept.setDataType(DataType.TEXT);
            concept.setInUse(true);
        }
        providerDimension.setConcept(concept);
        providerDimension.setSourceSystem(source);
        if (this.providerConceptTreeBuilder != null && !found) {
            this.providerConceptTreeBuilder.add(providerDimension);
            providerDimensionHandler.insert(providerDimension);
        }
        return providerDimension;
    }

    private String extractNamePart(String namePartReference, String namePartProperty, Proposition encounterProp, Map<UniqueId, Proposition> references, Set<String> sources) {
        if (namePartReference != null && namePartProperty != null) {
            Proposition provider = resolveReference(encounterProp, namePartReference, references);
            extractSource(sources, provider);
            return getNamePart(provider, namePartProperty);
        } else {
            return null;
        }
    }

    private void extractSource(Set<String> sources, Proposition provider) {
        if (provider != null) {
            sources.add(provider.getSourceSystem().getStringRepresentation());
        }
    }

    private Proposition resolveReference(Proposition encounterProp, String namePartReference, Map<UniqueId, Proposition> references) {
        Proposition provider;
        List<UniqueId> providerUIDs
                = encounterProp.getReferences(namePartReference);
        int size = providerUIDs.size();
        if (size > 0) {
            if (size > 1) {
                Logger logger = MetadataUtil.logger();
                logger.log(Level.WARNING,
                        "Multiple providers found for {0}, using only the first one",
                        encounterProp);
            }
            provider = references.get(providerUIDs.get(0));
        } else {
            provider = null;
        }
        return provider;
    }

    private String getNamePart(Proposition provider, String namePartProperty) {
        String namePart;
        if (provider != null) {
            namePart = getProperty(namePartProperty, provider);
        } else {
            namePart = null;
        }
        return namePart;
    }

    private String getProperty(String nameProperty, Proposition provider) {
        String name;
        if (nameProperty != null) {
            Value firstNameVal = provider.getProperty(nameProperty);
            if (firstNameVal != null) {
                name = firstNameVal.getFormatted();
            } else {
                name = null;
            }
        } else {
            name = null;
        }
        return name;
    }

    private String constructFullName(String firstName, String middleName, String lastName) {
        StringBuilder result = new StringBuilder();
        if (lastName != null) {
            result.append(lastName);
        }
        result.append(", ");
        if (firstName != null) {
            result.append(firstName);
        }
        if (middleName != null) {
            if (firstName != null) {
                result.append(' ');
            }
            result.append(middleName);
        }
        return result.toString();
    }

    public void buildDemographicsHierarchy() throws OntologyBuildException {
        DemographicsConceptTreeBuilder builder = new DemographicsConceptTreeBuilder(this.qrhId, this.knowledgeSource, this.dictSection, this.dataSection, this);
        this.rootConcept.add(builder.build());
    }

    public PatientDimension addPatient(String keyId, Proposition encounterProp,
            DictionarySection dictSection,
            DataSection obxSection,
            Map<UniqueId, Proposition> references, PatientDimension patientDimension) throws InvalidPatientRecordException {
        String obxSectionStr = dictSection.get("patientDimensionMRN");
        DataSpec dataSpec = obxSection.get(obxSectionStr);
        List<UniqueId> uids = encounterProp.getReferences(dataSpec.referenceName);
        int size = uids.size();
        Logger logger = MetadataUtil.logger();
        if (size > 0) {
            if (size > 1) {
                logger.log(Level.WARNING,
                        "Multiple propositions with MRN property found for {0}, using only the first one",
                        encounterProp);
            }
            Proposition prop = references.get(uids.get(0));
            if (prop == null) {
                throw new InvalidPatientRecordException("Encounter's "
                        + dataSpec.referenceName
                        + " reference points to a non-existant proposition");
            }
            Value val = prop.getProperty(dataSpec.propertyName);
            if (val != null) {
                Value zipCode = getField(dictSection, obxSection,
                        "patientDimensionZipCode", encounterProp,
                        references);
                Value maritalStatus = getField(dictSection, obxSection,
                        "patientDimensionMaritalStatus", encounterProp,
                        references);
                Value race = getField(dictSection, obxSection,
                        "patientDimensionRace", encounterProp,
                        references);
                Value birthdateVal = getField(dictSection, obxSection,
                        "patientDimensionBirthdate", encounterProp,
                        references);
                Value gender = getField(dictSection, obxSection,
                        "patientDimensionGender", encounterProp,
                        references);
                Value language = getField(dictSection, obxSection,
                        "patientDimensionLanguage", encounterProp,
                        references);
                Value religion = getField(dictSection, obxSection,
                        "patientDimensionReligion", encounterProp,
                        references);
                Date birthdate;
                if (birthdateVal != null) {
                    try {
                        birthdate = ((DateValue) birthdateVal).getDate();
                    } catch (ClassCastException cce) {
                        birthdate = null;
                        logger.log(Level.WARNING, "Birthdate property value not a DateValue");
                    }
                } else {
                    birthdate = null;
                }

                Long ageInYears;
                if (birthdate != null) {
                    ageInYears = AbsoluteTimeGranularity.YEAR.distance(
                            AbsoluteTimeGranularityUtil.asPosition(birthdate),
                            AbsoluteTimeGranularityUtil.asPosition(new Date()),
                            AbsoluteTimeGranularity.YEAR,
                            AbsoluteTimeUnit.YEAR);
                    Concept ageConcept = getFromIdCache(null, null,
                            NumberValue.getInstance(ageInYears));
                    if (ageConcept != null) {
                        ageConcept.setInUse(true);
                    }
                } else {
                    ageInYears = null;
                }

                patientDimension.setEncryptedPatientId(keyId);
                patientDimension.setEncryptedPatientIdSource(prop.getSourceSystem().getStringRepresentation());
                patientDimension.setZip(zipCode != null ? zipCode.getFormatted() : null);
                patientDimension.setAgeInYears(ageInYears);
                patientDimension.setGender(gender != null ? gender.getFormatted() : null);
                patientDimension.setLanguage(language != null ? language.getFormatted() : null);
                patientDimension.setReligion(religion != null ? religion.getFormatted() : null);
                patientDimension.setBirthDate(TableUtil.setDateAttribute(birthdate));
                patientDimension.setDeathDate(null);
                patientDimension.setMaritalStatus(maritalStatus != null ? maritalStatus.getFormatted() : null);
                patientDimension.setRace(race != null ? race.getFormatted() : null);
                patientDimension.setSourceSystem(prop.getSourceSystem().getStringRepresentation());
                patientDimension.setVital(VitalStatusCode.getInstance(null));
                return patientDimension;
            } else {
                throw new InvalidPatientRecordException("Null patient MRN for encounter "
                        + encounterProp);
            }
        } else {
            throw new InvalidPatientRecordException("No patient dimension information for "
                    + encounterProp);
        }
    }

    public VisitDimension addVisit(String encryptedPatientId,
            String encryptedPatientIdSourceSystem,
            TemporalProposition encounterProp, DictionarySection dictSection,
            DataSection obxSection,
            Map<UniqueId, Proposition> references,
            VisitDimension visitDimension) {
        java.util.Date visitStartDate = encounterProp != null ? AbsoluteTimeGranularityUtil.asDate(encounterProp.getInterval().getMinStart()) : null;
        java.util.Date visitEndDate = encounterProp != null ? AbsoluteTimeGranularityUtil.asDate(encounterProp.getInterval().getMinFinish()) : null;
        Value encryptedId = encounterProp != null ? getField(dictSection, obxSection, "visitDimensionDecipheredId", encounterProp, references) : null;
        String encryptedIdStr;
        if (encryptedId != null) {
            encryptedIdStr = encryptedId.getFormatted();
        } else {
            encryptedIdStr = '@' + encryptedPatientId;
        }
        Date updateDate;
        if (encounterProp != null) {
            updateDate = encounterProp.getUpdateDate();
            if (updateDate == null) {
                updateDate = encounterProp.getCreateDate();
            }
        } else {
            updateDate = null;
        }

        visitDimension.setEncryptedPatientId(TableUtil.setStringAttribute(encryptedPatientId));
        visitDimension.setStartDate(TableUtil.setDateAttribute(visitStartDate));
        visitDimension.setEndDate(TableUtil.setDateAttribute(visitEndDate));
        visitDimension.setEncryptedVisitId(TableUtil.setStringAttribute(encryptedIdStr));
        visitDimension.setEncryptedVisitIdSourceSystem(encounterProp != null ? encounterProp.getSourceSystem().getStringRepresentation() : this.qrhId);
        visitDimension.setVisitSourceSystem(this.qrhId);
        visitDimension.setEncryptedPatientIdSourceSystem(encryptedPatientIdSourceSystem);
        visitDimension.setActiveStatus(ActiveStatusCode.getInstance(true, visitStartDate, visitEndDate));
        visitDimension.setDownloadDate(TableUtil.setTimestampAttribute(encounterProp != null ? encounterProp.getDownloadDate() : null));
        visitDimension.setUpdateDate(TableUtil.setTimestampAttribute(updateDate));
        return visitDimension;
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

    private static Value getField(DictionarySection dictSection,
            DataSection obxSection, String field,
            Proposition encounterProp, Map<UniqueId, Proposition> references) {
        Value val;
        String obxSectionStr = dictSection.get(field);
        if (obxSectionStr != null) {
            DataSpec obxSpec = obxSection.get(obxSectionStr);
            assert obxSpec.propertyName != null : "propertyName cannot be null";
            if (obxSpec != null) {
                if (obxSpec.referenceName != null) {
                    List<UniqueId> uids = encounterProp.getReferences(obxSpec.referenceName);
                    int size = uids.size();
                    if (size > 0) {
                        if (size > 1) {
                            Logger logger = MetadataUtil.logger();
                            logger.log(Level.WARNING,
                                    "Multiple propositions with {0} property found for {1}, using only the first one",
                                    new Object[]{field, encounterProp});
                        }
                        Proposition prop = references.get(uids.get(0));
                        val = prop.getProperty(obxSpec.propertyName);
                    } else {
                        val = null;
                    }
                } else {
                    val = encounterProp.getProperty(obxSpec.propertyName);
                }
            } else {
                throw new AssertionError("Invalid key referred to in " + field + ": " + obxSectionStr);
            }
        } else {
            val = null;
        }
        return val;
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

    private void constructTreePre(FolderSpec[] folderSpecs)
            throws IOException, SQLException, KnowledgeSourceReadException,
            UnknownPropositionDefinitionException, InvalidConceptCodeException,
            OntologyBuildException, InvalidPromoteArgumentException {
        for (FolderSpec folderSpec : folderSpecs) {
            processFolderSpec(folderSpec);
        }
        if (this.userDefinedPropositionDefinitions.length > 0) {
            FolderSpec folderSpec = new FolderSpec();
            folderSpec.displayName = "User-defined Derived Variables";
            folderSpec.userDefined = true;
            String[] propIds
                    = new String[this.userDefinedPropositionDefinitions.length];
            for (int i = 0;
                    i < this.userDefinedPropositionDefinitions.length;
                    i++) {
                propIds[i] = this.userDefinedPropositionDefinitions[i].getId();
            }
            folderSpec.propositions = propIds;
            folderSpec.valueType = null;
            folderSpec.skipGen = 0;
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
                = ConceptId.getInstance(folderSpec.displayName, this);
        Concept concept = getFromIdCache(conceptId);
        if (concept == null) {
            concept
                    = new Concept(conceptId, folderSpec.conceptCodePrefix, this);
            concept.setSourceSystemCode(
                    MetadataUtil.toSourceSystemCode(this.qrhId));
            concept.setDisplayName(folderSpec.displayName);
            concept.setDataType(DataType.TEXT);
            addToIdCache(concept);
            this.rootConcept.add(concept);
        }
        Concept[] concepts;
        if (folderSpec.property == null) {
            PropositionConceptTreeBuilder propProxy
                    = new PropositionConceptTreeBuilder(this.knowledgeSource,
                            folderSpec.propositions, folderSpec.conceptCodePrefix,
                            folderSpec.valueType, this);
            concepts = propProxy.build();

        } else {
            ValueSetConceptTreeBuilder vsProxy
                    = new ValueSetConceptTreeBuilder(this.knowledgeSource,
                            folderSpec.propositions, folderSpec.property,
                            folderSpec.conceptCodePrefix, this);
            concepts = vsProxy.build();
        }
        for (Concept c : concepts) {
            concept.add(c);
        }
        promote(concept, folderSpec.skipGen);
    }
}
