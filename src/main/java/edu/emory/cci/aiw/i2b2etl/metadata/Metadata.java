/*
 * #%L
 * AIW i2b2 ETL
 * %%
 * Copyright (C) 2012 Emory University
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

import edu.emory.cci.aiw.i2b2etl.configuration.DictionarySection;
import edu.emory.cci.aiw.i2b2etl.configuration.ConceptsSection.FolderSpec;
import edu.emory.cci.aiw.i2b2etl.configuration.DataSection;
import edu.emory.cci.aiw.i2b2etl.configuration.DataSection.DataSpec;
import java.util.List;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;


import org.protempa.KnowledgeSource;
import edu.emory.cci.aiw.i2b2etl.table.PatientDimension;
import edu.emory.cci.aiw.i2b2etl.table.ProviderDimension;
import edu.emory.cci.aiw.i2b2etl.table.VisitDimension;
import java.io.*;
import java.sql.SQLException;
import java.util.*;
import org.apache.commons.collections.map.ReferenceMap;
import org.protempa.ConstantDefinition;
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
 *
 * a data file has a corresponding edu.emory.cci.registry.i2b2datareader handler
 * class. the meta information about the data files also have handlers in
 * edu.emory.cci.registry.i2b2metareader.
 *
 * currently, meta information is found in both the knowledgeSource AND in the
 * data files. the data is found in the data files created by PROTEMPA and a
 * file created by an SQL that fetches demographics data.
 *
 * there is a conf.xml file that declares how some of this process works. the
 * database & filesystem nodes point to database and file resources. files, once
 * declared, can be grouped into sets of files. the concept dimensions used in
 * i2b2 are declared in the meta node. each branch under the meta node is
 * created from data from the knowledgeSource, file(s), or de novo (hard-coded
 * in a java class).
 *
 * the metadatatree node declares how to assemble branches into the final
 * product for i2b2 (the metadata schema).
 *
 * lastly, the observation node declares what to process into observation_fact
 * entities in i2b2. this, along with the implicit Patient, Provider, Visit &
 * Concept dimensions are laoded into the data schema.
 *
 * the 'resources' folder in this project contains 'conf.xml' and 'demmeta.zip'.
 * the demmeta file needs pointing to from within the conf.xml file; it holds
 * information necessary to build out the demographics portion of the ontology
 * tree.
 *
 * the method doETL() orchestrates the entire process. the steps are commented
 * within that method.
 *
 * as little information as is possible is kept in memory so that the
 * observations get streamed from flat files, matched with in-memory data, and
 * batch-loaded into the database.
 */
public final class Metadata {
    
    private static final PropositionDefinition[] 
            EMPTY_PROPOSITION_DEFINITION_ARRAY =
            new PropositionDefinition[0];

    private final Concept rootConcept;
    private final Map<ConceptId, Concept> CACHE =
            new HashMap<ConceptId, Concept>();
    private final Map<List<Object>, ConceptId> conceptIdCache = new ReferenceMap();
    private final TreeMap<String, PatientDimension> patientCache =
            new TreeMap<String, PatientDimension>();
    private final TreeMap<Long, VisitDimension> visitCache =
            new TreeMap<Long, VisitDimension>();
    private final Set<String> conceptCodeCache = new HashSet<String>();
    private final KnowledgeSource knowledgeSource;
    private final Map<String, ProviderDimension> providers;
    private final DataSection dataSection;
    private final DictionarySection dictSection;
    private final PropositionDefinition[] userDefinedPropositionDefinitions;

    public Metadata(KnowledgeSource knowledgeSource,
            PropositionDefinition[] userDefinedPropositionDefinitions,
            String rootNodeDisplayName,
            FolderSpec[] folderSpecs,
            DictionarySection dictSection,
            DataSection dataSection) throws OntologyBuildException {
        if (knowledgeSource == null) {
            throw new IllegalArgumentException("knowledgeSource cannot be null");
        }
        if (dataSection == null) {
            throw new IllegalArgumentException("dataSource cannot be null");
        }
        if (folderSpecs == null) {
            throw new IllegalArgumentException("folderSpecs cannot be null");
        }
        if (userDefinedPropositionDefinitions == null) {
            this.userDefinedPropositionDefinitions = 
                    EMPTY_PROPOSITION_DEFINITION_ARRAY;
        } else {
            this.userDefinedPropositionDefinitions = 
                    userDefinedPropositionDefinitions.clone();
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
                MetadataUtil.toSourceSystemCode(
                I2B2QueryResultsHandlerSourceId.getInstance().getStringRepresentation()));
        this.providers = new HashMap<String, ProviderDimension>();
        this.dictSection = dictSection;
        this.dataSection = dataSection;
        
        Logger logger = MetadataUtil.logger();

        try {
            /*
             * Produce the ontology tree.
             */
            logger.log(Level.FINE, "STEP: construct tree");
            constructTreePre(folderSpecs.clone());

            buildDemographicsHierarchy();

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
        } catch (InvalidPromoteArgumentException ex) {
            throwOntologyBuildException(ex);
        } catch (InvalidConceptCodeException ex) {
            throwOntologyBuildException(ex);
        } catch (KnowledgeSourceReadException ex) {
            throwOntologyBuildException(ex);
        } catch (UnknownPropositionDefinitionException ex) {
            throwOntologyBuildException(ex);
        } catch (IOException ex) {
            throwOntologyBuildException(ex);
        } catch (SQLException ex) {
            throwOntologyBuildException(ex);
        }
    }

    private static void throwOntologyBuildException(Throwable ex) throws OntologyBuildException {
        throw new OntologyBuildException("Error building ontology", ex);
    }

    public Concept getRoot() {
        return this.rootConcept;
    }

    public Collection<PatientDimension> getPatients() {
        return patientCache.values();
    }

    public Collection<VisitDimension> getVisits() {
        return visitCache.values();
    }

    public ProviderDimension addProviderIfNeeded(Proposition encounterProp,
            String fullNameReference, String fullNameProperty,
            String firstNameReference, String firstNameProperty,
            String middleNameReference, String middleNameProperty,
            String lastNameReference, String lastNameProperty,
            Map<UniqueId, Proposition> references) {
        String firstName = getNamePart(resolveReference(encounterProp, firstNameReference, references), firstNameProperty);
        String middleName = getNamePart(resolveReference(encounterProp, middleNameReference, references), middleNameProperty);
        Proposition providerProp = resolveReference(encounterProp, lastNameReference, references);
        String lastName = getNamePart(providerProp, lastNameProperty);

        String fullName;
        if (lastName != null) {
            fullName = constructFullName(firstName, middleName, lastName);
        } else {
            providerProp = resolveReference(encounterProp, fullNameReference, references);
            fullName = getNamePart(providerProp, fullNameProperty);
        }

        ProviderDimension result = this.providers.get(fullName);
        if (result == null) {
            result = new ProviderDimension(
                    MetadataUtil.DEFAULT_CONCEPT_ID_PREFIX_INTERNAL + "|Provider:" + fullName,
                    fullName,
                    providerProp.getDataSourceType().getStringRepresentation());
            this.providers.put(fullName, result);
        }
        return result;


    }

    private Proposition resolveReference(Proposition encounterProp, String namePartReference, Map<UniqueId, Proposition> references) {
        Proposition provider;
        List<UniqueId> providerUIDs =
                encounterProp.getReferences(namePartReference);
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

    private String getNamePart(Proposition providerProposition, String firstNameProperty) {
        String firstName;
        if (providerProposition != null) {
            firstName = getProperty(firstNameProperty, providerProposition);
        } else {
            firstName = null;
        }
        return firstName;
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
        DemographicsConceptTreeBuilder builder = new DemographicsConceptTreeBuilder(this.knowledgeSource, this.dictSection, this.dataSection, this);
        this.rootConcept.add(builder.build());
    }

    public void buildProviderHierarchy() throws OntologyBuildException {
        ProviderConceptTreeBuilder builder = new ProviderConceptTreeBuilder(this.providers.values(), this);
        this.rootConcept.add(builder.build());
    }

    public Collection<ProviderDimension> getProviders() {
        return this.providers.values();
    }

    public PatientDimension addPatient(String keyId, Proposition encounterProp,
            DictionarySection dictSection,
            DataSection obxSection,
            Map<UniqueId, Proposition> references) throws InvalidPatientRecordException {
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
            Value val = prop.getProperty(dataSpec.propertyName);
            if (val != null) {
                PatientDimension patientDimension = this.patientCache.get(keyId);
                if (patientDimension == null) {
                    Value zipCode = getField(dictSection, obxSection,
                            "patientDimensionZipCode", encounterProp,
                            references);
                    Value maritalStatus = getField(dictSection, obxSection,
                            "patientDimensionMaritalStatus", encounterProp,
                            references);
                    Value ethnicity = getField(dictSection, obxSection,
                            "patientDimensionEthnicity", encounterProp,
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

                    Date now = new Date();
                    long ageInYears = AbsoluteTimeGranularity.YEAR.distance(
                            AbsoluteTimeGranularityUtil.asPosition(birthdate),
                            AbsoluteTimeGranularityUtil.asPosition(now),
                            AbsoluteTimeGranularity.YEAR,
                            AbsoluteTimeUnit.YEAR);

                    Concept ageConcept = getFromIdCache(null, null,
                            NumberValue.getInstance(ageInYears));
                    ageConcept.setInUse(true);

                    patientDimension = new PatientDimension(keyId,
                            zipCode != null ? zipCode.getFormatted() : null,
                            ageInYears,
                            gender != null ? gender.getFormatted() : null,
                            language != null ? language.getFormatted() : null,
                            religion != null ? religion.getFormatted() : null,
                            birthdate, null,
                            maritalStatus != null ? maritalStatus.getFormatted() : null,
                            ethnicity != null ? ethnicity.getFormatted() : null,
                            prop.getDataSourceType().getStringRepresentation());
                    this.patientCache.put(keyId, patientDimension);
                    return patientDimension;
                }
            } else {
                throw new InvalidPatientRecordException("Null patient MRN for encounter "
                        + encounterProp);
            }
        } else {
            throw new InvalidPatientRecordException("Multiple patient records in encounter "
                    + encounterProp);
        }
        return null;
    }

    public VisitDimension addVisit(long patientNum, String encryptedPatientId,
            String encryptedPatientIdSourceSystem,
            TemporalProposition encounterProp, DictionarySection dictSection,
            DataSection obxSection,
            Map<UniqueId, Proposition> references) {
        java.util.Date visitStartDate = AbsoluteTimeGranularityUtil.asDate(encounterProp.getInterval().getMinStart());
        java.util.Date visitEndDate = AbsoluteTimeGranularityUtil.asDate(encounterProp.getInterval().getMinFinish());
        Value encryptedId = getField(dictSection, obxSection, "visitDimensionDecipheredId", encounterProp, references);
        String encryptedIdStr;
        if (encryptedId != null) {
            encryptedIdStr = encryptedId.getFormatted();
        } else {
            encryptedIdStr = null;
        }
        VisitDimension vd = new VisitDimension(patientNum, encryptedPatientId,
                visitStartDate, visitEndDate, encryptedIdStr,
                encounterProp.getDataSourceType().getStringRepresentation(),
                encryptedPatientIdSourceSystem);
        visitCache.put(vd.getEncounterNum(), vd);
        return vd;
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
//        if (propId == null) {
//            throw new IllegalArgumentException("propId cannot be null");
//        }
        return getFromIdCache(
                ConceptId.getInstance(propId, propertyName, value, this));
    }

    public void addToIdCache(Concept concept) {
        this.CACHE.put(concept.getId(), concept);
    }

    public PatientDimension getPatient(String keyId) {
        return patientCache.get(keyId);
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

    public String[] extractDerived(PropositionDefinition[] propDefs)
            throws KnowledgeSourceReadException {
        Set<String> potentialDerivedConceptCodes = new HashSet<String>();

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
            String[] propIds =
                    new String[this.userDefinedPropositionDefinitions.length];
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
        ConceptId conceptId =
                ConceptId.getInstance(folderSpec.displayName, this);
        Concept concept = getFromIdCache(conceptId);
        if (concept == null) {
            concept =
                    new Concept(conceptId, folderSpec.conceptCodePrefix, this);
            concept.setSourceSystemCode(
                    MetadataUtil.toSourceSystemCode(
                    I2B2QueryResultsHandlerSourceId.getInstance().getStringRepresentation()));
            concept.setDisplayName(folderSpec.displayName);
            concept.setDataType(DataType.TEXT);
            addToIdCache(concept);
            this.rootConcept.add(concept);
        }
        Concept[] concepts;
        if (folderSpec.property == null) {
            PropositionConceptTreeBuilder propProxy =
                    new PropositionConceptTreeBuilder(this.knowledgeSource,
                    folderSpec.propositions, folderSpec.conceptCodePrefix,
                    folderSpec.valueType, this);
            concepts = propProxy.build();

        } else {
            ValueSetConceptTreeBuilder vsProxy =
                    new ValueSetConceptTreeBuilder(this.knowledgeSource,
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
