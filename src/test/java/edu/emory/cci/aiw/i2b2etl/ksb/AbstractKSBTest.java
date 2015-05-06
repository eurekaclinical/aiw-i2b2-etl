package edu.emory.cci.aiw.i2b2etl.ksb;

/*
 * #%L
 * AIW i2b2 ETL
 * %%
 * Copyright (C) 2012 - 2015 Emory University
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
import edu.emory.cci.aiw.i2b2etl.AbstractTest;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.arp.javautil.sql.DatabaseAPI;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.protempa.AbstractionDefinition;
import org.protempa.ContextDefinition;
import org.protempa.KnowledgeSourceReadException;
import org.protempa.PropertyDefinition;
import org.protempa.PropertyDefinitionBuilder;
import org.protempa.PropositionDefinition;
import org.protempa.TemporalPropositionDefinition;
import org.protempa.backend.BackendInitializationException;
import org.protempa.backend.BackendInstanceSpec;
import org.protempa.backend.Configuration;
import org.protempa.valueset.ValueSet;

/**
 *
 * @author Andrew Post
 */
public abstract class AbstractKSBTest extends AbstractTest {

    private static I2b2KnowledgeSourceBackend ksb;

    @BeforeClass
    public static void setUpClsAbstractKSBTest() throws Exception {
        Configuration config = getConfigFactory().getProtempaConfiguration();
        ksb = new I2b2KnowledgeSourceBackend();
        ksb.initialize(config.getKnowledgeSourceBackendSections().get(0));
    }
    
    @AfterClass
    public static void tearDownClsAbstractKSBTest() {
        ksb.close();
    }

    public static I2b2KnowledgeSourceBackend getKnowledgeSourceBackend() {
        return ksb;
    }

    public PropositionDefinition readPropositionDefinition(String propId) throws KnowledgeSourceReadException {
        return ksb.readPropositionDefinition(propId);
    }

    public DatabaseAPI getDatabaseApi() {
        return ksb.getDatabaseApi();
    }

    public void setDatabaseApi(DatabaseAPI databaseApi) {
        ksb.setDatabaseApi(databaseApi);
    }

    public String getTargetTable() {
        return ksb.getTargetTable();
    }

    public void setTargetTable(String tableName) {
        ksb.setTargetTable(tableName);
    }

    public void parseDatabaseApi(String databaseApiString) {
        ksb.parseDatabaseApi(databaseApiString);
    }

    public String getDatabaseId() {
        return ksb.getDatabaseId();
    }

    public void setDatabaseId(String databaseId) {
        ksb.setDatabaseId(databaseId);
    }

    public String getUsername() {
        return ksb.getUsername();
    }

    public void setUsername(String username) {
        ksb.setUsername(username);
    }

    public String getPassword() {
        return ksb.getPassword();
    }

    public void setPassword(String password) {
        ksb.setPassword(password);
    }

    public String getPatientPatientIdPropertyName() {
        return ksb.getPatientPatientIdPropertyName();
    }

    public void setPatientPatientIdPropertyName(String patientPatientIdPropertyName) {
        ksb.setPatientPatientIdPropertyName(patientPatientIdPropertyName);
    }

    public String getPatientDetailsPatientIdPropertyName() {
        return ksb.getPatientDetailsPatientIdPropertyName();
    }

    public void setPatientDetailsPatientIdPropertyName(String patientDetailsPatientIdPropertyName) {
        ksb.setPatientDetailsPatientIdPropertyName(patientDetailsPatientIdPropertyName);
    }

    public String getPatientPropositionId() {
        return ksb.getPatientPropositionId();
    }

    public void setPatientPropositionId(String patientPropositionId) {
        ksb.setPatientPropositionId(patientPropositionId);
    }

    public String getPatientDetailsPropositionId() {
        return ksb.getPatientDetailsPropositionId();
    }

    public void setPatientDetailsPropositionId(String patientDetailsPropositionId) {
        ksb.setPatientDetailsPropositionId(patientDetailsPropositionId);
    }

    public String getPatientDetailsDisplayName() {
        return ksb.getPatientDetailsDisplayName();
    }

    public void setPatientDetailsDisplayName(String patientDetailsDisplayName) {
        ksb.setPatientDetailsDisplayName(patientDetailsDisplayName);
    }

    public String getVisitPropositionId() {
        return ksb.getVisitPropositionId();
    }

    public void setVisitPropositionId(String visitPropositionId) {
        ksb.setVisitPropositionId(visitPropositionId);
    }

    public String getVisitDisplayName() {
        return ksb.getVisitDisplayName();
    }

    public void setVisitDisplayName(String visitDisplayName) {
        ksb.setVisitDisplayName(visitDisplayName);
    }

    public String getProviderPropositionId() {
        return ksb.getProviderPropositionId();
    }

    public void setProviderPropositionId(String providerPropositionId) {
        ksb.setProviderPropositionId(providerPropositionId);
    }

    public String getProviderDisplayName() {
        return ksb.getProviderDisplayName();
    }

    public void setProviderDisplayName(String providerDisplayName) {
        ksb.setProviderDisplayName(providerDisplayName);
    }

    public String getProviderNamePropertyName() {
        return ksb.getProviderNamePropertyName();
    }

    public void setProviderNamePropertyName(String providerNamePropertyName) {
        ksb.setProviderNamePropertyName(providerNamePropertyName);
    }

    public String getInoutPropertyName() {
        return ksb.getInoutPropertyName();
    }

    public void setInoutPropertyName(String inoutPropertyName) {
        ksb.setInoutPropertyName(inoutPropertyName);
    }

    public String getInoutPropertySource() {
        return ksb.getInoutPropertySource();
    }

    public void setInoutPropertySource(String inoutPropertySource) {
        ksb.setInoutPropertySource(inoutPropertySource);
    }

    public ValueSet getInoutPropertyValueSet() {
        return ksb.getInoutPropertyValueSet();
    }

    public void setInoutPropertyValueSet(ValueSet inoutPropertyValueSet) {
        ksb.setInoutPropertyValueSet(inoutPropertyValueSet);
    }

    public void parseInoutPropertyValueSet(String valueSetStr) {
        ksb.parseInoutPropertyValueSet(valueSetStr);
    }

    public String getInoutPropertyValueSetFile() {
        return ksb.getInoutPropertyValueSetFile();
    }

    public void setInoutPropertyValueSetFile(String inoutPropertyValueSetFile) {
        ksb.setInoutPropertyValueSetFile(inoutPropertyValueSetFile);
    }

    public Boolean getInoutPropertyValueSetHasHeader() {
        return ksb.getInoutPropertyValueSetHasHeader();
    }

    public void setInoutPropertyValueSetHasHeader(Boolean inoutPropertyValueSetHasHeader) {
        ksb.setInoutPropertyValueSetHasHeader(inoutPropertyValueSetHasHeader);
    }

    public String getInoutPropertyValueSetDelimiter() {
        return ksb.getInoutPropertyValueSetDelimiter();
    }

    public void setInoutPropertyValueSetDelimiter(String inoutPropertyValueSetDelimiter) {
        ksb.setInoutPropertyValueSetDelimiter(inoutPropertyValueSetDelimiter);
    }

    public Integer getInoutPropertyValueSetIdColumn() {
        return ksb.getInoutPropertyValueSetIdColumn();
    }

    public void setInoutPropertyValueSetIdColumn(Integer inoutPropertyValueSetIdColumn) {
        ksb.setInoutPropertyValueSetIdColumn(inoutPropertyValueSetIdColumn);
    }

    public Integer getInoutPropertyValueSetDisplayNameColumn() {
        return ksb.getInoutPropertyValueSetDisplayNameColumn();
    }

    public void setInoutPropertyValueSetDisplayNameColumn(Integer inoutPropertyValueSetDisplayNameColumn) {
        ksb.setInoutPropertyValueSetDisplayNameColumn(inoutPropertyValueSetDisplayNameColumn);
    }

    public Integer getInoutPropertyValueSetDescriptionColumn() {
        return ksb.getInoutPropertyValueSetDescriptionColumn();
    }

    public void setInoutPropertyValueSetDescriptionColumn(Integer inoutPropertyValueSetDescriptionColumn) {
        ksb.setInoutPropertyValueSetDescriptionColumn(inoutPropertyValueSetDescriptionColumn);
    }

    public String getVisitAgePropertyName() {
        return ksb.getVisitAgePropertyName();
    }

    public void setVisitAgePropertyName(String visitAgePropertyName) {
        ksb.setVisitAgePropertyName(visitAgePropertyName);
    }

    public String getGenderPropertyName() {
        return ksb.getGenderPropertyName();
    }

    public void setGenderPropertyName(String genderPropertyName) {
        ksb.setGenderPropertyName(genderPropertyName);
    }

    public ValueSet getGenderPropertyValueSet() {
        return ksb.getGenderPropertyValueSet();
    }

    public void setGenderPropertyValueSet(ValueSet genderPropertyValueSet) {
        ksb.setGenderPropertyValueSet(genderPropertyValueSet);
    }

    public void parseGenderPropertyValueSet(String valueSetStr) {
        ksb.parseGenderPropertyValueSet(valueSetStr);
    }

    public String getGenderPropertySource() {
        return ksb.getGenderPropertySource();
    }

    public void setGenderPropertySource(String genderPropertySource) {
        ksb.setGenderPropertySource(genderPropertySource);
    }

    public String getGenderPropertyValueSetFile() {
        return ksb.getGenderPropertyValueSetFile();
    }

    public void setGenderPropertyValueSetFile(String genderPropertyValueSetFile) {
        ksb.setGenderPropertyValueSetFile(genderPropertyValueSetFile);
    }

    public Boolean getGenderPropertyValueSetHasHeader() {
        return ksb.getGenderPropertyValueSetHasHeader();
    }

    public void setGenderPropertyValueSetHasHeader(Boolean genderPropertyValueSetHasHeader) {
        ksb.setGenderPropertyValueSetHasHeader(genderPropertyValueSetHasHeader);
    }

    public String getGenderPropertyValueSetDelimiter() {
        return ksb.getGenderPropertyValueSetDelimiter();
    }

    public void setGenderPropertyValueSetDelimiter(String genderPropertyValueSetDelimiter) {
        ksb.setGenderPropertyValueSetDelimiter(genderPropertyValueSetDelimiter);
    }

    public Integer getGenderPropertyValueSetIdColumn() {
        return ksb.getGenderPropertyValueSetIdColumn();
    }

    public void setGenderPropertyValueSetIdColumn(Integer genderPropertyValueSetIdColumn) {
        ksb.setGenderPropertyValueSetIdColumn(genderPropertyValueSetIdColumn);
    }

    public Integer getGenderPropertyValueSetDisplayNameColumn() {
        return ksb.getGenderPropertyValueSetDisplayNameColumn();
    }

    public void setGenderPropertyValueSetDisplayNameColumn(Integer genderPropertyValueSetDisplayNameColumn) {
        ksb.setGenderPropertyValueSetDisplayNameColumn(genderPropertyValueSetDisplayNameColumn);
    }

    public Integer getGenderPropertyValueSetDescriptionColumn() {
        return ksb.getGenderPropertyValueSetDescriptionColumn();
    }

    public void setGenderPropertyValueSetDescriptionColumn(Integer genderPropertyValueSetDescriptionColumn) {
        ksb.setGenderPropertyValueSetDescriptionColumn(genderPropertyValueSetDescriptionColumn);
    }

    public String getRacePropertyName() {
        return ksb.getRacePropertyName();
    }

    public void setRacePropertyName(String racePropertyName) {
        ksb.setRacePropertyName(racePropertyName);
    }

    public ValueSet getRacePropertyValueSet() {
        return ksb.getRacePropertyValueSet();
    }

    public void setRacePropertyValueSet(ValueSet racePropertyValueSet) {
        ksb.setRacePropertyValueSet(racePropertyValueSet);
    }

    public void parseRacePropertyValueSet(String valueSetStr) {
        ksb.parseRacePropertyValueSet(valueSetStr);
    }

    public String getRacePropertyValueSetFile() {
        return ksb.getRacePropertyValueSetFile();
    }

    public void setRacePropertyValueSetFile(String racePropertyValueSetFile) {
        ksb.setRacePropertyValueSetFile(racePropertyValueSetFile);
    }

    public Boolean getRacePropertyValueSetHasHeader() {
        return ksb.getRacePropertyValueSetHasHeader();
    }

    public void setRacePropertyValueSetHasHeader(Boolean racePropertyValueSetHasHeader) {
        ksb.setRacePropertyValueSetHasHeader(racePropertyValueSetHasHeader);
    }

    public String getRacePropertyValueSetDelimiter() {
        return ksb.getRacePropertyValueSetDelimiter();
    }

    public void setRacePropertyValueSetDelimiter(String racePropertyValueSetDelimiter) {
        ksb.setRacePropertyValueSetDelimiter(racePropertyValueSetDelimiter);
    }

    public Integer getRacePropertyValueSetIdColumn() {
        return ksb.getRacePropertyValueSetIdColumn();
    }

    public void setRacePropertyValueSetIdColumn(Integer racePropertyValueSetIdColumn) {
        ksb.setRacePropertyValueSetIdColumn(racePropertyValueSetIdColumn);
    }

    public Integer getRacePropertyValueSetDisplayNameColumn() {
        return ksb.getRacePropertyValueSetDisplayNameColumn();
    }

    public void setRacePropertyValueSetDisplayNameColumn(Integer racePropertyValueSetDisplayNameColumn) {
        ksb.setRacePropertyValueSetDisplayNameColumn(racePropertyValueSetDisplayNameColumn);
    }

    public Integer getRacePropertyValueSetDescriptionColumn() {
        return ksb.getRacePropertyValueSetDescriptionColumn();
    }

    public void setRacePropertyValueSetDescriptionColumn(Integer racePropertyValueSetDescriptionColumn) {
        ksb.setRacePropertyValueSetDescriptionColumn(racePropertyValueSetDescriptionColumn);
    }

    public String getRacePropertySource() {
        return ksb.getRacePropertySource();
    }

    public void setRacePropertySource(String racePropertySource) {
        ksb.setRacePropertySource(racePropertySource);
    }

    public String getLanguagePropertyName() {
        return ksb.getLanguagePropertyName();
    }

    public void setLanguagePropertyName(String languagePropertyName) {
        ksb.setLanguagePropertyName(languagePropertyName);
    }

    public ValueSet getLanguagePropertyValueSet() {
        return ksb.getLanguagePropertyValueSet();
    }

    public void setLanguagePropertyValueSet(ValueSet languagePropertyValueSet) {
        ksb.setLanguagePropertyValueSet(languagePropertyValueSet);
    }

    public void parseLanguagePropertyValueSet(String valueSetStr) {
        ksb.parseLanguagePropertyValueSet(valueSetStr);
    }

    public String getLanguagePropertyValueSetFile() {
        return ksb.getLanguagePropertyValueSetFile();
    }

    public void setLanguagePropertyValueSetFile(String languagePropertyValueSetFile) {
        ksb.setLanguagePropertyValueSetFile(languagePropertyValueSetFile);
    }

    public Boolean getLanguagePropertyValueSetHasHeader() {
        return ksb.getLanguagePropertyValueSetHasHeader();
    }

    public void setLanguagePropertyValueSetHasHeader(Boolean languagePropertyValueSetHasHeader) {
        ksb.setLanguagePropertyValueSetHasHeader(languagePropertyValueSetHasHeader);
    }

    public String getLanguagePropertyValueSetDelimiter() {
        return ksb.getLanguagePropertyValueSetDelimiter();
    }

    public void setLanguagePropertyValueSetDelimiter(String languagePropertyValueSetDelimiter) {
        ksb.setLanguagePropertyValueSetDelimiter(languagePropertyValueSetDelimiter);
    }

    public Integer getLanguagePropertyValueSetIdColumn() {
        return ksb.getLanguagePropertyValueSetIdColumn();
    }

    public void setLanguagePropertyValueSetIdColumn(Integer languagePropertyValueSetIdColumn) {
        ksb.setLanguagePropertyValueSetIdColumn(languagePropertyValueSetIdColumn);
    }

    public Integer getLanguagePropertyValueSetDisplayNameColumn() {
        return ksb.getLanguagePropertyValueSetDisplayNameColumn();
    }

    public void setLanguagePropertyValueSetDisplayNameColumn(Integer languagePropertyValueSetDisplayNameColumn) {
        ksb.setLanguagePropertyValueSetDisplayNameColumn(languagePropertyValueSetDisplayNameColumn);
    }

    public Integer getLanguagePropertyValueSetDescriptionColumn() {
        return ksb.getLanguagePropertyValueSetDescriptionColumn();
    }

    public void setLanguagePropertyValueSetDescriptionColumn(Integer languagePropertyValueSetDescriptionColumn) {
        ksb.setLanguagePropertyValueSetDescriptionColumn(languagePropertyValueSetDescriptionColumn);
    }

    public String getLanguagePropertySource() {
        return ksb.getLanguagePropertySource();
    }

    public void setLanguagePropertySource(String languagePropertySource) {
        ksb.setLanguagePropertySource(languagePropertySource);
    }

    public ValueSet getMaritalStatusPropertyValueSet() {
        return ksb.getMaritalStatusPropertyValueSet();
    }

    public void setMaritalStatusPropertyValueSet(ValueSet maritalStatusPropertyValueSet) {
        ksb.setMaritalStatusPropertyValueSet(maritalStatusPropertyValueSet);
    }

    public void parseMaritalStatusPropertyValueSet(String valueSetStr) {
        ksb.parseMaritalStatusPropertyValueSet(valueSetStr);
    }

    public String getMaritalStatusPropertyValueSetFile() {
        return ksb.getMaritalStatusPropertyValueSetFile();
    }

    public void setMaritalStatusPropertyValueSetFile(String maritalStatusPropertyValueSetFile) {
        ksb.setMaritalStatusPropertyValueSetFile(maritalStatusPropertyValueSetFile);
    }

    public Boolean getMaritalStatusPropertyValueSetHasHeader() {
        return ksb.getMaritalStatusPropertyValueSetHasHeader();
    }

    public void setMaritalStatusPropertyValueSetHasHeader(Boolean maritalStatusPropertyValueSetHasHeader) {
        ksb.setMaritalStatusPropertyValueSetHasHeader(maritalStatusPropertyValueSetHasHeader);
    }

    public String getMaritalStatusPropertyValueSetDelimiter() {
        return ksb.getMaritalStatusPropertyValueSetDelimiter();
    }

    public void setMaritalStatusPropertyValueSetDelimiter(String maritalStatusPropertyValueSetDelimiter) {
        ksb.setMaritalStatusPropertyValueSetDelimiter(maritalStatusPropertyValueSetDelimiter);
    }

    public Integer getMaritalStatusPropertyValueSetIdColumn() {
        return ksb.getMaritalStatusPropertyValueSetIdColumn();
    }

    public void setMaritalStatusPropertyValueSetIdColumn(Integer maritalStatusPropertyValueSetIdColumn) {
        ksb.setMaritalStatusPropertyValueSetIdColumn(maritalStatusPropertyValueSetIdColumn);
    }

    public Integer getMaritalStatusPropertyValueSetDisplayNameColumn() {
        return ksb.getMaritalStatusPropertyValueSetDisplayNameColumn();
    }

    public void setMaritalStatusPropertyValueSetDisplayNameColumn(Integer maritalStatusPropertyValueSetDisplayNameColumn) {
        ksb.setMaritalStatusPropertyValueSetDisplayNameColumn(maritalStatusPropertyValueSetDisplayNameColumn);
    }

    public Integer getMaritalStatusPropertyValueSetDescriptionColumn() {
        return ksb.getMaritalStatusPropertyValueSetDescriptionColumn();
    }

    public void setMaritalStatusPropertyValueSetDescriptionColumn(Integer maritalStatusPropertyValueSetDescriptionColumn) {
        ksb.setMaritalStatusPropertyValueSetDescriptionColumn(maritalStatusPropertyValueSetDescriptionColumn);
    }

    public String getMaritalStatusPropertySource() {
        return ksb.getMaritalStatusPropertySource();
    }

    public void setMaritalStatusPropertySource(String maritalStatusPropertySource) {
        ksb.setMaritalStatusPropertySource(maritalStatusPropertySource);
    }

    public String getMaritalStatusPropertyName() {
        return ksb.getMaritalStatusPropertyName();
    }

    public void setMaritalStatusPropertyName(String maritalStatusPropertyName) {
        ksb.setMaritalStatusPropertyName(maritalStatusPropertyName);
    }

    public String getReligionPropertyName() {
        return ksb.getReligionPropertyName();
    }

    public void setReligionPropertyName(String religionPropertyName) {
        ksb.setReligionPropertyName(religionPropertyName);
    }

    public ValueSet getReligionPropertyValueSet() {
        return ksb.getReligionPropertyValueSet();
    }

    public void setReligionPropertyValueSet(ValueSet religionPropertyValueSet) {
        ksb.setReligionPropertyValueSet(religionPropertyValueSet);
    }

    public void parseReligionPropertyValueSet(String valueSetStr) {
        ksb.parseReligionPropertyValueSet(valueSetStr);
    }

    public String getReligionPropertyValueSetFile() {
        return ksb.getReligionPropertyValueSetFile();
    }

    public void setReligionPropertyValueSetFile(String religionPropertyValueSetFile) {
        ksb.setReligionPropertyValueSetFile(religionPropertyValueSetFile);
    }

    public Boolean getReligionPropertyValueSetHasHeader() {
        return ksb.getReligionPropertyValueSetHasHeader();
    }

    public void setReligionPropertyValueSetHasHeader(Boolean religionPropertyValueSetHasHeader) {
        ksb.setReligionPropertyValueSetHasHeader(religionPropertyValueSetHasHeader);
    }

    public String getReligionPropertyValueSetDelimiter() {
        return ksb.getReligionPropertyValueSetDelimiter();
    }

    public void setReligionPropertyValueSetDelimiter(String religionPropertyValueSetDelimiter) {
        ksb.setReligionPropertyValueSetDelimiter(religionPropertyValueSetDelimiter);
    }

    public Integer getReligionPropertyValueSetIdColumn() {
        return ksb.getReligionPropertyValueSetIdColumn();
    }

    public void setReligionPropertyValueSetIdColumn(Integer religionPropertyValueSetIdColumn) {
        ksb.setReligionPropertyValueSetIdColumn(religionPropertyValueSetIdColumn);
    }

    public Integer getReligionPropertyValueSetDisplayNameColumn() {
        return ksb.getReligionPropertyValueSetDisplayNameColumn();
    }

    public void setReligionPropertyValueSetDisplayNameColumn(Integer religionPropertyValueSetDisplayNameColumn) {
        ksb.setReligionPropertyValueSetDisplayNameColumn(religionPropertyValueSetDisplayNameColumn);
    }

    public Integer getReligionPropertyValueSetDescriptionColumn() {
        return ksb.getReligionPropertyValueSetDescriptionColumn();
    }

    public void setReligionPropertyValueSetDescriptionColumn(Integer religionPropertyValueSetDescriptionColumn) {
        ksb.setReligionPropertyValueSetDescriptionColumn(religionPropertyValueSetDescriptionColumn);
    }

    public String getReligionPropertySource() {
        return ksb.getReligionPropertySource();
    }

    public void setReligionPropertySource(String religionPropertySource) {
        ksb.setReligionPropertySource(religionPropertySource);
    }

    public String getDateOfBirthPropertyName() {
        return ksb.getDateOfBirthPropertyName();
    }

    public void setDateOfBirthPropertyName(String dateOfBirthPropertyName) {
        ksb.setDateOfBirthPropertyName(dateOfBirthPropertyName);
    }

    public String getAgeInYearsPropertyName() {
        return ksb.getAgeInYearsPropertyName();
    }

    public void setAgeInYearsPropertyName(String ageInYearsPropertyName) {
        ksb.setAgeInYearsPropertyName(ageInYearsPropertyName);
    }

    public String getDateOfDeathPropertyName() {
        return ksb.getDateOfDeathPropertyName();
    }

    public void setDateOfDeathPropertyName(String dateOfDeathPropertyName) {
        ksb.setDateOfDeathPropertyName(dateOfDeathPropertyName);
    }

    public String getVitalStatusPropertyName() {
        return ksb.getVitalStatusPropertyName();
    }

    public void setVitalStatusPropertyName(String vitalStatusPropertyName) {
        ksb.setVitalStatusPropertyName(vitalStatusPropertyName);
    }

    public ConceptProperty[] getConceptProperties() {
        return ksb.getConceptProperties();
    }

    public void setConceptProperties(ConceptProperty[] conceptProperties) {
        ksb.setConceptProperties(conceptProperties);
    }

    public void parseConceptProperties(String conceptProperties) {
        ksb.parseConceptProperties(conceptProperties);
    }

    public List<PropositionDefinition> readPropositionDefinitions(String[] ids) throws KnowledgeSourceReadException {
        return ksb.readPropositionDefinitions(ids);
    }

    public String[] readIsA(String propId) throws KnowledgeSourceReadException {
        return ksb.readIsA(propId);
    }

    public Set<String> getKnowledgeSourceSearchResults(String searchKey) throws KnowledgeSourceReadException {
        return ksb.getKnowledgeSourceSearchResults(searchKey);
    }

    public void initialize(BackendInstanceSpec config) throws BackendInitializationException {
        ksb.initialize(config);
    }

    public AbstractionDefinition readAbstractionDefinition(String id) throws KnowledgeSourceReadException {
        return ksb.readAbstractionDefinition(id);
    }

    public List<AbstractionDefinition> readAbstractionDefinitions(String[] ids) throws KnowledgeSourceReadException {
        return ksb.readAbstractionDefinitions(ids);
    }

    public ContextDefinition readContextDefinition(String id) throws KnowledgeSourceReadException {
        return ksb.readContextDefinition(id);
    }

    public List<ContextDefinition> readContextDefinitions(String[] toArray) throws KnowledgeSourceReadException {
        return ksb.readContextDefinitions(toArray);
    }

    public TemporalPropositionDefinition readTemporalPropositionDefinition(String id) throws KnowledgeSourceReadException {
        return ksb.readTemporalPropositionDefinition(id);
    }

    public List<TemporalPropositionDefinition> readTemporalPropositionDefinitions(String[] ids) throws KnowledgeSourceReadException {
        return ksb.readTemporalPropositionDefinitions(ids);
    }

    public String[] readAbstractedInto(String propId) throws KnowledgeSourceReadException {
        return ksb.readAbstractedInto(propId);
    }

    public String[] readInduces(String propId) throws KnowledgeSourceReadException {
        return ksb.readInduces(propId);
    }

    public String[] readSubContextOfs(String propId) throws KnowledgeSourceReadException {
        return ksb.readSubContextOfs(propId);
    }

    public ValueSet readValueSet(String id) throws KnowledgeSourceReadException {
        return ksb.readValueSet(id);
    }

    public Collection<String> collectPropIdDescendantsUsingAllNarrower(boolean inDataSourceOnly, String[] propIds) throws KnowledgeSourceReadException {
        return ksb.collectPropIdDescendantsUsingAllNarrower(inDataSourceOnly, propIds);
    }

    public Collection<String> collectPropIdDescendantsUsingInverseIsA(String[] propIds) throws KnowledgeSourceReadException {
        return ksb.collectPropIdDescendantsUsingInverseIsA(propIds);
    }

    public Collection<PropositionDefinition> collectPropDefDescendantsUsingAllNarrower(boolean inDataSourceOnly, String[] propIds) throws KnowledgeSourceReadException {
        return ksb.collectPropDefDescendantsUsingAllNarrower(inDataSourceOnly, propIds);
    }

    public Collection<PropositionDefinition> collectPropDefDescendantsUsingInverseIsA(String[] propIds) throws KnowledgeSourceReadException {
        return ksb.collectPropDefDescendantsUsingInverseIsA(propIds);
    }

    public Set<String> toPropId(Set<PropositionDefinition> actualPropDef) {
        Set<String> actual = new HashSet<>();
        for (PropositionDefinition propDef : actualPropDef) {
            actual.add(propDef.getId());
        }
        return actual;
    }

    public Set<PropertyDefinitionBuilder> collectPropertyDefinitionBuilders(PropositionDefinition propDef) {
        return collectPropertyDefinitionBuilders(Collections.singleton(propDef));
    }

    public Set<PropertyDefinitionBuilder> collectPropertyDefinitionBuilders(Collection<PropositionDefinition> propDefs) {
        Set<PropertyDefinitionBuilder> propertyDefBuilders = new HashSet<>();
        for (PropositionDefinition propDef : propDefs) {
            for (PropertyDefinition pd : propDef.getPropertyDefinitions()) {
                propertyDefBuilders.add(new PropertyDefinitionBuilder(pd));
            }
        }
        return propertyDefBuilders;
    }

}
