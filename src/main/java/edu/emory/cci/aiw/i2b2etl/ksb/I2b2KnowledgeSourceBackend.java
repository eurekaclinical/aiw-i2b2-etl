package edu.emory.cci.aiw.i2b2etl.ksb;

/*
 * #%L
 * Protempa BioPortal Knowledge Source Backend
 * %%
 * Copyright (C) 2012 - 2014 Emory University
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
import au.com.bytecode.opencsv.CSVReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import org.apache.commons.lang3.ArrayUtils;
import org.arp.javautil.sql.DatabaseAPI;
import org.protempa.AbstractionDefinition;
import org.protempa.ContextDefinition;
import org.protempa.EventDefinition;
import org.protempa.KnowledgeSourceReadException;
import org.protempa.PropositionDefinition;
import org.protempa.TemporalPropositionDefinition;
import org.protempa.backend.AbstractCommonsKnowledgeSourceBackend;
import org.protempa.backend.annotations.BackendInfo;
import org.protempa.backend.annotations.BackendProperty;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.arp.javautil.arrays.Arrays;
import org.arp.javautil.collections.Collections;
import org.arp.javautil.io.IOUtil;
import org.protempa.AbstractPropositionDefinition;
import org.protempa.ConstantDefinition;
import org.protempa.PrimitiveParameterDefinition;
import org.protempa.PropertyDefinition;
import org.protempa.ProtempaUtil;
import org.protempa.ReferenceDefinition;
import org.protempa.ValueSet;
import org.protempa.ValueSet.ValueSetElement;
import org.protempa.backend.BackendInitializationException;
import org.protempa.backend.BackendInstanceSpec;
import org.protempa.proposition.value.NominalValue;
import org.protempa.proposition.value.ValueType;
import org.xml.sax.SAXParseException;
import org.xml.sax.XMLReader;

/**
 * Implements a knowledge source backend based on a database export of BioPortal
 * ontologies. Since BioPortal only has standard ontologies (eg, ICD-9, CPT,
 * LOINC), all of the KnowledgeSourceBackend methods that look for higher level
 * abstractions return null or empty arrays. All terms in BioPortal ontologies
 * are assumed to be {@link org.protempa.EventDefinition} objects in Protempa.
 */
@BackendInfo(displayName = "I2b2 Knowledge Source Backend")
public class I2b2KnowledgeSourceBackend extends AbstractCommonsKnowledgeSourceBackend {

    private static final Logger logger = Logger.getLogger(I2b2KnowledgeSourceBackend.class.getName());
    private static final char DEFAULT_DELIMITER = '\t';
    private static final ConceptProperty[] DEFAULT_CONCEPT_PROPERTIES_ARR = new ConceptProperty[0];
    private static final Properties visitPropositionProperties;
    private static final Properties patientDetailsPropositionProperties;
    private static final Properties providerPropositionProperties;
    private final static String[] VALUE_TYPE_CDS = {"LAB", "DOC"};
    private static final String defaultPatientDetailsPropositionId;
    private static final ValueSet defaultLanguagePropertyValueSet;
    private static final ValueSet defaultMaritalStatusPropertyValueSet;
    private static final ValueSet defaultReligionPropertyValueSet;
    private static final ValueSet defaultGenderPropertyValueSet;
    private static final ValueSet defaultRacePropertyValueSet;
    private static final ValueSet defaultInoutPropertyValueSet;
    private static final String defaultVisitPropositionId;
    
    static {
        try {
            visitPropositionProperties = IOUtil.loadPropertiesFromResource(I2b2KnowledgeSourceBackend.class, "/visitProposition.properties");
            patientDetailsPropositionProperties = IOUtil.loadPropertiesFromResource(I2b2KnowledgeSourceBackend.class, "/patientDetailsProposition.properties");
            providerPropositionProperties = IOUtil.loadPropertiesFromResource(I2b2KnowledgeSourceBackend.class, "/providerProposition.properties");
        } catch (IOException ex) {
            throw new AssertionError("Can't find dimension proposition properties file: " + ex.getMessage());
        }
        defaultPatientDetailsPropositionId = patientDetailsPropositionProperties.getProperty("propositionId");
        defaultLanguagePropertyValueSet = parseValueSetResource(
                defaultPatientDetailsPropositionId, 
                patientDetailsPropositionProperties.getProperty("language.valueSet.resource.name"),
                patientDetailsPropositionProperties.getProperty("language.valueSet.resource.hasHeader"),
                patientDetailsPropositionProperties.getProperty("language.valueSet.resource.delimiter"),
                patientDetailsPropositionProperties.getProperty("language.valueSet.resource.column.id"),
                patientDetailsPropositionProperties.getProperty("language.valueSet.resource.column.displayName"),
                patientDetailsPropositionProperties.getProperty("language.valueSet.resource.column.description"));
        defaultMaritalStatusPropertyValueSet = parseValueSetResource(
                defaultPatientDetailsPropositionId, 
                patientDetailsPropositionProperties.getProperty("maritalStatus.valueSet.resource.name"),
                patientDetailsPropositionProperties.getProperty("maritalStatus.valueSet.resource.hasHeader"),
                patientDetailsPropositionProperties.getProperty("maritalStatus.valueSet.resource.delimiter"),
                patientDetailsPropositionProperties.getProperty("maritalStatus.valueSet.resource.column.id"),
                patientDetailsPropositionProperties.getProperty("maritalStatus.valueSet.resource.column.displayName"),
                patientDetailsPropositionProperties.getProperty("maritalStatus.valueSet.resource.column.description"));
        defaultReligionPropertyValueSet = parseValueSetResource(
                defaultPatientDetailsPropositionId, 
                patientDetailsPropositionProperties.getProperty("religion.valueSet.resource.name"),
                patientDetailsPropositionProperties.getProperty("religion.valueSet.resource.hasHeader"),
                patientDetailsPropositionProperties.getProperty("religion.valueSet.resource.delimiter"),
                patientDetailsPropositionProperties.getProperty("religion.valueSet.resource.column.id"),
                patientDetailsPropositionProperties.getProperty("religion.valueSet.resource.column.displayName"),
                patientDetailsPropositionProperties.getProperty("religion.valueSet.resource.column.description"));
        defaultGenderPropertyValueSet = parseValueSet(defaultPatientDetailsPropositionId, patientDetailsPropositionProperties.getProperty("gender.valueSet.values"));
        defaultRacePropertyValueSet = parseValueSet(defaultPatientDetailsPropositionId, patientDetailsPropositionProperties.getProperty("race.valueSet.values"));
        
        defaultVisitPropositionId = visitPropositionProperties.getProperty("propositionId");
        defaultInoutPropertyValueSet = parseValueSet(defaultVisitPropositionId, visitPropositionProperties.getProperty("inout.valueSet.values"));
        
    }

    private ValueMetadataSupport valueMetadataSupport;
    private final QuerySupport querySupport;
    private final LevelReader levelReader;
    private final ConceptPropertyReader conceptPropertyReader;
    private String patientDetailsPropositionId;
    private String visitPropositionId;
    private String providerPropositionId;
    
    private String providerNamePropertyName;
    private String inoutPropertyName;
    private String inoutPropertySource;
    private ValueSet inoutPropertyValueSet;
    private String inoutPropertyValueSetFile;
    private boolean inoutPropertyValueSetHasHeader;
    private char inoutPropertyValueSetDelimiter;
    private int inoutPropertyValueSetIdColumn;
    private Integer inoutPropertyValueSetDisplayNameColumn;
    private Integer inoutPropertyValueSetDescriptionColumn;
    private String visitAgePropertyName;
    private String genderPropertyName;
    private ValueSet genderPropertyValueSet;
    private String genderPropertySource;
    private String genderPropertyValueSetFile;
    private boolean genderPropertyValueSetHasHeader;
    private char genderPropertyValueSetDelimiter;
    private int genderPropertyValueSetIdColumn;
    private Integer genderPropertyValueSetDisplayNameColumn;
    private Integer genderPropertyValueSetDescriptionColumn;
    private String racePropertyName;
    private ValueSet racePropertyValueSet;
    private String racePropertySource;
    private String racePropertyValueSetFile;
    private boolean racePropertyValueSetHasHeader;
    private char racePropertyValueSetDelimiter;
    private int racePropertyValueSetIdColumn;
    private Integer racePropertyValueSetDisplayNameColumn;
    private Integer racePropertyValueSetDescriptionColumn;
    private String languagePropertyName;
    private ValueSet languagePropertyValueSet;
    private String languagePropertySource;
    private String languagePropertyValueSetFile;
    private boolean languagePropertyValueSetHasHeader;
    private char languagePropertyValueSetDelimiter;
    private int languagePropertyValueSetIdColumn;
    private Integer languagePropertyValueSetDisplayNameColumn;
    private Integer languagePropertyValueSetDescriptionColumn;
    private ValueSet maritalStatusPropertyValueSet;
    private String maritalStatusPropertySource;
    private String maritalStatusPropertyName;
    private String maritalStatusPropertyValueSetFile;
    private boolean maritalStatusPropertyValueSetHasHeader;
    private char maritalStatusPropertyValueSetDelimiter;
    private int maritalStatusPropertyValueSetIdColumn;
    private Integer maritalStatusPropertyValueSetDisplayNameColumn;
    private Integer maritalStatusPropertyValueSetDescriptionColumn;
    private String religionPropertyName;
    private ValueSet religionPropertyValueSet;
    private String religionPropertySource;
    private String religionPropertyValueSetFile;
    private boolean religionPropertyValueSetHasHeader;
    private char religionPropertyValueSetDelimiter;
    private int religionPropertyValueSetIdColumn;
    private Integer religionPropertyValueSetDisplayNameColumn;
    private Integer religionPropertyValueSetDescriptionColumn;
    private String dateOfBirthPropertyName;
    private String ageInYearsPropertyName;
    private String dateOfDeathPropertyName;
    private String vitalStatusPropertyName;
    private String patientDetailsDisplayName;
    private String visitDisplayName;
    private String providerDisplayName;
    private ConceptProperty[] conceptProperties;
    private Map<String, ValueSet> valueSets;
    
    public I2b2KnowledgeSourceBackend() {
        this.querySupport = new QuerySupport();
        this.levelReader = new LevelReader(this.querySupport);
        this.conceptPropertyReader = new ConceptPropertyReader(this.querySupport);
        this.conceptProperties = DEFAULT_CONCEPT_PROPERTIES_ARR;
        this.valueSets = new HashMap<>();
        
        /**
         * Patient
         */
        this.patientDetailsPropositionId = defaultPatientDetailsPropositionId;
        this.patientDetailsDisplayName = patientDetailsPropositionProperties.getProperty("displayName");
        
        this.genderPropertyName = patientDetailsPropositionProperties.getProperty("gender.propertyName");
        this.genderPropertyValueSet = defaultGenderPropertyValueSet;
        if (this.genderPropertyValueSet != null) {
            this.valueSets.put(this.genderPropertyValueSet.getId(), this.genderPropertyValueSet);
        }
        this.genderPropertySource = patientDetailsPropositionProperties.getProperty("gender.valueSet.source");
        
        this.racePropertyName = patientDetailsPropositionProperties.getProperty("race.propertyName");
        this.racePropertyValueSet = defaultRacePropertyValueSet;
        if (this.racePropertyValueSet != null) {
            this.valueSets.put(this.racePropertyValueSet.getId(), this.racePropertyValueSet);
        }
        this.racePropertySource = patientDetailsPropositionProperties.getProperty("race.valueSet.source");
        
        this.languagePropertyName = patientDetailsPropositionProperties.getProperty("language.propertyName");
        this.languagePropertyValueSet = defaultLanguagePropertyValueSet;
        if (this.languagePropertyValueSet != null) {
            this.valueSets.put(this.languagePropertyValueSet.getId(), this.languagePropertyValueSet);
        }
        this.languagePropertySource = patientDetailsPropositionProperties.getProperty("language.valueSet.source");
        
        this.maritalStatusPropertyName = patientDetailsPropositionProperties.getProperty("maritalStatus.propertyName");
        this.maritalStatusPropertyValueSet = defaultMaritalStatusPropertyValueSet;
        if (this.maritalStatusPropertyValueSet != null) {
            this.valueSets.put(this.maritalStatusPropertyValueSet.getId(), this.maritalStatusPropertyValueSet);
        }
        this.maritalStatusPropertySource = patientDetailsPropositionProperties.getProperty("maritalStatus.valueSet.source");
        
        this.religionPropertyName = patientDetailsPropositionProperties.getProperty("religion.propertyName");
        this.religionPropertyValueSet = defaultReligionPropertyValueSet;
        if (this.religionPropertyValueSet != null) {
            this.valueSets.put(this.religionPropertyValueSet.getId(), this.religionPropertyValueSet);
        }
        this.religionPropertySource = patientDetailsPropositionProperties.getProperty("religion.valueSet.source");
        
        this.dateOfBirthPropertyName = patientDetailsPropositionProperties.getProperty("dateOfBirth.propertyName");
        
        this.ageInYearsPropertyName = patientDetailsPropositionProperties.getProperty("ageInYears.propertyName");
        
        this.dateOfDeathPropertyName = patientDetailsPropositionProperties.getProperty("dateOfDeath.propertyName");
        
        this.vitalStatusPropertyName = patientDetailsPropositionProperties.getProperty("vitalStatus.propertyName");
        
        /**
         * Visit
         */
        this.visitPropositionId = defaultVisitPropositionId;
        this.visitDisplayName = visitPropositionProperties.getProperty("displayName");
        
        this.inoutPropertyName = visitPropositionProperties.getProperty("inout.propertyName");
        this.inoutPropertyValueSet = defaultInoutPropertyValueSet;
        if (this.inoutPropertyValueSet != null) {
            this.valueSets.put(this.inoutPropertyValueSet.getId(), this.inoutPropertyValueSet);
        }
        this.inoutPropertySource = visitPropositionProperties.getProperty("inout.valueSet.source");
        
        this.visitAgePropertyName = visitPropositionProperties.getProperty("age.propertyName");
        
        /**
         * Provider
         */
        this.providerPropositionId = providerPropositionProperties.getProperty("propositionId");
        this.providerDisplayName = visitPropositionProperties.getProperty("displayName");
        this.providerNamePropertyName = providerPropositionProperties.getProperty("name.propertyName");
        
        this.genderPropertyValueSetDelimiter = DEFAULT_DELIMITER;
        this.racePropertyValueSetDelimiter = DEFAULT_DELIMITER;
        this.languagePropertyValueSetDelimiter = DEFAULT_DELIMITER;
        this.maritalStatusPropertyValueSetDelimiter = DEFAULT_DELIMITER;
        this.religionPropertyValueSetDelimiter = DEFAULT_DELIMITER;
    }

    /**
     * Returns which Java database API this backend is configured to use.
     *
     * @return a {@link DatabaseAPI}. The default value is
     * {@link org.arp.javautil.sql.DatabaseAPI}<code>.DRIVERMANAGER</code>
     */
    public DatabaseAPI getDatabaseApi() {
        return this.querySupport.getDatabaseApi();
    }

    /**
     * Configures which Java database API to use ({@link java.sql.DriverManager}
     * or {@link javax.sql.DataSource}. If <code>null</code>, the default is
     * assigned
     * ({@link org.arp.javautil.sql.DatabaseAPI}<code>.DRIVERMANAGER</code>).
     *
     * @param databaseApi a {@link DatabaseAPI}.
     */
    public void setDatabaseApi(DatabaseAPI databaseApi) {
        this.querySupport.setDatabaseApi(databaseApi);
    }

    /**
     * Configures which Java database API to use ({@link java.sql.DriverManager}
     * or {@link javax.sql.DataSource} by parsing a {@link DatabaseAPI}'s name.
     * Cannot be null.
     *
     * @param databaseApiString a {@link DatabaseAPI}'s name.
     */
    @BackendProperty(propertyName = "databaseAPI")
    public void parseDatabaseApi(String databaseApiString) {
        setDatabaseApi(DatabaseAPI.valueOf(databaseApiString));
    }

    public String getDatabaseId() {
        return this.querySupport.getDatabaseId();
    }

    @BackendProperty
    public void setDatabaseId(String databaseId) {
        this.querySupport.setDatabaseId(databaseId);
    }

    public String getUsername() {
        return this.querySupport.getUsername();
    }

    @BackendProperty
    public void setUsername(String username) {
        this.querySupport.setUsername(username);
    }

    public String getPassword() {
        return this.querySupport.getPassword();
    }

    @BackendProperty
    public void setPassword(String password) {
        this.querySupport.setPassword(password);
    }

    public String getPatientDetailsPropositionId() {
        return patientDetailsPropositionId;
    }

    @BackendProperty
    public void setPatientDetailsPropositionId(String patientDimensionConfigFile) {
        if (patientDimensionConfigFile != null) {
            this.patientDetailsPropositionId = patientDimensionConfigFile;
        } else {
            this.patientDetailsPropositionId = defaultPatientDetailsPropositionId;
        }
    }
    
    public String getPatientDetailsDisplayName() {
        return patientDetailsDisplayName;
    }

    @BackendProperty
    public void setPatientDetailsDisplayName(String patientDetailsDisplayName) {
        this.patientDetailsDisplayName = patientDetailsDisplayName;
    }
    
    public String getVisitPropositionId() {
        return visitPropositionId;
    }

    @BackendProperty
    public void setVisitPropositionId(String visitPropositionId) {
        if (visitPropositionId != null) {
            this.visitPropositionId = visitPropositionId;
        } else {
            this.visitPropositionId = defaultVisitPropositionId;
        }
    }

    public String getVisitDisplayName() {
        return visitDisplayName;
    }

    @BackendProperty
    public void setVisitDisplayName(String visitDisplayName) {
        this.visitDisplayName = visitDisplayName;
    }
    
    public String getProviderPropositionId() {
        return providerPropositionId;
    }

    @BackendProperty
    public void setProviderPropositionId(String providerPropositionId) {
        if (providerPropositionId != null) {
            this.providerPropositionId = providerPropositionId;
        } else {
            this.providerPropositionId = this.providerPropositionProperties.getProperty("propositionId");
        }
    }

    public String getProviderDisplayName() {
        return providerDisplayName;
    }

    @BackendProperty
    public void setProviderDisplayName(String providerDisplayName) {
        this.providerDisplayName = providerDisplayName;
    }
    
    public String getProviderNamePropertyName() {
        return providerNamePropertyName;
    }

    @BackendProperty
    public void setProviderNamePropertyName(String providerNamePropertyName) {
        this.providerNamePropertyName = providerNamePropertyName;
    }

    public String getInoutPropertyName() {
        return inoutPropertyName;
    }

    @BackendProperty
    public void setInoutPropertyName(String inoutPropertyName) {
        this.inoutPropertyName = inoutPropertyName;
    }

    public String getInoutPropertySource() {
        return inoutPropertySource;
    }

    @BackendProperty
    public void setInoutPropertySource(String inoutPropertySource) {
        this.inoutPropertySource = inoutPropertySource;
    }

    public ValueSet getInoutPropertyValueSet() {
        return inoutPropertyValueSet;
    }
    
    public void setInoutPropertyValueSet(ValueSet inoutPropertyValueSet) {
        this.inoutPropertyValueSet = inoutPropertyValueSet;
    }
    
    @BackendProperty(propertyName = "inoutPropertyValueSet")
    public void parseInoutPropertyValueSet(String valueSetStr) {
        this.inoutPropertyValueSet = parseValueSet(this.visitPropositionId, valueSetStr);
    }

    public String getInoutPropertyValueSetFile() {
        return inoutPropertyValueSetFile;
    }

    @BackendProperty
    public void setInoutPropertyValueSetFile(String inoutPropertyValueSetFile) {
        this.inoutPropertyValueSetFile = inoutPropertyValueSetFile;
    }

    public Boolean getInoutPropertyValueSetHasHeader() {
        return inoutPropertyValueSetHasHeader;
    }

    @BackendProperty
    public void setInoutPropertyValueSetHasHeader(Boolean inoutPropertyValueSetHasHeader) {
        if (inoutPropertyValueSetHasHeader != null) {
            this.inoutPropertyValueSetHasHeader = inoutPropertyValueSetHasHeader;
        } else {
            this.inoutPropertyValueSetHasHeader = false;
        }
    }

    public String getInoutPropertyValueSetDelimiter() {
        return Character.toString(inoutPropertyValueSetDelimiter);
    }

    @BackendProperty
    public void setInoutPropertyValueSetDelimiter(String inoutPropertyValueSetDelimiter) {
        if (inoutPropertyValueSetDelimiter != null) {
            if (inoutPropertyValueSetDelimiter.length() > 1) {
                throw new IllegalArgumentException("delimiter can only be one character");
            }
            if (inoutPropertyValueSetDelimiter.length() < 1) {
                throw new IllegalArgumentException("delimiter must be at least one character");
            }
        }
        if (inoutPropertyValueSetDelimiter != null) {
            this.inoutPropertyValueSetDelimiter = inoutPropertyValueSetDelimiter.charAt(0);
        } else {
            this.inoutPropertyValueSetDelimiter = DEFAULT_DELIMITER;
        }
    }

    public Integer getInoutPropertyValueSetIdColumn() {
        return inoutPropertyValueSetIdColumn;
    }

    @BackendProperty
    public void setInoutPropertyValueSetIdColumn(Integer inoutPropertyValueSetIdColumn) {
        if (inoutPropertyValueSetIdColumn == null) {
            throw new IllegalArgumentException("inoutPropertyValueSetIdColumn cannot be null");
        }
        this.inoutPropertyValueSetIdColumn = inoutPropertyValueSetIdColumn;
    }

    public Integer getInoutPropertyValueSetDisplayNameColumn() {
        return inoutPropertyValueSetDisplayNameColumn;
    }

    @BackendProperty
    public void setInoutPropertyValueSetDisplayNameColumn(Integer inoutPropertyValueSetDisplayNameColumn) {
        this.inoutPropertyValueSetDisplayNameColumn = inoutPropertyValueSetDisplayNameColumn;
    }

    public Integer getInoutPropertyValueSetDescriptionColumn() {
        return inoutPropertyValueSetDescriptionColumn;
    }

    @BackendProperty
    public void setInoutPropertyValueSetDescriptionColumn(Integer inoutPropertyValueSetDescriptionColumn) {
        this.inoutPropertyValueSetDescriptionColumn = inoutPropertyValueSetDescriptionColumn;
    }
    
    public String getVisitAgePropertyName() {
        return visitAgePropertyName;
    }

    @BackendProperty
    public void setVisitAgePropertyName(String visitAgePropertyName) {
        this.visitAgePropertyName = visitAgePropertyName;
    }

    public String getGenderPropertyName() {
        return genderPropertyName;
    }

    @BackendProperty
    public void setGenderPropertyName(String genderPropertyName) {
        this.genderPropertyName = genderPropertyName;
    }

    public ValueSet getGenderPropertyValueSet() {
        return genderPropertyValueSet;
    }

    public void setGenderPropertyValueSet(ValueSet genderPropertyValueSet) {
        this.genderPropertyValueSet = genderPropertyValueSet;
    }

    @BackendProperty(propertyName = "genderPropertyValueSet")
    public void parseGenderPropertyValueSet(String valueSetStr) {
        this.genderPropertyValueSet = parseValueSet(this.patientDetailsPropositionId, valueSetStr);
    }
    
    public String getGenderPropertySource() {
        return genderPropertySource;
    }

    @BackendProperty
    public void setGenderPropertySource(String genderPropertySource) {
        this.genderPropertySource = genderPropertySource;
    }

    public String getGenderPropertyValueSetFile() {
        return genderPropertyValueSetFile;
    }

    @BackendProperty
    public void setGenderPropertyValueSetFile(String genderPropertyValueSetFile) {
        this.genderPropertyValueSetFile = genderPropertyValueSetFile;
    }

    public Boolean getGenderPropertyValueSetHasHeader() {
        return genderPropertyValueSetHasHeader;
    }

    @BackendProperty
    public void setGenderPropertyValueSetHasHeader(Boolean genderPropertyValueSetHasHeader) {
        if (genderPropertyValueSetHasHeader != null) {
            this.genderPropertyValueSetHasHeader = genderPropertyValueSetHasHeader;
        } else {
            this.genderPropertyValueSetHasHeader = false;
        }
    }

    public String getGenderPropertyValueSetDelimiter() {
        return Character.toString(genderPropertyValueSetDelimiter);
    }

    @BackendProperty
    public void setGenderPropertyValueSetDelimiter(String genderPropertyValueSetDelimiter) {
        if (genderPropertyValueSetDelimiter != null) {
            if (genderPropertyValueSetDelimiter.length() > 1) {
                throw new IllegalArgumentException("delimiter can only be one character");
            }
            if (genderPropertyValueSetDelimiter.length() < 1) {
                throw new IllegalArgumentException("delimiter must be at least one character");
            }
        }
        if (genderPropertyValueSetDelimiter != null) {
            this.genderPropertyValueSetDelimiter = genderPropertyValueSetDelimiter.charAt(0);
        } else {
            this.genderPropertyValueSetDelimiter = DEFAULT_DELIMITER;
        }
    }

    public Integer getGenderPropertyValueSetIdColumn() {
        return genderPropertyValueSetIdColumn;
    }

    @BackendProperty
    public void setGenderPropertyValueSetIdColumn(Integer genderPropertyValueSetIdColumn) {
        if (genderPropertyValueSetIdColumn == null) {
            throw new IllegalArgumentException("genderPropertyValueSetIdColumn cannot be null");
        }
        this.genderPropertyValueSetIdColumn = genderPropertyValueSetIdColumn;
    }

    public Integer getGenderPropertyValueSetDisplayNameColumn() {
        return genderPropertyValueSetDisplayNameColumn;
    }

    @BackendProperty
    public void setGenderPropertyValueSetDisplayNameColumn(Integer genderPropertyValueSetDisplayNameColumn) {
        this.genderPropertyValueSetDisplayNameColumn = genderPropertyValueSetDisplayNameColumn;
    }

    public Integer getGenderPropertyValueSetDescriptionColumn() {
        return genderPropertyValueSetDescriptionColumn;
    }

    @BackendProperty
    public void setGenderPropertyValueSetDescriptionColumn(Integer genderPropertyValueSetDescriptionColumn) {
        this.genderPropertyValueSetDescriptionColumn = genderPropertyValueSetDescriptionColumn;
    }
    
    public String getRacePropertyName() {
        return racePropertyName;
    }

    @BackendProperty
    public void setRacePropertyName(String racePropertyName) {
        this.racePropertyName = racePropertyName;
    }

    public ValueSet getRacePropertyValueSet() {
        return racePropertyValueSet;
    }

    public void setRacePropertyValueSet(ValueSet racePropertyValueSet) {
        this.racePropertyValueSet = racePropertyValueSet;
    }

    @BackendProperty(propertyName = "racePropertyValueSet")
    public void parseRacePropertyValueSet(String valueSetStr) {
        this.racePropertyValueSet = parseValueSet(this.patientDetailsPropositionId, valueSetStr);
    }

    public String getRacePropertyValueSetFile() {
        return racePropertyValueSetFile;
    }

    @BackendProperty
    public void setRacePropertyValueSetFile(String racePropertyValueSetFile) {
        this.racePropertyValueSetFile = racePropertyValueSetFile;
    }

    public Boolean getRacePropertyValueSetHasHeader() {
        return racePropertyValueSetHasHeader;
    }

    @BackendProperty
    public void setRacePropertyValueSetHasHeader(Boolean racePropertyValueSetHasHeader) {
        if (racePropertyValueSetHasHeader != null) {
            this.racePropertyValueSetHasHeader = racePropertyValueSetHasHeader;
        } else {
            this.racePropertyValueSetHasHeader = false;
        }
    }

    public String getRacePropertyValueSetDelimiter() {
        return Character.toString(racePropertyValueSetDelimiter);
    }

    @BackendProperty
    public void setRacePropertyValueSetDelimiter(String racePropertyValueSetDelimiter) {
        if (racePropertyValueSetDelimiter != null) {
            if (racePropertyValueSetDelimiter.length() > 1) {
                throw new IllegalArgumentException("delimiter can only be one character");
            }
            if (racePropertyValueSetDelimiter.length() < 1) {
                throw new IllegalArgumentException("delimiter must be at least one character");
            }
        }
        if (racePropertyValueSetDelimiter != null) {
            this.racePropertyValueSetDelimiter = racePropertyValueSetDelimiter.charAt(0);
        } else {
            this.racePropertyValueSetDelimiter = DEFAULT_DELIMITER;
        }
    }

    public Integer getRacePropertyValueSetIdColumn() {
        return racePropertyValueSetIdColumn;
    }

    @BackendProperty
    public void setRacePropertyValueSetIdColumn(Integer racePropertyValueSetIdColumn) {
        if (racePropertyValueSetIdColumn == null) {
            throw new IllegalArgumentException("racePropertyValueSetIdColumn cannot be null");
        }
        this.racePropertyValueSetIdColumn = racePropertyValueSetIdColumn;
    }

    public Integer getRacePropertyValueSetDisplayNameColumn() {
        return racePropertyValueSetDisplayNameColumn;
    }

    @BackendProperty
    public void setRacePropertyValueSetDisplayNameColumn(Integer racePropertyValueSetDisplayNameColumn) {
        this.racePropertyValueSetDisplayNameColumn = racePropertyValueSetDisplayNameColumn;
    }

    public Integer getRacePropertyValueSetDescriptionColumn() {
        return racePropertyValueSetDescriptionColumn;
    }

    @BackendProperty
    public void setRacePropertyValueSetDescriptionColumn(Integer racePropertyValueSetDescriptionColumn) {
        this.racePropertyValueSetDescriptionColumn = racePropertyValueSetDescriptionColumn;
    }
    
    public String getRacePropertySource() {
        return racePropertySource;
    }

    @BackendProperty
    public void setRacePropertySource(String racePropertySource) {
        this.racePropertySource = racePropertySource;
    }

    public String getLanguagePropertyName() {
        return languagePropertyName;
    }

    @BackendProperty
    public void setLanguagePropertyName(String languagePropertyName) {
        this.languagePropertyName = languagePropertyName;
    }

    public ValueSet getLanguagePropertyValueSet() {
        return languagePropertyValueSet;
    }

    public void setLanguagePropertyValueSet(ValueSet languagePropertyValueSet) {
        this.languagePropertyValueSet = languagePropertyValueSet;
    }

    @BackendProperty(propertyName = "languagePropertyValueSet")
    public void parseLanguagePropertyValueSet(String valueSetStr) {
        this.languagePropertyValueSet = parseValueSet(this.patientDetailsPropositionId, valueSetStr);
    }

    public String getLanguagePropertyValueSetFile() {
        return languagePropertyValueSetFile;
    }

    @BackendProperty
    public void setLanguagePropertyValueSetFile(String languagePropertyValueSetFile) {
        this.languagePropertyValueSetFile = languagePropertyValueSetFile;
    }

    public Boolean getLanguagePropertyValueSetHasHeader() {
        return languagePropertyValueSetHasHeader;
    }

    @BackendProperty
    public void setLanguagePropertyValueSetHasHeader(Boolean languagePropertyValueSetHasHeader) {
        if (languagePropertyValueSetHasHeader != null) {
            this.languagePropertyValueSetHasHeader = languagePropertyValueSetHasHeader;
        } else {
            this.languagePropertyValueSetHasHeader = false;
        }
    }

    public String getLanguagePropertyValueSetDelimiter() {
        return Character.toString(languagePropertyValueSetDelimiter);
    }

    @BackendProperty
    public void setLanguagePropertyValueSetDelimiter(String languagePropertyValueSetDelimiter) {
        if (languagePropertyValueSetDelimiter != null) {
            if (languagePropertyValueSetDelimiter.length() > 1) {
                throw new IllegalArgumentException("delimiter can only be one character");
            }
            if (languagePropertyValueSetDelimiter.length() < 1) {
                throw new IllegalArgumentException("delimiter must be at least one character");
            }
        }
        if (languagePropertyValueSetDelimiter != null) {
            this.languagePropertyValueSetDelimiter = languagePropertyValueSetDelimiter.charAt(0);
        } else {
            this.languagePropertyValueSetDelimiter = DEFAULT_DELIMITER;
        }
    }

    public Integer getLanguagePropertyValueSetIdColumn() {
        return languagePropertyValueSetIdColumn;
    }

    @BackendProperty
    public void setLanguagePropertyValueSetIdColumn(Integer languagePropertyValueSetIdColumn) {
        if (languagePropertyValueSetIdColumn == null) {
            throw new IllegalArgumentException("languagePropertyValueSetIdColumn cannot be null");
        }
        this.languagePropertyValueSetIdColumn = languagePropertyValueSetIdColumn;
    }

    public Integer getLanguagePropertyValueSetDisplayNameColumn() {
        return languagePropertyValueSetDisplayNameColumn;
    }

    @BackendProperty
    public void setLanguagePropertyValueSetDisplayNameColumn(Integer languagePropertyValueSetDisplayNameColumn) {
        this.languagePropertyValueSetDisplayNameColumn = languagePropertyValueSetDisplayNameColumn;
    }

    public Integer getLanguagePropertyValueSetDescriptionColumn() {
        return languagePropertyValueSetDescriptionColumn;
    }

    @BackendProperty
    public void setLanguagePropertyValueSetDescriptionColumn(Integer languagePropertyValueSetDescriptionColumn) {
        this.languagePropertyValueSetDescriptionColumn = languagePropertyValueSetDescriptionColumn;
    }
    
    public String getLanguagePropertySource() {
        return languagePropertySource;
    }

    @BackendProperty
    public void setLanguagePropertySource(String languagePropertySource) {
        this.languagePropertySource = languagePropertySource;
    }

    public ValueSet getMaritalStatusPropertyValueSet() {
        return maritalStatusPropertyValueSet;
    }

    public void setMaritalStatusPropertyValueSet(ValueSet maritalStatusPropertyValueSet) {
        this.maritalStatusPropertyValueSet = maritalStatusPropertyValueSet;
    }
    
    @BackendProperty(propertyName = "maritalStatusPropertyValueSet")
    public void parseMaritalStatusPropertyValueSet(String valueSetStr) {
        this.maritalStatusPropertyValueSet = parseValueSet(this.patientDetailsPropositionId, valueSetStr);
    }

    public String getMaritalStatusPropertyValueSetFile() {
        return maritalStatusPropertyValueSetFile;
    }

    @BackendProperty
    public void setMaritalStatusPropertyValueSetFile(String maritalStatusPropertyValueSetFile) {
        this.maritalStatusPropertyValueSetFile = maritalStatusPropertyValueSetFile;
    }

    public Boolean getMaritalStatusPropertyValueSetHasHeader() {
        return maritalStatusPropertyValueSetHasHeader;
    }

    @BackendProperty
    public void setMaritalStatusPropertyValueSetHasHeader(Boolean maritalStatusPropertyValueSetHasHeader) {
        if (maritalStatusPropertyValueSetHasHeader != null) {
            this.maritalStatusPropertyValueSetHasHeader = maritalStatusPropertyValueSetHasHeader;
        } else {
            this.maritalStatusPropertyValueSetHasHeader = false;
        }
    }

    public String getMaritalStatusPropertyValueSetDelimiter() {
        return Character.toString(maritalStatusPropertyValueSetDelimiter);
    }

    @BackendProperty
    public void setMaritalStatusPropertyValueSetDelimiter(String maritalStatusPropertyValueSetDelimiter) {
        if (maritalStatusPropertyValueSetDelimiter != null) {
            if (maritalStatusPropertyValueSetDelimiter.length() > 1) {
                throw new IllegalArgumentException("delimiter can only be one character");
            }
            if (maritalStatusPropertyValueSetDelimiter.length() < 1) {
                throw new IllegalArgumentException("delimiter must be at least one character");
            }
        }
        if (maritalStatusPropertyValueSetDelimiter != null) {
            this.maritalStatusPropertyValueSetDelimiter = maritalStatusPropertyValueSetDelimiter.charAt(0);
        } else {
            this.maritalStatusPropertyValueSetDelimiter = DEFAULT_DELIMITER;
        }
    }

    public Integer getMaritalStatusPropertyValueSetIdColumn() {
        return maritalStatusPropertyValueSetIdColumn;
    }

    @BackendProperty
    public void setMaritalStatusPropertyValueSetIdColumn(Integer maritalStatusPropertyValueSetIdColumn) {
        if (maritalStatusPropertyValueSetIdColumn == null) {
            throw new IllegalArgumentException("maritalStatusPropertyValueSetIdColumn cannot be null");
        }
        this.maritalStatusPropertyValueSetIdColumn = maritalStatusPropertyValueSetIdColumn;
    }

    public Integer getMaritalStatusPropertyValueSetDisplayNameColumn() {
        return maritalStatusPropertyValueSetDisplayNameColumn;
    }

    @BackendProperty
    public void setMaritalStatusPropertyValueSetDisplayNameColumn(Integer maritalStatusPropertyValueSetDisplayNameColumn) {
        this.maritalStatusPropertyValueSetDisplayNameColumn = maritalStatusPropertyValueSetDisplayNameColumn;
    }

    public Integer getMaritalStatusPropertyValueSetDescriptionColumn() {
        return maritalStatusPropertyValueSetDescriptionColumn;
    }

    @BackendProperty
    public void setMaritalStatusPropertyValueSetDescriptionColumn(Integer maritalStatusPropertyValueSetDescriptionColumn) {
        this.maritalStatusPropertyValueSetDescriptionColumn = maritalStatusPropertyValueSetDescriptionColumn;
    }
    
    public String getMaritalStatusPropertySource() {
        return maritalStatusPropertySource;
    }

    @BackendProperty
    public void setMaritalStatusPropertySource(String maritalStatusPropertySource) {
        this.maritalStatusPropertySource = maritalStatusPropertySource;
    }

    public String getMaritalStatusPropertyName() {
        return maritalStatusPropertyName;
    }

    @BackendProperty
    public void setMaritalStatusPropertyName(String maritalStatusPropertyName) {
        this.maritalStatusPropertyName = maritalStatusPropertyName;
    }

    public String getReligionPropertyName() {
        return religionPropertyName;
    }

    @BackendProperty
    public void setReligionPropertyName(String religionPropertyName) {
        this.religionPropertyName = religionPropertyName;
    }

    public ValueSet getReligionPropertyValueSet() {
        return religionPropertyValueSet;
    }

    public void setReligionPropertyValueSet(ValueSet religionPropertyValueSet) {
        this.religionPropertyValueSet = religionPropertyValueSet;
    }
    
    @BackendProperty(propertyName = "religionPropertyValueSet")
    public void parseReligionPropertyValueSet(String valueSetStr) {
        this.religionPropertyValueSet = parseValueSet(this.patientDetailsPropositionId, valueSetStr);
    }

    public String getReligionPropertyValueSetFile() {
        return religionPropertyValueSetFile;
    }

    @BackendProperty
    public void setReligionPropertyValueSetFile(String religionPropertyValueSetFile) {
        this.religionPropertyValueSetFile = religionPropertyValueSetFile;
    }

    public Boolean getReligionPropertyValueSetHasHeader() {
        return religionPropertyValueSetHasHeader;
    }

    @BackendProperty
    public void setReligionPropertyValueSetHasHeader(Boolean religionPropertyValueSetHasHeader) {
        if (religionPropertyValueSetHasHeader != null) {
            this.religionPropertyValueSetHasHeader = religionPropertyValueSetHasHeader;
        } else {
            this.religionPropertyValueSetHasHeader = false;
        }
    }

    public String getReligionPropertyValueSetDelimiter() {
        return Character.toString(religionPropertyValueSetDelimiter);
    }

    @BackendProperty
    public void setReligionPropertyValueSetDelimiter(String religionPropertyValueSetDelimiter) {
        if (religionPropertyValueSetDelimiter != null) {
            if (religionPropertyValueSetDelimiter.length() > 1) {
                throw new IllegalArgumentException("delimiter can only be one character");
            }
            if (religionPropertyValueSetDelimiter.length() < 1) {
                throw new IllegalArgumentException("delimiter must be at least one character");
            }
        }
        if (religionPropertyValueSetDelimiter != null) {
            this.religionPropertyValueSetDelimiter = religionPropertyValueSetDelimiter.charAt(0);
        } else {
            this.religionPropertyValueSetDelimiter = DEFAULT_DELIMITER;
        }
    }

    public Integer getReligionPropertyValueSetIdColumn() {
        return religionPropertyValueSetIdColumn;
    }

    @BackendProperty
    public void setReligionPropertyValueSetIdColumn(Integer religionPropertyValueSetIdColumn) {
        if (religionPropertyValueSetIdColumn == null) {
            throw new IllegalArgumentException("religionPropertyValueSetIdColumn cannot be null");
        }
        this.religionPropertyValueSetIdColumn = religionPropertyValueSetIdColumn;
    }

    public Integer getReligionPropertyValueSetDisplayNameColumn() {
        return religionPropertyValueSetDisplayNameColumn;
    }

    @BackendProperty
    public void setReligionPropertyValueSetDisplayNameColumn(Integer religionPropertyValueSetDisplayNameColumn) {
        this.religionPropertyValueSetDisplayNameColumn = religionPropertyValueSetDisplayNameColumn;
    }

    public Integer getReligionPropertyValueSetDescriptionColumn() {
        return religionPropertyValueSetDescriptionColumn;
    }

    @BackendProperty
    public void setReligionPropertyValueSetDescriptionColumn(Integer religionPropertyValueSetDescriptionColumn) {
        this.religionPropertyValueSetDescriptionColumn = religionPropertyValueSetDescriptionColumn;
    }
    
    public String getReligionPropertySource() {
        return religionPropertySource;
    }
    
    @BackendProperty
    public void setReligionPropertySource(String religionPropertySource) {
        this.religionPropertySource = religionPropertySource;
    }

    public String getDateOfBirthPropertyName() {
        return dateOfBirthPropertyName;
    }

    @BackendProperty
    public void setDateOfBirthPropertyName(String dateOfBirthPropertyName) {
        this.dateOfBirthPropertyName = dateOfBirthPropertyName;
    }

    public String getAgeInYearsPropertyName() {
        return ageInYearsPropertyName;
    }

    @BackendProperty
    public void setAgeInYearsPropertyName(String ageInYearsPropertyName) {
        this.ageInYearsPropertyName = ageInYearsPropertyName;
    }

    public String getDateOfDeathPropertyName() {
        return dateOfDeathPropertyName;
    }

    @BackendProperty
    public void setDateOfDeathPropertyName(String dateOfDeathPropertyName) {
        this.dateOfDeathPropertyName = dateOfDeathPropertyName;
    }

    public String getVitalStatusPropertyName() {
        return vitalStatusPropertyName;
    }

    @BackendProperty
    public void setVitalStatusPropertyName(String vitalStatusPropertyName) {
        this.vitalStatusPropertyName = vitalStatusPropertyName;
    }

    public ConceptProperty[] getConceptProperties() {
        return conceptProperties.clone();
    }

    public void setConceptProperties(ConceptProperty[] conceptProperties) {
        if (conceptProperties == null) {
            this.conceptProperties = DEFAULT_CONCEPT_PROPERTIES_ARR;
        } else {
            this.conceptProperties = conceptProperties.clone();
        }
    }
    
    @BackendProperty(propertyName = "conceptProperties")
    public void parseConceptProperties(String conceptProperties) {
        if (conceptProperties != null) {
            List<ConceptProperty> cps = new ArrayList<>();
            try (Reader reader = new StringReader(conceptProperties);
                    CSVReader r = new CSVReader(reader, ',')) {
                String[] cols = r.readNext();
                for (String col : cols) {
                    try (Reader reader2 = new StringReader(col);
                            CSVReader r2 = new CSVReader(reader2, '|')) {
                        String[] cols2 = r2.readNext();
                        if (cols2.length == 2) {
                            cps.add(new ConceptProperty(cols2[0], cols2[1]));
                        } else {
                            throw new IllegalArgumentException("conceptProperties must be a comma-separated list of fullName|propositionId pairs");
                        }
                    }
                }
            } catch (IOException ioe) {
                throw new IllegalArgumentException("conceptProperties must be a comma-separated list of fullName|propositionId pairs");
            }
        }
    }
    
    @Override
    public PropositionDefinition readPropositionDefinition(String id) throws KnowledgeSourceReadException {
        if (logger.isLoggable(Level.FINEST)) {
            logger.log(Level.FINEST, "Looking for proposition {0}", id);
        }
        
        if (id != null) {
            PropositionDefinition result;
            if (id.equals(this.patientDetailsPropositionId)) {
                result = newPatientPropositionDefinition();
            } else if (id.equals(this.visitPropositionId)) {
                result = newVisitPropositionDefinition();
            } else if (id.equals(this.providerPropositionId)) {
                result = newProviderPropositionDefinition();
            } else {
                result = readFact(id);
            }

            if (result != null) {
                logger.log(Level.FINEST, "Found proposition id: {0}", id);

                return result;
            }
            logger.log(Level.FINER, "Failed to find proposition id: {0}", id);
        }
        return null;
    }

    @Override
    public String[] readIsA(String propId) throws KnowledgeSourceReadException {
        Set<String> parents = this.levelReader.readParentsFromDatabase(propId);
        return parents.toArray(new String[parents.size()]);
    }
    
    private static final QueryConstructor SEARCH_QUERY_CONSTRUCTOR = new QueryConstructor() {

        @Override
        public void appendStatement(StringBuilder sql, String table) {
            sql.append("SELECT C_SYMBOL FROM ");
            sql.append(table);
            sql.append(" WHERE UPPER(C_NAME) LIKE UPPER(?) AND C_BASECODE IS NOT NULL");

        }
    };
    
    private static final ResultSetReader<Set<String>> SEARCH_RESULT_SET_READER = new ResultSetReader<Set<String>>() {

        @Override
        public Set<String> read(ResultSet rs) throws KnowledgeSourceReadException {
            try {
                Set<String> result = new HashSet<>();
                while (rs.next()) {
                    result.add(rs.getString(1));
                }
                return result;
            } catch (SQLException ex) {
                throw new KnowledgeSourceReadException(ex);
            }
        }

    };

    @Override
    public Set<String> getKnowledgeSourceSearchResults(String searchKey) throws KnowledgeSourceReadException {
        try (QueryExecutor queryExecutor = this.querySupport.getQueryExecutorInstance(SEARCH_QUERY_CONSTRUCTOR)) {
            return queryExecutor.execute(
                "%" + I2B2Util.escapeLike(searchKey) + "%",
                SEARCH_RESULT_SET_READER
            );
        }
    }

    @Override
    public void initialize(BackendInstanceSpec config) throws BackendInitializationException {
        super.initialize(config);
        this.valueMetadataSupport = new ValueMetadataSupport();
    }

    private ConstantDefinition newProviderPropositionDefinition() {
        Date now = new Date();
        ConstantDefinition providerDim = new ConstantDefinition(this.providerPropositionId);
        providerDim.setDisplayName(this.providerPropositionId);
        providerDim.setAccessed(now);
        providerDim.setCreated(now);
        providerDim.setInDataSource(true);
        providerDim.setPropertyDefinitions(
                new PropertyDefinition(this.providerPropositionId, this.providerNamePropertyName, ValueType.NOMINALVALUE, null, this.providerPropositionId));
        return providerDim;
    }
    
    private static class Root {

        private final String fullName;
        private final String symbol;

        Root(String fullName, String symbol) {
            this.fullName = fullName;
            this.symbol = symbol;
        }

        public String getFullName() {
            return fullName;
        }

        public String getSymbol() {
            return symbol;
        }

    }
    
    private static final QueryConstructor ROOT_QUERY_CONSTRUCTOR = new QueryConstructor() {

        @Override
        public void appendStatement(StringBuilder sql, String table) {
            sql.append("SELECT C_FULLNAME, C_SYMBOL FROM ");
            sql.append(table);
            sql.append(" WHERE C_FULLNAME = (SELECT C_FULLNAME FROM TABLE_ACCESS WHERE C_TABLE_NAME = '");
            sql.append(table);
            sql.append("')");
        }
    };

    private static final ResultSetReader<List<Root>> ROOT_RESULT_SET_READER = new ResultSetReader<List<Root>>() {

        @Override
        public List<Root> read(ResultSet rs) throws KnowledgeSourceReadException {
            List<Root> result = new ArrayList<>();
            try {
                while (rs.next()) {
                    result.add(new Root(rs.getString(1), rs.getString(2)));
                }
                return result;
            } catch (SQLException ex) {
                throw new KnowledgeSourceReadException(ex);
            }
        }
        
    };

    private EventDefinition newVisitPropositionDefinition() throws KnowledgeSourceReadException {
        Date now = new Date();
        EventDefinition visitDim = new EventDefinition(this.visitPropositionId);
        visitDim.setDisplayName(this.visitPropositionId);
        visitDim.setAccessed(now);
        visitDim.setCreated(now);
        visitDim.setInDataSource(true);
        List<PropertyDefinition> visitDimPropertyDefs = new ArrayList<>();
        visitDimPropertyDefs.add(new PropertyDefinition(this.visitPropositionId, this.visitAgePropertyName, ValueType.NUMERICALVALUE, null, this.visitPropositionId));
        visitDimPropertyDefs.add(new PropertyDefinition(this.visitPropositionId, this.inoutPropertyName, ValueType.NOMINALVALUE, this.inoutPropertyValueSet.getId(), this.visitPropositionId));
        for (ConceptProperty cp : this.conceptProperties) {
            if (visitDim.getId().equals(cp.getPropositionId())) {
                PropertyDefinition pd = new PropertyDefinition(this.visitPropositionId, cp.getPropertyBaseCode(), ValueType.NOMINALVALUE, cp.getPropertyBaseCode(), this.visitPropositionId);
                visitDimPropertyDefs.add(pd);
            }
        }
        visitDim.setPropertyDefinitions(visitDimPropertyDefs.toArray(new PropertyDefinition[visitDimPropertyDefs.size()]));
        List<ReferenceDefinition> refDefs = new ArrayList<>();
        refDefs.add(new ReferenceDefinition("provider", this.providerPropositionId));
        refDefs.add(new ReferenceDefinition("patientDetails", this.patientDetailsPropositionId));
        try (QueryExecutor queryExecutor = this.querySupport.getQueryExecutorInstance(ROOT_QUERY_CONSTRUCTOR)) {
            for (Root root : queryExecutor.execute(ROOT_RESULT_SET_READER)) {
                refDefs.add(new ReferenceDefinition(root.getFullName(), root.getSymbol()));
            }
        }
        visitDim.setReferenceDefinitions(refDefs.toArray(new ReferenceDefinition[refDefs.size()]));
        return visitDim;
    }

    private ConstantDefinition newPatientPropositionDefinition() {
        Date now = new Date();
        ConstantDefinition patientDim = new ConstantDefinition(this.patientDetailsPropositionId);
        patientDim.setAccessed(now);
        patientDim.setCreated(now);
        patientDim.setDisplayName(this.patientDetailsPropositionId);
        patientDim.setInDataSource(true);
        patientDim.setPropertyDefinitions(
                new PropertyDefinition(this.patientDetailsPropositionId, this.ageInYearsPropertyName, ValueType.NUMERICALVALUE, null, this.patientDetailsPropositionId),
                new PropertyDefinition(this.patientDetailsPropositionId, this.dateOfBirthPropertyName, ValueType.DATEVALUE, null, this.patientDetailsPropositionId),
                new PropertyDefinition(this.patientDetailsPropositionId, this.dateOfDeathPropertyName, ValueType.DATEVALUE, null, this.patientDetailsPropositionId),
                new PropertyDefinition(this.patientDetailsPropositionId, this.genderPropertyName, ValueType.NOMINALVALUE, this.genderPropertyValueSet.getId(), this.patientDetailsPropositionId),
                new PropertyDefinition(this.patientDetailsPropositionId, this.languagePropertyName, ValueType.NOMINALVALUE, this.languagePropertyValueSet.getId(), this.patientDetailsPropositionId),
                new PropertyDefinition(this.patientDetailsPropositionId, this.maritalStatusPropertyName, ValueType.NOMINALVALUE, this.maritalStatusPropertyValueSet.getId(), this.patientDetailsPropositionId),
                new PropertyDefinition(this.patientDetailsPropositionId, this.racePropertyName, ValueType.NOMINALVALUE, this.racePropertyValueSet.getId(), this.patientDetailsPropositionId),
                new PropertyDefinition(this.patientDetailsPropositionId, this.vitalStatusPropertyName, ValueType.BOOLEANVALUE, null, this.patientDetailsPropositionId));
        patientDim.setReferenceDefinitions(
                new ReferenceDefinition("encounters", this.visitPropositionId)
        );
        return patientDim;
    }

    @Override
    public AbstractionDefinition readAbstractionDefinition(String id) throws KnowledgeSourceReadException {
        return null;
    }

    @Override
    public ContextDefinition readContextDefinition(String id) throws KnowledgeSourceReadException {
        return null;
    }

    @Override
    public TemporalPropositionDefinition readTemporalPropositionDefinition(String id) throws KnowledgeSourceReadException {
        if (logger.isLoggable(Level.FINEST)) {
            logger.log(Level.FINEST, "Looking for proposition {0}", id);
        }
        
        if (id != null) {
            TemporalPropositionDefinition result;
            if (id.equals(this.visitPropositionId)) {
                result = newVisitPropositionDefinition();
            } else {
                result = readFact(id);
            }

            if (result != null) {
                logger.log(Level.FINEST, "Found proposition id: {0}", id);
                return result;
            }
            logger.log(Level.FINER, "Failed to find proposition id: {0}", id);
        }
        return null;
    }

    @Override
    public String[] readAbstractedInto(String propId) throws KnowledgeSourceReadException {
        return ArrayUtils.EMPTY_STRING_ARRAY;
    }

    @Override
    public String[] readInduces(String propId) throws KnowledgeSourceReadException {
        return ArrayUtils.EMPTY_STRING_ARRAY;
    }

    @Override
    public String[] readSubContextOfs(String propId) throws KnowledgeSourceReadException {
        return ArrayUtils.EMPTY_STRING_ARRAY;
    }
    
    private static final QueryConstructor READ_ONE_PROP_DEF_QUERY_CONSTRUCTOR = new QueryConstructor() {

        @Override
        public void appendStatement(StringBuilder sql, String table) {
            sql.append("SELECT C_METADATAXML FROM ");
            sql.append(table);
            sql.append(" WHERE C_SYMBOL=? AND C_SYNONYM_CD='N' AND M_APPLIED_PATH<>'@'");
        }
    };

    @Override
    public ValueSet readValueSet(final String id) throws KnowledgeSourceReadException {
        ValueSet result = this.valueSets.get(id);
        if (result != null) {
            return result;
        }
        for (ConceptProperty cp : this.conceptProperties) {
            if (cp.getPropositionId().equals(id)) {
                return this.conceptPropertyReader.readFromDatabase(cp.getPropertyBaseCode());
            }
        }
        try (QueryExecutor queryExecutor = this.querySupport.getQueryExecutorInstance(READ_ONE_PROP_DEF_QUERY_CONSTRUCTOR)) {
            return queryExecutor.execute(
                    id,
                    new ResultSetReader<ValueSet>() {

                        @Override
                        public ValueSet read(ResultSet rs) throws KnowledgeSourceReadException {
                            try {
                                if (rs.next()) {
                                    CMetadataXmlParser valueMetadataParser = new CMetadataXmlParser();
                                    XMLReader xmlReader = valueMetadataSupport.init(valueMetadataParser);
                                    valueMetadataParser.setConceptBaseCode(id);
                                    valueMetadataSupport.parseAndFreeClob(xmlReader, rs.getClob(1));
                                    SAXParseException exception = valueMetadataParser.getException();
                                    if (exception != null) {
                                        throw exception;
                                    }
                                    return valueMetadataParser.getValueSet();
                                } else {
                                    return null;
                                }
                            } catch (SQLException | SAXParseException ex) {
                                throw new KnowledgeSourceReadException(ex);
                            }
                        }

                    }
            );
        }
    }
    
    private static final ResultSetReader<Collection<String>> IN_DS_RESULT_SET_READER = new ResultSetReader<Collection<String>>() {

        @Override
        public Collection<String> read(ResultSet rs) throws KnowledgeSourceReadException {
            List<String> result = new ArrayList<>();
            try {
                while (rs.next()) {
                    result.add(rs.getString(1));
                }
                return result;
            } catch (SQLException ex) {
                throw new KnowledgeSourceReadException(ex);
            }
        }
    };
    
    private final static QueryConstructor IDS_PROPID_QC = new QueryConstructor() {

        @Override
        public void appendStatement(StringBuilder sql, String table) {
            sql.append("SELECT DISTINCT C_SYMBOL FROM ");
            sql.append(table);
            sql.append(" WHERE C_FULLNAME LIKE (SELECT C_FULLNAME FROM ");
            sql.append(table);
            sql.append(" WHERE C_SYMBOL=? AND C_SYNONYM_CD='N' AND C_BASECODE IS NOT NULL) || '%'");
        }

    };
    
    private final static QueryConstructor N_PROPID_QC = new QueryConstructor() {

        @Override
        public void appendStatement(StringBuilder sql, String table) {
            sql.append("SELECT DISTINCT C_SYMBOL FROM ");
            sql.append(table);
            sql.append(" WHERE C_FULLNAME LIKE (SELECT C_FULLNAME FROM ");
            sql.append(table);
            sql.append(" WHERE C_SYMBOL=? AND C_SYNONYM_CD='N') || '%'");
        }

    };
    
    @Override
    public Collection<String> collectPropIdDescendantsUsingAllNarrower(boolean inDataSourceOnly, final String[] propIds) throws KnowledgeSourceReadException {
        if (propIds != null) {
            ProtempaUtil.checkArrayForNullElement(propIds, racePropertyName);
        }
        final List<String> result = new ArrayList<>();
        if (propIds != null && propIds.length > 0) {
        final List<String> partial = new ArrayList<>(propIds.length);
        for (String propId : propIds) {
            if (patientDetailsPropositionId.equals(propId)) {
                result.add(propId);
            } else if (visitPropositionId.equals(propId)) {
                result.add(propId);
            } else if (providerPropositionId.equals(propId)) {
                result.add(propId);
            } else {
                partial.add(propId);
            }
        }
        try (QueryExecutor queryExecutor = this.querySupport.getQueryExecutorInstance(inDataSourceOnly ? IDS_PROPID_QC : N_PROPID_QC)) {
            queryExecutor.prepare();
            for (String partialPropId : partial) {
                result.addAll(queryExecutor.execute(
                        partialPropId, 
                        IN_DS_RESULT_SET_READER));
                }
            }
        }
        return result;
    }
    
    private final static QueryConstructor COLLECT_SUBTREE_PROPID_QC = new QueryConstructor() {

        @Override
        public void appendStatement(StringBuilder sql, String table) {
            sql.append("SELECT DISTINCT C_SYMBOL FROM ");
            sql.append(table);
            sql.append(" WHERE C_FULLNAME LIKE (SELECT C_FULLNAME FROM ");
            sql.append(table);
            sql.append(" WHERE C_SYMBOL=? AND C_SYNONYM_CD='N') || '%'");
        }

    };
    
    @Override
    public Collection<String> collectPropIdDescendantsUsingInverseIsA(final String[] propIds) throws KnowledgeSourceReadException {
        if (propIds != null) {
            ProtempaUtil.checkArrayForNullElement(propIds, racePropertyName);
        }
        final List<String> result = new ArrayList<>();
        if (propIds != null && propIds.length > 0) {
            final List<String> partial = new ArrayList<>(propIds.length);
            for (String propId : propIds) {
                if (patientDetailsPropositionId.equals(propId)) {
                    result.add(propId);
                } else if (visitPropositionId.equals(propId)) {
                    result.add(propId);
                } else if (providerPropositionId.equals(propId)) {
                    result.add(propId);
                } else {
                    partial.add(propId);
                }
            }
            try (QueryExecutor queryExecutor = this.querySupport.getQueryExecutorInstance(COLLECT_SUBTREE_PROPID_QC)) {
                queryExecutor.prepare();
                for (String partialPropId : partial) {
                    result.addAll(queryExecutor.execute(
                        partialPropId, 
                        IN_DS_RESULT_SET_READER));
                }
            }
        }
        return result;
    }
    
    private final static QueryConstructor IDS_PROPDEF_QC = new QueryConstructor() {

        @Override
        public void appendStatement(StringBuilder sql, String table) {
            sql.append("SELECT C_NAME, C_FULLNAME, VALUETYPE_CD, C_COMMENT, C_METADATAXML, CASE WHEN C_BASECODE IS NOT NULL THEN 1 ELSE 0 END, C_SYMBOL FROM ");
            sql.append(table);
            sql.append(" WHERE C_FULLNAME LIKE (SELECT C_FULLNAME FROM ");
            sql.append(table);
            sql.append(" WHERE C_SYMBOL=? AND C_SYNONYM_CD='N' AND C_BASECODE IS NOT NULL) || '%'");
        }

    };
    
    private final static QueryConstructor N_PROPDEF_QC = new QueryConstructor() {

        @Override
        public void appendStatement(StringBuilder sql, String table) {
            sql.append("SELECT C_NAME, C_FULLNAME, VALUETYPE_CD, C_COMMENT, C_METADATAXML, CASE WHEN C_BASECODE IS NOT NULL THEN 1 ELSE 0 END, C_SYMBOL FROM ");
            sql.append(table);
            sql.append(" WHERE C_FULLNAME LIKE (SELECT C_FULLNAME FROM ");
            sql.append(table);
            sql.append(" WHERE C_SYMBOL=? AND C_SYNONYM_CD='N') || '%'");
        }

    };

    @Override
    public Collection<PropositionDefinition> collectPropDefDescendantsUsingAllNarrower(boolean inDataSourceOnly, final String[] propIds) throws KnowledgeSourceReadException {
        if (propIds != null) {
            ProtempaUtil.checkArrayForNullElement(propIds, "propIds");
        }
        final List<PropositionDefinition> result = new ArrayList<>();
        final List<String> partial = new ArrayList<>();
        if (propIds != null && propIds.length > 0) {
            for (String propId : propIds) {
                if (patientDetailsPropositionId.equals(propId)) {
                    result.add(newPatientPropositionDefinition());
                } else if (visitPropositionId.equals(propId)) {
                    result.add(newVisitPropositionDefinition());
                } else if (providerPropositionId.equals(propId)) {
                    result.add(newProviderPropositionDefinition());
                } else {
                    partial.add(propId);
                }
            }
            try (QueryExecutor queryExecutor = this.querySupport.getQueryExecutorInstance(inDataSourceOnly ? IDS_PROPDEF_QC : N_PROPDEF_QC)) {
                queryExecutor.prepare();
                for (String partialPropId : partial) {
                    result.addAll(queryExecutor.execute(
                    partialPropId, 
                    listResultSetReader));
                }
            }
            
            final Map<String, PropositionDefinition> resultMap = new HashMap<>();
            for (PropositionDefinition propDef : result) {
                resultMap.put(propDef.getId(), propDef);
            }
            Map<String, List<String>> children = this.levelReader.readChildrenFromDatabase(resultMap.keySet());
            for (Map.Entry<String, List<String>> me : children.entrySet()) {
                PropositionDefinition pd = resultMap.get(me.getKey());
                List<String> value = me.getValue();
                ((AbstractPropositionDefinition) pd).setInverseIsA(value.toArray(new String[value.size()]));
            }
            
            List<PropertyDefinitionPartial> properties;
            try (QueryExecutor queryExecutor = this.querySupport.getQueryExecutorInstance(READ_ALL_PROPERTIES_CONSTRUCTOR)) {
                properties = queryExecutor.execute(ALL_PROPS_RSR);
            }
            
            try (QueryExecutor queryExecutor = this.querySupport.getQueryExecutorInstance(READ_APPLICABLE_CONCEPTS_CONSTRUCTOR)) {
                queryExecutor.prepare();
                for (final PropertyDefinitionPartial property : properties) {
                    queryExecutor.execute(property.getFullName(), new ResultSetReader<Boolean>() {

                        @Override
                        public Boolean read(ResultSet rs) throws KnowledgeSourceReadException {
                            Map<String, List<PropertyDefinition>> propDefMap = new HashMap<>();
                            try {
                                while (rs.next()) {
                                    Collections.putList(propDefMap, rs.getString(1), property.getPropertyDefinition(rs.getString(1)));
                                }
                            } catch (SQLException ex) {
                                throw new KnowledgeSourceReadException(ex);
                            }
                            for (Map.Entry<String, List<PropertyDefinition>> me : propDefMap.entrySet()) {
                                PropositionDefinition pd = resultMap.get(me.getKey());
                                if (pd != null) {
                                    List<PropertyDefinition> value = me.getValue();
                                    if (value != null) {
                                        ((AbstractPropositionDefinition) pd).setPropertyDefinitions(value.toArray(new PropertyDefinition[value.size()]));
                                    }
                                }
                            }
                            return Boolean.TRUE;
                        }
                    });
                }
            }
        }
        return result;
    }
    
    private final static QueryConstructor COLLECT_SUBTREE_PROPDEF_QC = new QueryConstructor() {

        @Override
        public void appendStatement(StringBuilder sql, String table) {
            sql.append("SELECT C_NAME, C_FULLNAME, VALUETYPE_CD, C_COMMENT, C_METADATAXML, CASE WHEN C_BASECODE IS NOT NULL THEN 1 ELSE 0 END, C_SYMBOL FROM ");
            sql.append(table);
            sql.append(" WHERE C_FULLNAME LIKE (SELECT C_FULLNAME FROM ");
            sql.append(table);
            sql.append(" WHERE C_SYMBOL=? AND C_SYNONYM_CD='N') || '%'");
        }

    };
    
    @Override
    public Collection<PropositionDefinition> collectPropDefDescendantsUsingInverseIsA(final String[] propIds) throws KnowledgeSourceReadException {
        if (propIds != null) {
            ProtempaUtil.checkArrayForNullElement(propIds, "propIds");
        }
        final List<PropositionDefinition> result = new ArrayList<>();
        final List<String> partial = new ArrayList<>();
        if (propIds != null && propIds.length > 0) {
            for (String propId : propIds) {
                if (patientDetailsPropositionId.equals(propId)) {
                    result.add(newPatientPropositionDefinition());
                } else if (visitPropositionId.equals(propId)) {
                    result.add(newVisitPropositionDefinition());
                } else if (providerPropositionId.equals(propId)) {
                    result.add(newProviderPropositionDefinition());
                } else {
                    partial.add(propId);
                }
            }
            
            try (QueryExecutor queryExecutor = this.querySupport.getQueryExecutorInstance(COLLECT_SUBTREE_PROPDEF_QC)) {
                queryExecutor.prepare();
                for (String partialPropId : partial) {
                    result.addAll(queryExecutor.execute(
                    partialPropId, 
                    listResultSetReader));
                }
            }
            final Map<String, PropositionDefinition> resultMap = new HashMap<>();
            for (PropositionDefinition propDef : result) {
                resultMap.put(propDef.getId(), propDef);
            }
            Map<String, List<String>> children = this.levelReader.readChildrenFromDatabase(resultMap.keySet());
            for (Map.Entry<String, List<String>> me : children.entrySet()) {
                PropositionDefinition pd = resultMap.get(me.getKey());
                List<String> value = me.getValue();
                ((AbstractPropositionDefinition) pd).setInverseIsA(value.toArray(new String[value.size()]));
            }
            
            List<PropertyDefinitionPartial> properties;
            try (QueryExecutor queryExecutor = this.querySupport.getQueryExecutorInstance(READ_ALL_PROPERTIES_CONSTRUCTOR)) {
                properties = queryExecutor.execute(ALL_PROPS_RSR);
            }
            
            try (QueryExecutor queryExecutor = this.querySupport.getQueryExecutorInstance(READ_APPLICABLE_CONCEPTS_CONSTRUCTOR)) {
                queryExecutor.prepare();
                for (final PropertyDefinitionPartial property : properties) {
                    queryExecutor.execute(property.getFullName(), new ResultSetReader<Boolean>() {

                        @Override
                        public Boolean read(ResultSet rs) throws KnowledgeSourceReadException {
                            Map<String, List<PropertyDefinition>> propDefMap = new HashMap<>();
                            try {
                                while (rs.next()) {
                                    Collections.putList(propDefMap, rs.getString(1), property.getPropertyDefinition(rs.getString(1)));
                                }
                            } catch (SQLException ex) {
                                throw new KnowledgeSourceReadException(ex);
                            }
                            for (Map.Entry<String, List<PropertyDefinition>> me : propDefMap.entrySet()) {
                                PropositionDefinition pd = resultMap.get(me.getKey());
                                if (pd != null) {
                                    List<PropertyDefinition> value = me.getValue();
                                    if (value != null) {
                                        ((AbstractPropositionDefinition) pd).setPropertyDefinitions(value.toArray(new PropertyDefinition[value.size()]));
                                    }
                                }
                            }
                            return Boolean.TRUE;
                        }
                    });
                }
            }
            result.addAll(resultMap.values());
        }
        return result;
    }
    
    private static final QueryConstructor READ_ALL_PROPERTIES_CONSTRUCTOR = new QueryConstructor() {

        @Override
        public void appendStatement(StringBuilder sql, String table) {
            sql.append("SELECT A1.C_NAME, A1.VALUETYPE_CD, A1.C_METADATAXML, (SELECT C_SYMBOL FROM ");
            sql.append(table);
            sql.append(" A2 WHERE A2.C_FULLNAME = CASE WHEN SUBSTR(A1.M_APPLIED_PATH, LENGTH(A1.M_APPLIED_PATH), 1) = '%' THEN SUBSTR(A1.M_APPLIED_PATH, 1, LENGTH(A1.M_APPLIED_PATH) - 1) ELSE A1.M_APPLIED_PATH END AND A2.C_SYNONYM_CD='N' and A2.M_APPLIED_PATH='@'), A1.C_FULLNAME FROM ");
            sql.append(table);
            sql.append(" A1 WHERE A1.C_SYNONYM_CD ='N' AND A1.M_APPLIED_PATH<>'@'");
        }

    };
    
    private static final QueryConstructor READ_APPLICABLE_CONCEPTS_CONSTRUCTOR = new QueryConstructor() {

        @Override
        public void appendStatement(StringBuilder sql, String table) {
            sql.append("SELECT C_SYMBOL FROM ");
            sql.append(table);
            sql.append(" WHERE C_SYNONYM_CD='N' and M_APPLIED_PATH='@' and C_FULLNAME LIKE (SELECT M_APPLIED_PATH FROM ");
            sql.append(table);
            sql.append(" WHERE C_FULLNAME=?)");
        }

    };
    
    private final ResultSetReader<List<PropertyDefinitionPartial>> ALL_PROPS_RSR = new ResultSetReader<List<PropertyDefinitionPartial>>() {

        @Override
        public List<PropertyDefinitionPartial> read(ResultSet rs) throws KnowledgeSourceReadException {
            List<PropertyDefinitionPartial> props = new ArrayList<>();
            try {
                CMetadataXmlParser valueMetadataParser = new CMetadataXmlParser();
                XMLReader xmlReader = valueMetadataSupport.init(valueMetadataParser);
                while (rs.next()) {
                    valueMetadataSupport.parseAndFreeClob(xmlReader, rs.getClob(3));
                    ValueType valueType = valueMetadataParser.getValueType();
                    ValueSet valueSet = valueMetadataParser.getValueSet();
                    props.add(new PropertyDefinitionPartial(rs.getString(5), rs.getString(1), valueType, valueSet != null ? valueSet.getId() : null, rs.getString(4)));
                }
            } catch (SQLException ex) {
                throw new KnowledgeSourceReadException(ex);
            }
            return props;
        }
        
    };
    
    private static final QueryConstructor READ_FACT_QUERY_CONSTRUCTOR = new QueryConstructor() {

        @Override
        public void appendStatement(StringBuilder sql, String table) {
            sql.append("SELECT C_NAME, C_FULLNAME, VALUETYPE_CD, C_COMMENT, C_METADATAXML, CASE WHEN C_BASECODE IS NOT NULL THEN 1 ELSE 0 END, C_SYMBOL FROM ");
            sql.append(table);
            sql.append(" WHERE C_SYNONYM_CD='N' AND C_SYMBOL=?");
        }

    };
    
    private void newTemporalPropositionDefinition(ResultSet rs, List<TemporalPropositionDefinition> r) throws SAXParseException, KnowledgeSourceReadException, SQLException {
        if (Arrays.contains(VALUE_TYPE_CDS, rs.getString(3))) {
            CMetadataXmlParser valueMetadataParser = new CMetadataXmlParser();
            XMLReader xmlReader = valueMetadataSupport.init(valueMetadataParser);
            valueMetadataSupport.parseAndFreeClob(xmlReader, rs.getClob(4));
            SAXParseException exception = valueMetadataParser.getException();
            if (exception != null) {
                throw exception;
            }
            ValueType valueType = valueMetadataParser.getValueType();
            PrimitiveParameterDefinition result = new PrimitiveParameterDefinition(rs.getString(7));
            result.setDisplayName(rs.getString(1));
            result.setDescription(rs.getString(4));
            result.setInDataSource(rs.getBoolean(6));
            result.setValueType(valueType);
            r.add(result);
        } else {
            EventDefinition result = new EventDefinition(rs.getString(7));
            result.setDisplayName(rs.getString(1));
            result.setDescription(rs.getString(4));
            result.setInDataSource(rs.getBoolean(6));
            r.add(result);
        }
    }
    
    private final ResultSetReader<List<TemporalPropositionDefinition>> listResultSetReader = new ResultSetReader<List<TemporalPropositionDefinition>>() {

        @Override
        public List<TemporalPropositionDefinition> read(ResultSet rs) throws KnowledgeSourceReadException {
            try {
                List<TemporalPropositionDefinition> r = new ArrayList<>();
                while (rs.next()) {
                    newTemporalPropositionDefinition(rs, r);
                }
                return r;
            } catch (SQLException | SAXParseException ex) {
                throw new KnowledgeSourceReadException(ex);
            }
        }

    };
    
    private final ResultSetReader<TemporalPropositionDefinition> resultSetReader = new ResultSetReader<TemporalPropositionDefinition>() {

        @Override
        public TemporalPropositionDefinition read(ResultSet rs) throws KnowledgeSourceReadException {
            try {
                List<TemporalPropositionDefinition> r = new ArrayList<>();
                if (rs.next()) {
                    newTemporalPropositionDefinition(rs, r);
                    AbstractPropositionDefinition abd = (AbstractPropositionDefinition) r.get(0);
                    Set<String> children = levelReader.readChildrenFromDatabase(rs.getString(2));
                    abd.setInverseIsA(children.toArray(new String[children.size()]));
                    List<PropertyDefinition> propDefs = readPropertyDefinitions(rs.getString(7), rs.getString(2));
                    abd.setPropertyDefinitions(propDefs.toArray(new PropertyDefinition[propDefs.size()]));
                }
                return r.isEmpty() ? null : r.get(0);
            } catch (SQLException | SAXParseException ex) {
                throw new KnowledgeSourceReadException(ex);
            }
        }

    };
    
    private TemporalPropositionDefinition readFact(final String id) throws KnowledgeSourceReadException {
        try (QueryExecutor queryExecutor = this.querySupport.getQueryExecutorInstance(READ_FACT_QUERY_CONSTRUCTOR)) {
            return queryExecutor.execute(
                id, 
                resultSetReader);
        }
    }
    
    private static final QueryConstructor READ_PROP_DEF_QUERY_CONSTRUCTOR = new QueryConstructor() {

        @Override
        public void appendStatement(StringBuilder sql, String table) {
            sql.append("SELECT A1.C_SYMBOL, A1.C_NAME, A1.C_METADATAXML, (SELECT A2.C_SYMBOL FROM ");
            sql.append(table);
            sql.append(" A2 WHERE (CASE WHEN SUBSTR(A1.M_APPLIED_PATH, LENGTH(A1.M_APPLIED_PATH), 1) = '%' THEN SUBSTR(A1.M_APPLIED_PATH, 1, LENGTH(A1.M_APPLIED_PATH) - 1) ELSE A1.M_APPLIED_PATH END)=A2.C_FULLNAME) FROM ");
            sql.append(table);
            sql.append(" A1 WHERE ? LIKE A1.M_APPLIED_PATH");
        }
    };
    
    private List<PropertyDefinition> readPropertyDefinitions(final String symbol, String fullName) throws KnowledgeSourceReadException {
        try (QueryExecutor queryExecutor = this.querySupport.getQueryExecutorInstance(READ_PROP_DEF_QUERY_CONSTRUCTOR)) {
            return queryExecutor.execute(
                fullName,
                new ResultSetReader<List<PropertyDefinition>>() {

                    @Override
                    public List<PropertyDefinition> read(ResultSet rs) throws KnowledgeSourceReadException {
                        try {
                            List<PropertyDefinition> result = new ArrayList<>();
                            if (rs.isBeforeFirst()) {
                                CMetadataXmlParser valueMetadataParser = new CMetadataXmlParser();
                                XMLReader xmlReader = valueMetadataSupport.init(valueMetadataParser);
                                while (rs.next()) {
                                    valueMetadataParser.setConceptBaseCode(rs.getString(1));
                                    valueMetadataSupport.parseAndFreeClob(xmlReader, rs.getClob(3));
                                    SAXParseException exception = valueMetadataParser.getException();
                                    if (exception != null) {
                                        throw exception;
                                    }
                                    ValueType valueType = valueMetadataParser.getValueType();
                                    ValueSet valueSet = valueMetadataParser.getValueSet();
                                    result.add(new PropertyDefinition(symbol, rs.getString(2), valueType, valueSet != null ? valueSet.getId() : null, rs.getString(4)));
                                }
                            }
                            return result;
                        } catch (SQLException | SAXParseException ex) {
                            throw new KnowledgeSourceReadException(ex);
                        }
                    }

                });
        }
    }

    private static ValueSet parseValueSet(String propId, String vses) {
        try (Reader reader = new StringReader(vses);
                CSVReader r = new CSVReader(reader, '\t')) {
            String[] readNext = r.readNext();
            ValueSetElement[] vsesArr = new ValueSetElement[readNext.length];
            for (int i = 0; i < readNext.length; i++) {
                try (Reader reader2 = new StringReader(readNext[i]);
                        CSVReader r2 = new CSVReader(reader2, '|')) {
                    String[] segments = r2.readNext();
                    vsesArr[i] = new ValueSetElement(NominalValue.getInstance(segments[0]), segments[1], null);
                }
            }
            return new ValueSet(propId, vsesArr, null);
        } catch (IOException ignore) {
            throw new AssertionError("Should never happen");
        }
    }

    private static ValueSet parseValueSetResource(String propositionId, String name, String hasHeader, String delimiter, String idCol, String displayNameCol, String descriptionCol) {
        boolean header = Boolean.parseBoolean(hasHeader);
        int id = Integer.parseInt(idCol) - 1;
        int displayName;
        if (displayNameCol != null) {
            displayName = Integer.parseInt(displayNameCol) - 1;
        } else {
            displayName = -1;
        }
        int description;
        if (descriptionCol != null) {
            description = Integer.parseInt(descriptionCol) - 1;
        } else {
            description = -1;
        }
        int maxIndex = Math.max(id, displayName);
        maxIndex = Math.max(maxIndex, description);
        try (Reader reader = new InputStreamReader(I2b2KnowledgeSourceBackend.class.getResourceAsStream(name));
                CSVReader r = new CSVReader(reader, delimiter.charAt(0))) {
            String[] cols;
            List<ValueSetElement> vsel = new ArrayList<>();
            boolean first = true;
            while ((cols = r.readNext()) != null) {
                if (first) {
                    first = false;
                } else if (cols.length - 1 >= maxIndex) {
                    vsel.add(new ValueSetElement(NominalValue.getInstance(cols[id]), displayName > -1 ? cols[displayName] : null, null));
                }
            }
            return new ValueSet(propositionId, vsel.toArray(new ValueSetElement[vsel.size()]), null);
        } catch (IOException ex) {
            throw new AssertionError("Should never happen");
        }
    }
    
}
