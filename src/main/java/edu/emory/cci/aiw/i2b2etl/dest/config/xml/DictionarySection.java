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
package edu.emory.cci.aiw.i2b2etl.dest.config.xml;

import edu.emory.cci.aiw.i2b2etl.dest.RemoveMethod;
import edu.emory.cci.aiw.i2b2etl.dest.config.ConfigurationReadException;
import edu.emory.cci.aiw.i2b2etl.dest.config.Settings;
import edu.emory.cci.aiw.i2b2etl.dest.config.SettingsSupport;
import java.util.Set;
import java.util.TreeMap;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.w3c.dom.*;

/**
 *
 * @author Andrew Post
 */
final class DictionarySection extends ConfigurationSection implements Settings {
    private final TreeMap<String, String> dictionary = new TreeMap<>();
    private final SettingsSupport settingsSupport;

    DictionarySection() {
        this.settingsSupport = new SettingsSupport(this);
    }
    
    String get(String key) {
        return dictionary.get(key);
    }
    
    @Override
    protected void put(Node node) throws ConfigurationReadException {
        NamedNodeMap nnm = node.getAttributes();
        String key = readAttribute(nnm, "key", true);
        String value = readAttribute(nnm, "value", true);
        dictionary.put(key, value);
    }

    @Override
    protected String getNodeName() {
        return "entry";
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    @Override
    public String getProviderFullName() {
        return this.dictionary.get("providerFullName");
    }

    @Override
    public String getProviderFirstName() {
        return this.dictionary.get("providerFirstName");
    }

    @Override
    public String getProviderMiddleName() {
        return this.dictionary.get("providerMiddleName");
    }

    @Override
    public String getProviderLastName() {
       return this.dictionary.get("providerLastName");
    }

    @Override
    public String getVisitDimension() {
        return this.dictionary.get("visitDimension");
    }

    @Override
    public boolean getSkipProviderHierarchy() {
        return Boolean.parseBoolean(this.dictionary.get("skipProviderHierarchy"));
    }
    
    @Override
    public boolean getSkipDemographicsHierarchy() {
        return Boolean.parseBoolean(this.dictionary.get("skipDemographicsHierarchy"));
    }

    @Override
    public RemoveMethod getDataRemoveMethod() {
        String dataRemoveMethodString = this.dictionary.get("dataRemoveMethod");
        if (dataRemoveMethodString == null) {
            return RemoveMethod.TRUNCATE;
        } else {
            return RemoveMethod.valueOf(dataRemoveMethodString);
        }
    }

    @Override
    public RemoveMethod getMetaRemoveMethod() {
        String metaRemoveMethodString = this.dictionary.get("metaRemoveMethod");
        if (metaRemoveMethodString == null) {
            return RemoveMethod.TRUNCATE;
        } else {
            return RemoveMethod.valueOf(metaRemoveMethodString);
        }
    }

    @Override
    public String getSourceSystemCode() {
        return this.dictionary.get("sourcesystem_cd");
    }

    @Override
    public String getPatientDimensionMRN() {
        return this.dictionary.get("patientDimensionMRN");
    }

    @Override
    public String getPatientDimensionZipCode() {
        return this.dictionary.get("patientDimensionZipCode");
    }

    @Override
    public String getPatientDimensionMaritalStatus() {
        return this.dictionary.get("patientDimensionMaritalStatus");
    }

    @Override
    public String getPatientDimensionRace() {
        return this.dictionary.get("patientDimensionRace");
    }

    @Override
    public String getPatientDimensionBirthdate() {
        return this.dictionary.get("patientDimensionBirthdate");
    }

    @Override
    public String getPatientDimensionGender() {
        return this.dictionary.get("patientDimensionGender");
    }

    @Override
    public String getPatientDimensionLanguage() {
        return this.dictionary.get("patientDimensionLanguage");
    }

    @Override
    public String getPatientDimensionReligion() {
        return this.dictionary.get("patientDimensionReligion");
    }

    @Override
    public String getRootNodeName() {
        return this.dictionary.get("rootNodeName");
    }
    
    @Override
    public String getVisitDimensionId() {
        return this.dictionary.get("visitDimensionDecipheredId");
    }

    @Override
    public String getVisitDimensionInOut() {
        return this.dictionary.get("visitDimensionInOut");
    }
    
    @Override
    public String getAgeConceptCodePrefix() {
        return this.dictionary.get("ageConceptCodePrefix");
    }

    @Override
    public String getPatientDimensionVital() {
        return this.dictionary.get("patientDimensionVital");
    }

    @Override
    public String getMetaTableName() {
        return this.dictionary.get("metaTableName");
    }

    @Override
    public Set<String> getDimensionDataTypes() {
        return this.settingsSupport.getDimensionDataTypes();
    }

    @Override
    public String getPatientDimensionDeathDate() {
        return this.dictionary.get("patientDimensionDeathDate");
    }
    
    

}
