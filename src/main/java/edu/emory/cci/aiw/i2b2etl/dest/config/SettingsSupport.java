package edu.emory.cci.aiw.i2b2etl.dest.config;

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

import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author Andrew Post
 */
public class SettingsSupport {
    private final Settings settings;
    
    public SettingsSupport(Settings settings) {
        if (settings == null) {
            throw new IllegalArgumentException("settings cannot be null");
        }
        this.settings = settings;
    }

    public Settings getSettings() {
        return settings;
    }
    
    public Set<String> getDimensionDataTypes() {
        Set<String> result = new HashSet<>();
        result.add(this.settings.getPatientDimensionMRN());
        result.add(this.settings.getPatientDimensionGender());
        result.add(this.settings.getPatientDimensionRace());
        result.add(this.settings.getPatientDimensionMaritalStatus());
        result.add(this.settings.getPatientDimensionLanguage());
        result.add(this.settings.getPatientDimensionReligion());
        result.add(this.settings.getPatientDimensionBirthdate());
        result.add(this.settings.getVisitDimensionDecipheredId());
        result.add(this.settings.getProviderFullName());
        result.add(this.settings.getProviderMiddleName());
        result.add(this.settings.getProviderLastName());
        result.add(this.settings.getProviderFirstName());
        return result;
    }
}
