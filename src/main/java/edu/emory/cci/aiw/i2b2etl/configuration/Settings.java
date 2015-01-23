/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.emory.cci.aiw.i2b2etl.configuration;

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

import edu.emory.cci.aiw.i2b2etl.RemoveMethod;
import java.util.Set;

/**
 *
 * @author arpost
 */
public interface Settings {
    String getProviderFullName();
    String getProviderFirstName();
    String getProviderMiddleName();
    String getProviderLastName();
    String getVisitDimension();
    boolean getSkipProviderHierarchy();
    boolean getSkipDemographicsHierarchy();
    RemoveMethod getDataRemoveMethod();
    RemoveMethod getMetaRemoveMethod();
    String getSourceSystemCode();
    String getPatientDimensionMRN();
    String getPatientDimensionZipCode();
    String getPatientDimensionMaritalStatus();
    String getPatientDimensionRace();
    String getPatientDimensionBirthdate();
    String getPatientDimensionGender();
    String getPatientDimensionLanguage();
    String getPatientDimensionReligion();
    String getPatientDimensionVital();
    String getRootNodeName();
    String getVisitDimensionDecipheredId();
    String getAgeConceptCodePrefix();
    String getMetaTableName();
    Set<String> getPatientDimensionDataTypes();
}
