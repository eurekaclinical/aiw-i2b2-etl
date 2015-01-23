package edu.emory.cci.aiw.i2b2etl.table;

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

import edu.emory.cci.aiw.i2b2etl.configuration.Data;
import edu.emory.cci.aiw.i2b2etl.configuration.DataSection;
import edu.emory.cci.aiw.i2b2etl.configuration.DataSpec;
import edu.emory.cci.aiw.i2b2etl.configuration.Settings;
import edu.emory.cci.aiw.i2b2etl.metadata.Metadata;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.arp.javautil.sql.ConnectionSpec;
import org.protempa.proposition.Proposition;
import org.protempa.proposition.UniqueId;
import org.protempa.proposition.value.AbsoluteTimeGranularity;
import org.protempa.proposition.value.AbsoluteTimeGranularityUtil;
import org.protempa.proposition.value.AbsoluteTimeUnit;
import org.protempa.proposition.value.DateValue;
import org.protempa.proposition.value.Value;

/**
 *
 * @author arpost
 */
public class PatientDimensionFactory extends DimensionFactory {
    private final Metadata metadata;
    private final PatientDimension patientDimension;
    private final PatientDimensionHandler patientDimensionHandler;
    private final PatientMappingHandler patientMappingHandler;
    private final Settings settings;

    public PatientDimensionFactory(Metadata metadata, Settings settings,
            Data obxSection, ConnectionSpec dataConnectionSpec) throws SQLException {
        super(obxSection);
        this.settings = settings;
        this.metadata = metadata;
        this.patientDimension = new PatientDimension();
        this.patientDimensionHandler = new PatientDimensionHandler(dataConnectionSpec);
        this.patientMappingHandler = new PatientMappingHandler(dataConnectionSpec);
    }
    
    public PatientDimension getInstance(String keyId, Proposition encounterProp,
            Map<UniqueId, Proposition> references) throws InvalidPatientRecordException, SQLException {
        String obxSectionStr = this.settings.getPatientDimensionMRN();
        DataSpec dataSpec = getData().get(obxSectionStr);
        List<UniqueId> uids = encounterProp.getReferences(dataSpec.getReferenceName());
        int size = uids.size();
        Logger logger = TableUtil.logger();
        if (size > 0) {
            if (size > 1) {
                logger.log(Level.WARNING,
                        "Multiple propositions with MRN property found for {0}, using only the first one",
                        encounterProp);
            }
            Proposition prop = references.get(uids.get(0));
            if (prop == null) {
                throw new InvalidPatientRecordException("Encounter's "
                        + dataSpec.getReferenceName()
                        + " reference points to a non-existant proposition");
            }
            Value val = prop.getProperty(dataSpec.getPropertyName());
            if (val != null) {
                Value zipCode = getField(
                        this.settings.getPatientDimensionZipCode(), encounterProp, references);
                Value maritalStatus = getField(
                        this.settings.getPatientDimensionMaritalStatus(), encounterProp, references);
                Value race = getField(
                        this.settings.getPatientDimensionRace(), encounterProp, references);
                Value birthdateVal = getField(
                        this.settings.getPatientDimensionBirthdate(), encounterProp, references);
                Value gender = getField(
                        this.settings.getPatientDimensionGender(), encounterProp, references);
                Value language = getField(
                        this.settings.getPatientDimensionLanguage(), encounterProp, references);
                Value religion = getField(
                        this.settings.getPatientDimensionReligion(), encounterProp, references);
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

                Long ageInYears = computeAgeInYears(birthdate);

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
                this.patientDimensionHandler.insert(patientDimension);
                this.patientMappingHandler.insert(patientDimension);
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
    
    public void close() throws SQLException {
        this.patientDimensionHandler.close();
        this.patientMappingHandler.close();
    }

    private Long computeAgeInYears(Date birthdate) {
        Long ageInYears;
        if (birthdate != null) {
            ageInYears = AbsoluteTimeGranularity.YEAR.distance(
                    AbsoluteTimeGranularityUtil.asPosition(birthdate),
                    AbsoluteTimeGranularityUtil.asPosition(new Date()),
                    AbsoluteTimeGranularity.YEAR,
                    AbsoluteTimeUnit.YEAR);
            this.metadata.markAge(ageInYears);
        } else {
            ageInYears = null;
        }
        return ageInYears;
    }

}
