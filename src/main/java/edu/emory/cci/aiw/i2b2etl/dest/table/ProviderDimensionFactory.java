package edu.emory.cci.aiw.i2b2etl.dest.table;

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

import edu.emory.cci.aiw.i2b2etl.dest.metadata.Concept;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.conceptid.ConceptId;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.conceptid.PropDefConceptId;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.ConceptOperator;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.DataType;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.conceptid.InvalidConceptCodeException;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.Metadata;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.MetadataUtil;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.conceptid.SimpleConceptId;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.StringUtils;
import org.arp.javautil.sql.ConnectionSpec;
import org.protempa.proposition.Proposition;
import org.protempa.proposition.UniqueId;
import org.protempa.proposition.value.Value;

/**
 *
 * @author arpost
 */
public class ProviderDimensionFactory {
    private static final String PROVIDER_ID_PREFIX = MetadataUtil.DEFAULT_CONCEPT_ID_PREFIX_INTERNAL + "|Provider:";
    private static final String NOT_RECORDED_PROVIDER_ID = PROVIDER_ID_PREFIX + "NotRecorded";
    
    private final Metadata metadata;
    private final String qrhId;
    private final ProviderDimension providerDimension;
    private final ProviderDimensionHandler providerDimensionHandler;
    private final boolean skipProviderHierarchy;

    public ProviderDimensionFactory(String qrhId, Metadata metadata, ConnectionSpec dataConnectionSpec, boolean skipProviderHierarchy) throws SQLException {
        this.qrhId = qrhId;
        this.metadata = metadata;
        this.providerDimension = new ProviderDimension();
        this.providerDimensionHandler = new ProviderDimensionHandler(dataConnectionSpec);
        this.skipProviderHierarchy = skipProviderHierarchy;
    }
    
    public ProviderDimension getInstance(Proposition encounterProp, String fullNameReference, String fullNameProperty,
            String firstNameReference, String firstNameProperty,
            String middleNameReference, String middleNameProperty,
            String lastNameReference, String lastNameProperty,
            Map<UniqueId, Proposition> references) throws InvalidConceptCodeException, SQLException {
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
        ConceptId cid = SimpleConceptId.getInstance(id, this.metadata);
        Concept concept = this.metadata.getFromIdCache(cid);
        boolean found = concept != null;
        if (!found) {
            concept = new Concept(cid, null, this.metadata);
            concept.setSourceSystemCode(source);
            concept.setDisplayName(fullName);
            concept.setDataType(DataType.TEXT);
            concept.setInUse(true);
            concept.setFactTableColumn("provider_id");
            concept.setTableName("provider_dimension");
            concept.setColumnName("provider_path");
        }
        providerDimension.setConcept(concept);
        providerDimension.setSourceSystem(source);
        if (!found) {
            if (!skipProviderHierarchy) {
                this.metadata.addProvider(providerDimension);
            }
            providerDimensionHandler.insert(providerDimension);
        }
        return providerDimension;
    }
    
    public void close() throws SQLException {
        this.providerDimensionHandler.close();
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
                Logger logger = TableUtil.logger();
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
}
