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
import org.junit.Test;
import org.protempa.PropositionDefinition;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.ArrayUtils;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.protempa.PropertyDefinitionBuilder;
import org.protempa.valueset.ValueSet;

/**
 *
 */
public class I2b2KnowledgeSourceBackendTest extends AbstractKSBTest {

    private static final String ICD9_250_ID = "ICD9:250";

    @Test
    public void testReadPropositionDefinitionNotNull() throws Exception {
        PropositionDefinition propDef = readPropositionDefinition(ICD9_250_ID);
        assertNotNull(propDef);
    }
    
    @Test
    public void testReadPropositionsDefinitionNotNull() throws Exception {
        List<PropositionDefinition> propDefs = readPropositionDefinitions(new String[]{ICD9_250_ID});
        assertEquals(1, propDefs.size());
    }

    @Test
    public void testReadPropositionDefinitionId() throws Exception {
        PropositionDefinition propDef = readPropositionDefinition(ICD9_250_ID);
        assertEquals("ICD9:250", propDef.getId());
    }

    @Test
    public void testReadPropositionDefinitionDisplayName() throws Exception {
        PropositionDefinition propDef = readPropositionDefinition(ICD9_250_ID);
        assertEquals("Diabetes mellitus due to insulin receptor antibodies", propDef.getDisplayName());
    }

    @Test
    public void testReadPropositionDefinitionAbbrevDisplayName() throws Exception {
        PropositionDefinition propDef = readPropositionDefinition(ICD9_250_ID);
        assertEquals("", propDef.getAbbreviatedDisplayName());
    }

    @Test
    public void testReadPropositionDefinitionInDataSource() throws Exception {
        PropositionDefinition propDef = readPropositionDefinition(ICD9_250_ID);
        assertTrue(propDef.getInDataSource());
    }

    @Test
    public void testReadPropositionDefinitionInverseIsA() throws Exception {
        PropositionDefinition propDef = readPropositionDefinition(ICD9_250_ID);
        assertEqualsStrings("/truth/testReadPropositionDefinitionInverseIsA", propDef.getChildren());
    }

    @Test
    public void testReadPropositionDefinitionPropertyDefs() throws Exception {
        PropositionDefinition propDef = readPropositionDefinition(ICD9_250_ID);
        Set<PropertyDefinitionBuilder> actual = collectPropertyDefinitionBuilders(propDef);
        assertEqualsSetOfObjects("/truth/testReadPropositionDefinitionPropertyDefs.xml", actual);
    }

    @Test
    public void testReadIsA() throws Exception {
        String[] isa = readIsA(ICD9_250_ID + ".1");
        assertArrayEquals(new String[]{ICD9_250_ID}, isa);
    }

    @Test
    public void testGetKnowledgeSourceSearchResults() throws Exception {
        assertEqualsStrings("/truth/testGetKnowledgeSourceSearchResults", 
                getKnowledgeSourceSearchResults("diabetes"));
    }

    @Test
    public void testCollectPropIdDescendantsUsingAllNarrower() throws Exception {
        assertEqualsStrings(
                "/truth/testCollectPropIdDescendantsUsingAllNarrower", 
                collectPropIdDescendantsUsingAllNarrower(false, new String[]{"ICD9:Procedures"}));
    }

    @Test
    public void testCollectPropPropDefDescendantsUsingAllNarrower() throws Exception {
        Set<PropositionDefinition> actualPropDef = new HashSet<>(collectPropDefDescendantsUsingAllNarrower(false, new String[]{"ICD9:Procedures"}));
        assertEqualsStrings(
                "/truth/testCollectPropDefDescendantsUsingAllNarrower", 
                toPropId(actualPropDef));
    }

    @Test
    public void testCollectPropDefDescendantsUsingAllNarrowerInDataSource() throws Exception {
        Set<PropositionDefinition> actualPropDef = new HashSet<>(collectPropDefDescendantsUsingAllNarrower(true, new String[]{"ICD9:Procedures"}));
        assertEqualsStrings(
                "/truth/testCollectPropDefDescendantsUsingAllNarrowerInDataSource", 
                toPropId(actualPropDef));
    }

    @Test
    public void testCollectPropDefDescendants285_22() throws Exception {
        Set<PropositionDefinition> actualPropDef = new HashSet<>(collectPropDefDescendantsUsingAllNarrower(true, new String[]{"ICD9:285.22"}));
        Set<PropertyDefinitionBuilder> actual = collectPropertyDefinitionBuilders(actualPropDef);
        assertEqualsSetOfObjects("/truth/testCollectPropDefDescendants285.xml", actual);
    }

    @Test
    public void testCollectPropIdDescendantsUsingInverseIsA() throws Exception {
        assertEqualsStrings(
                "/truth/testCollectPropIdDescendantsUsingInverseIsA", 
                collectPropIdDescendantsUsingInverseIsA(new String[]{"LAB:LabTest"}));
    }

    @Test
    public void testCollectPropDefDescendantsUsingInverseIsA() throws Exception {
        Set<PropositionDefinition> actualPropDef = new HashSet<>(collectPropDefDescendantsUsingInverseIsA(new String[]{"LAB:LabTest"}));
        assertEqualsStrings("/truth/testCollectPropDefDescendantsUsingInverseIsA", toPropId(actualPropDef));
    }

    @Test
    public void testCollectPropDefDescendantsUsingUsingAllNarrowerProperties() throws Exception {
        Collection<PropositionDefinition> collectPropDefDescendantsUsingAllNarrower = collectPropDefDescendantsUsingAllNarrower(false, new String[]{"ICD9:Diagnoses"});
        Set<PropertyDefinitionBuilder> actual = collectPropertyDefinitionBuilders(collectPropDefDescendantsUsingAllNarrower);
        assertEqualsSetOfObjects("/truth/testCollectPropDefDescendantsUsingAllNarrowerProperties.xml", actual);
    }

    @Test
    public void testCollectPropDefDescendantsUsingUsingAllNarrowerInDataSourceProperties() throws Exception {
        Collection<PropositionDefinition> collectPropDefDescendantsUsingAllNarrowerInDataSource = collectPropDefDescendantsUsingAllNarrower(true, new String[]{"ICD9:Diagnoses"});
        Set<PropertyDefinitionBuilder> actual = collectPropertyDefinitionBuilders(collectPropDefDescendantsUsingAllNarrowerInDataSource);
        assertEqualsSetOfObjects("/truth/testCollectPropDefDescendantsUsingAllNarrowerInDataSourceProperties.xml", actual);
    }

    @Test
    public void testCollectPropDefDescendantsUsingInverseIsAProperties() throws Exception {
        Collection<PropositionDefinition> collectPropDefDescendantsUsingInverseIsA = collectPropDefDescendantsUsingInverseIsA(new String[]{"ICD9:Diagnoses"});
        Set<PropertyDefinitionBuilder> actual = collectPropertyDefinitionBuilders(collectPropDefDescendantsUsingInverseIsA);
        assertEqualsSetOfObjects("/truth/testCollectPropDefDescendantsUsingInverseIsAProperties.xml", actual);
    }
    
    @Test
    public void testReadValueSetExistsCMetadataXML() throws Exception {
        ValueSet valueSet = readValueSet("ICD9:Diagnoses^DXPRIORITY");
        assertNotNull(valueSet);
    }

    @Test
    public void testReadValueSetExistsNoCMetadataXML() throws Exception {
        ValueSet valueSet = readValueSet("ICD9:Diagnoses^DXSOURCE");
        assertNotNull(valueSet);
    }

    @Test
    public void testReadValueSetContentCMetadataXML() throws Exception {
        ValueSet actual = readValueSet("ICD9:Diagnoses^DXPRIORITY");
        assertEqualsObjects("/truth/testReadValueSetContentCMetadataXML.xml", actual.asBuilder());
    }

    @Test
    public void testReadValueSetContentNoCMetadataXML() throws Exception {
        ValueSet actual = readValueSet("ICD9:Diagnoses^DXSOURCE");
        assertEqualsObjects("/truth/testReadValueSetContentNoCMetadataXML.xml", actual.asBuilder());
    }

    @Test
    public void testReadAbstractionDefinition() throws Exception {
        assertNull(readAbstractionDefinition(ICD9_250_ID));
    }

    @Test
    public void testReadContextDefinition() throws Exception {
        assertNull(readContextDefinition(ICD9_250_ID));
    }

    @Test
    public void testReadTemporalPropositionDefinition() throws Exception {
        assertNotNull(readTemporalPropositionDefinition(ICD9_250_ID));
    }

    @Test
    public void testReadAbstractedInto() throws Exception {
        assertArrayEquals(ArrayUtils.EMPTY_STRING_ARRAY, readAbstractedInto(ICD9_250_ID));
    }

    @Test
    public void testReadInduces() throws Exception {
        assertArrayEquals(ArrayUtils.EMPTY_STRING_ARRAY, readInduces(ICD9_250_ID));
    }

    @Test
    public void testReadSubContextsOf() throws Exception {
        assertArrayEquals(ArrayUtils.EMPTY_STRING_ARRAY, readSubContextOfs(ICD9_250_ID));
    }

}
