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
import java.beans.XMLDecoder;
import java.beans.XMLEncoder;
import java.io.BufferedInputStream;
import org.junit.Test;
import org.protempa.KnowledgeSourceReadException;
import org.protempa.PropositionDefinition;
import org.protempa.SourceFactory;
import org.protempa.bconfigs.ini4j.INIConfigurations;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import javax.naming.NamingException;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang.ArrayUtils;
import org.arp.javautil.arrays.Arrays;
import org.arp.javautil.sql.BasicDataSourceFactory;
import org.arp.javautil.sql.DataSourceInitialContextBinder;
import org.arp.javautil.test.ExpectedSetOfStringsReader;
import org.junit.AfterClass;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.BeforeClass;
import org.protempa.KnowledgeSource;
import org.protempa.PropertyDefinition;
import org.protempa.PropertyDefinitionBuilder;
import org.protempa.ProtempaException;
import org.protempa.SourceCloseException;
import org.protempa.valueset.ValueSet;
import org.protempa.backend.ksb.KnowledgeSourceBackend;
import org.protempa.valueset.ValueSetBuilder;

/**
 *
 */
public class I2b2KnowledgeSourceBackendTest {

    private static final String ICD9_250_ID = "ICD9:250";
    private static KnowledgeSource ks;
    private static KnowledgeSourceBackend ksb;
    private static BasicDataSource bds;
    /*
     * Binding for the BioPortal H2 database connection pool
     */
    private static DataSourceInitialContextBinder initialContextBinder;

    private static ExpectedSetOfStringsReader expectedReader;

    @BeforeClass
    public static void setUpCls() throws IOException, SQLException, NamingException, ProtempaException {
        // Create the bioportal ksb database
        File ksbDb = File.createTempFile("i2b2-dsb", ".db");
        try (Connection connection = DriverManager.getConnection("jdbc:h2:" + ksbDb.getAbsolutePath() + ";INIT=RUNSCRIPT FROM 'src/test/resources/i2b2.sql'")) {

        }

        // set up a data source/connection pool for accessing the BioPortal H2 database
        bds = BasicDataSourceFactory.getInstance();
        bds.setDriverClassName("org.h2.Driver");
        bds.setUrl("jdbc:h2:" + ksbDb.getAbsolutePath() + ";DEFAULT_ESCAPE='';INIT=RUNSCRIPT FROM 'src/test/resources/i2b2_temp_tables.sql';CACHE_SIZE=262400");
        bds.setMinIdle(1);
        bds.setMaxIdle(5);
        bds.setMaxTotal(5);
        initialContextBinder = new DataSourceInitialContextBinder();
        initialContextBinder.bind("I2b2DS", bds);
        SourceFactory sf = new SourceFactory(new INIConfigurations(new File("src/test/resources")),
                "i2b2-test-config");
        ks = sf.newKnowledgeSourceInstance();
        ksb = ks.getBackends()[0];
        expectedReader = new ExpectedSetOfStringsReader();
    }

    @AfterClass
    public static void tearDown() throws NamingException, SourceCloseException {
        try {
            ks.close();
            initialContextBinder.unbind(bds);
            initialContextBinder = null;
        } finally {
            if (initialContextBinder != null) {
                try {
                    initialContextBinder.unbind(bds);
                } catch (NamingException ignore) {
                }
            }
        }
    }

    @Test
    public void testReadPropositionDefinitionNotNull() throws KnowledgeSourceReadException {
        PropositionDefinition propDef = ksb.readPropositionDefinition(ICD9_250_ID);
        assertNotNull(propDef);
    }

    @Test
    public void testReadPropositionDefinitionId() throws KnowledgeSourceReadException {
        PropositionDefinition propDef = ksb.readPropositionDefinition(ICD9_250_ID);
        assertEquals("ICD9:250", propDef.getId());
    }

    @Test
    public void testReadPropositionDefinitionDisplayName() throws KnowledgeSourceReadException {
        PropositionDefinition propDef = ksb.readPropositionDefinition(ICD9_250_ID);
        assertEquals("Diabetes mellitus due to insulin receptor antibodies", propDef.getDisplayName());
    }

    @Test
    public void testReadPropositionDefinitionAbbrevDisplayName() throws KnowledgeSourceReadException {
        PropositionDefinition propDef = ksb.readPropositionDefinition(ICD9_250_ID);
        assertEquals("", propDef.getAbbreviatedDisplayName());
    }

    @Test
    public void testReadPropositionDefinitionInDataSource() throws KnowledgeSourceReadException {
        PropositionDefinition propDef = ksb.readPropositionDefinition(ICD9_250_ID);
        assertTrue(propDef.getInDataSource());
    }

    @Test
    public void testReadPropositionDefinitionInverseIsA() throws KnowledgeSourceReadException, IOException {
        PropositionDefinition propDef = ksb.readPropositionDefinition(ICD9_250_ID);
        Set<String> actual = Arrays.asSet(propDef.getChildren());
        Set<String> expected = expectedReader.readAsSet("/truth/testReadPropositionDefinitionInverseIsA", getClass());
        assertEquals(expected, actual);
    }

    @Test
    public void testReadPropositionDefinitionPropertyDefs() throws KnowledgeSourceReadException, IOException {
        PropositionDefinition propDef = ksb.readPropositionDefinition(ICD9_250_ID);
        Set<PropertyDefinitionBuilder> actual = new HashSet<>();
        for (PropertyDefinition propertyDef : propDef.getPropertyDefinitions()) {
            actual.add(new PropertyDefinitionBuilder(propertyDef));
        }
        Set<PropertyDefinitionBuilder> expected = new HashSet<>();
        try (XMLDecoder d = new XMLDecoder(
                new BufferedInputStream(
                        getClass().getResourceAsStream("/truth/testReadPropositionDefinitionPropertyDefs.xml")))) {
                    Integer size = (Integer) d.readObject();
                    for (int i = 0; i < size; i++) {
                        expected.add((PropertyDefinitionBuilder) d.readObject());
                    }
                }
                assertEquals(expected, actual);
    }

    @Test
    public void testReadIsA() throws KnowledgeSourceReadException {
        String[] isa = ksb.readIsA(ICD9_250_ID + ".1");
        assertArrayEquals(new String[]{ICD9_250_ID}, isa);
    }

    @Test
    public void testGetKnowledgeSourceSearchResults() throws KnowledgeSourceReadException, IOException {
        Set<String> actual = ksb.getKnowledgeSourceSearchResults("diabetes");
        Set<String> expected = expectedReader.readAsSet("/truth/testGetKnowledgeSourceSearchResults", getClass());
        assertEquals(expected, actual);
    }

    @Test
    public void testCollectPropIdDescendantsUsingAllNarrower() throws KnowledgeSourceReadException, IOException {
        Set<String> actual = new HashSet<>(ksb.collectPropIdDescendantsUsingAllNarrower(false, new String[]{"ICD9:Procedures"}));
        Set<String> expected = expectedReader.readAsSet("/truth/testCollectPropIdDescendantsUsingAllNarrower", getClass());
        assertEquals(expected, actual);
    }

    @Test
    public void testCollectPropPropDefDescendantsUsingAllNarrower() throws KnowledgeSourceReadException, IOException {
        Set<PropositionDefinition> actualPropDef = new HashSet<>(ksb.collectPropDefDescendantsUsingAllNarrower(false, new String[]{"ICD9:Procedures"}));
        Set<String> actual = toPropId(actualPropDef);
        Set<String> expected = expectedReader.readAsSet("/truth/testCollectPropDefDescendantsUsingAllNarrower", getClass());
        assertEquals(expected, actual);
    }

    @Test
    public void testCollectPropDefDescendantsUsingAllNarrowerInDataSource() throws KnowledgeSourceReadException, IOException {
        Set<PropositionDefinition> actualPropDef = new HashSet<>(ksb.collectPropDefDescendantsUsingAllNarrower(true, new String[]{"ICD9:Procedures"}));
        Set<String> actual = toPropId(actualPropDef);
        Set<String> expected = expectedReader.readAsSet("/truth/testCollectPropDefDescendantsUsingAllNarrowerInDataSource", getClass());
        assertEquals(expected, actual);
    }

    @Test
    public void testCollectPropDefDescendants285_22() throws KnowledgeSourceReadException, IOException {
        Set<PropositionDefinition> actualPropDef = new HashSet<>(ksb.collectPropDefDescendantsUsingAllNarrower(true, new String[]{"ICD9:285.22"}));
        Set<PropertyDefinitionBuilder> actual = new HashSet<>();
        for (PropositionDefinition pd : actualPropDef) {
            for (PropertyDefinition propDef : pd.getPropertyDefinitions()) {
                actual.add(new PropertyDefinitionBuilder(propDef));
            }
        }

        Set<PropertyDefinitionBuilder> expected = new HashSet<>();
        try (XMLDecoder d = new XMLDecoder(
                new BufferedInputStream(
                        getClass().getResourceAsStream("/truth/testCollectPropDefDescendants285.xml")))) {
            Integer size = (Integer) d.readObject();
            for (int i = 0; i < size; i++) {
                expected.add((PropertyDefinitionBuilder) d.readObject());
            }
        }
        assertEquals(expected, actual);
    }

    @Test
    public void testCollectPropIdDescendantsUsingInverseIsA() throws KnowledgeSourceReadException, IOException {
        Set<String> actual = new HashSet<>(ksb.collectPropIdDescendantsUsingInverseIsA(new String[]{"LAB:LabTest"}));
        Set<String> expected = expectedReader.readAsSet("/truth/testCollectPropIdDescendantsUsingInverseIsA", getClass());
        assertEquals(expected, actual);
    }

    @Test
    public void testCollectPropDefDescendantsUsingInverseIsA() throws KnowledgeSourceReadException, IOException {
        Set<PropositionDefinition> actualPropDef = new HashSet<>(ksb.collectPropDefDescendantsUsingInverseIsA(new String[]{"LAB:LabTest"}));
        Set<String> actual = toPropId(actualPropDef);
        Set<String> expected = expectedReader.readAsSet("/truth/testCollectPropDefDescendantsUsingInverseIsA", getClass());
        assertEquals(expected, actual);
    }
//
//    @Test
//    public void testCollectPropDefDescendantsUsingUsingAllNarrowerProperties() throws KnowledgeSourceReadException, IOException {
//        Collection<PropositionDefinition> collectPropDefDescendantsUsingInverseIsA = ksb.collectPropDefDescendantsUsingAllNarrower(false, new String[]{"ICD9:Diagnoses"});
//        Set<PropertyDefinitionBuilder> actual = new HashSet<>();
//        for (PropositionDefinition propDef : collectPropDefDescendantsUsingInverseIsA) {
//            for (PropertyDefinition pd : propDef.getPropertyDefinitions()) {
//                actual.add(new PropertyDefinitionBuilder(pd));
//            }
//        }
//
//        Set<PropertyDefinitionBuilder> expected = new HashSet<>();
//        try (XMLDecoder d = new XMLDecoder(
//                new BufferedInputStream(
//                        getClass().getResourceAsStream("/truth/testCollectPropDefDescendantsUsingAllNarrowerProperties.xml")))) {
//                    Integer size = (Integer) d.readObject();
//                    for (int i = 0; i < size; i++) {
//                        expected.add((PropertyDefinitionBuilder) d.readObject());
//                    }
//                }
//                assertEquals(expected, actual);
//    }

//    @Test
//    public void testCollectPropDefDescendantsUsingUsingAllNarrowerInDataSourceProperties() throws KnowledgeSourceReadException, IOException {
//        Collection<PropositionDefinition> collectPropDefDescendantsUsingInverseIsA = ksb.collectPropDefDescendantsUsingAllNarrower(true, new String[]{"ICD9:Diagnoses"});
//        Set<PropertyDefinitionBuilder> actual = new HashSet<>();
//        for (PropositionDefinition propDef : collectPropDefDescendantsUsingInverseIsA) {
//            for (PropertyDefinition pd : propDef.getPropertyDefinitions()) {
//                actual.add(new PropertyDefinitionBuilder(pd));
//            }
//        }
//
//        Set<PropertyDefinitionBuilder> expected = new HashSet<>();
//        try (XMLDecoder d = new XMLDecoder(
//                new BufferedInputStream(
//                        getClass().getResourceAsStream("/truth/testCollectPropDefDescendantsUsingAllNarrowerInDataSourceProperties.xml")))) {
//                    Integer size = (Integer) d.readObject();
//                    for (int i = 0; i < size; i++) {
//                        expected.add((PropertyDefinitionBuilder) d.readObject());
//                    }
//                }
//                assertEquals(expected, actual);
//    }
//
//    @Test
//    public void testCollectPropDefDescendantsUsingInverseIsAProperties() throws KnowledgeSourceReadException, IOException {
//        Collection<PropositionDefinition> collectPropDefDescendantsUsingInverseIsA = ksb.collectPropDefDescendantsUsingInverseIsA(new String[]{"ICD9:Diagnoses"});
//        Set<PropertyDefinitionBuilder> actual = new HashSet<>();
//        for (PropositionDefinition propDef : collectPropDefDescendantsUsingInverseIsA) {
//            for (PropertyDefinition pd : propDef.getPropertyDefinitions()) {
//                actual.add(new PropertyDefinitionBuilder(pd));
//            }
//        }
//
//        Set<PropertyDefinitionBuilder> expected = new HashSet<>();
//        try (XMLDecoder d = new XMLDecoder(
//                new BufferedInputStream(
//                        getClass().getResourceAsStream("/truth/testCollectPropDefDescendantsUsingInverseIsAProperties.xml")))) {
//                    Integer size = (Integer) d.readObject();
//                    for (int i = 0; i < size; i++) {
//                        expected.add((PropertyDefinitionBuilder) d.readObject());
//                    }
//                }
//                assertEquals(expected, actual);
//    }

    @Test
    public void testReadValueSetExistsCMetadataXML() throws KnowledgeSourceReadException {
        ValueSet valueSet = ksb.readValueSet("DXPRIORITY");
        assertNotNull(valueSet);
    }

    @Test
    public void testReadValueSetExistsNoCMetadataXML() throws KnowledgeSourceReadException {
        ValueSet valueSet = ksb.readValueSet("DXSOURCE");
        assertNotNull(valueSet);
    }

    @Test
    public void testReadValueSetContentCMetadataXML() throws KnowledgeSourceReadException {
        ValueSet actual = ksb.readValueSet("DXPRIORITY");
        if (actual != null) {
            ValueSetBuilder expected;
            try (XMLDecoder e = new XMLDecoder(
                    new BufferedInputStream(
                            getClass().getResourceAsStream("/truth/testReadValueSetContentCMetadataXML.xml")))) {
                        expected = (ValueSetBuilder) e.readObject();
                    }
                    assertEquals(expected, new ValueSetBuilder(actual));
        }
    }

    @Test
    public void testReadValueSetContentNoCMetadataXML() throws KnowledgeSourceReadException {
        ValueSet actual = ksb.readValueSet("DXSOURCE");
        if (actual != null) {
            ValueSetBuilder expected;
            try (XMLDecoder d = new XMLDecoder(new BufferedInputStream(getClass().getResourceAsStream("/truth/testReadValueSetContentNoCMetadataXML.xml")))) {
                expected = (ValueSetBuilder) d.readObject();
            }
            assertEquals(expected, new ValueSetBuilder(actual));
        }
    }

    @Test
    public void testReadAbstractionDefinition() throws KnowledgeSourceReadException {
        assertNull(ksb.readAbstractionDefinition(ICD9_250_ID));
    }

    @Test
    public void testReadContextDefinition() throws KnowledgeSourceReadException {
        assertNull(ksb.readContextDefinition(ICD9_250_ID));
    }

    @Test
    public void testReadTemporalPropositionDefinition() throws KnowledgeSourceReadException {
        assertNotNull(ksb.readTemporalPropositionDefinition(ICD9_250_ID));
    }

    @Test
    public void testReadAbstractedInto() throws KnowledgeSourceReadException {
        assertArrayEquals(ArrayUtils.EMPTY_STRING_ARRAY, ksb.readAbstractedInto(ICD9_250_ID));
    }

    @Test
    public void testReadInduces() throws KnowledgeSourceReadException {
        assertArrayEquals(ArrayUtils.EMPTY_STRING_ARRAY, ksb.readInduces(ICD9_250_ID));
    }

    @Test
    public void testReadSubContextsOf() throws KnowledgeSourceReadException {
        assertArrayEquals(ArrayUtils.EMPTY_STRING_ARRAY, ksb.readSubContextOfs(ICD9_250_ID));
    }

    private Set<String> toPropId(Set<PropositionDefinition> actualPropDef) {
        Set<String> actual = new HashSet<>();
        for (PropositionDefinition propDef : actualPropDef) {
            actual.add(propDef.getId());
        }
        return actual;
    }
}
