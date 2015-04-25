package edu.emory.cci.aiw.i2b2etl;

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

import java.beans.XMLDecoder;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.arp.javautil.arrays.Arrays;
import org.arp.javautil.test.ExpectedSetOfStringsReader;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;

/**
 *
 * @author Andrew Post
 */
public abstract class AbstractTest {
    private static ConfigurationFactory configFactory;
    private static ProtempaFactory protempaFactory;
    private static I2b2DestinationFactory i2b2DestFactory;
    
    private final ExpectedSetOfStringsReader expectedSetOfStringsReader;

    public AbstractTest() {
        this.expectedSetOfStringsReader = new ExpectedSetOfStringsReader();
    }
    
    @BeforeClass
    public static void setUpClsAbstractTest() throws Exception {
        configFactory = new ConfigurationFactory();
        protempaFactory = new ProtempaFactory(configFactory);
        i2b2DestFactory = new I2b2DestinationFactory();
    }
    
    @AfterClass
    public static void tearDownClsCloseConfigFactory() throws Exception {
        configFactory.close();
    }
    
    @AfterClass
    public static void tearDownClsCloseProtempaFactory() throws Exception {
        protempaFactory.close();
    }

    public static ConfigurationFactory getConfigFactory() {
        return configFactory;
    }

    public static ProtempaFactory getProtempaFactory() {
        return protempaFactory;
    }

    public static I2b2DestinationFactory getI2b2DestFactory() {
        return i2b2DestFactory;
    }
    
    public void assertEqualsStrings(String expectedResource, Set<String> actual) throws IOException {
        assertEquals(this.expectedSetOfStringsReader.readAsSet(expectedResource, getClass()), actual);
    }
    
    public void assertEqualsStrings(String expectedResource, String[] actual) throws IOException {
        assertEquals(this.expectedSetOfStringsReader.readAsSet(expectedResource, getClass()), Arrays.asSet(actual));
    }
    
    public void assertEqualsStrings(String expectedResource, Collection<String> actual) throws IOException {
        assertEquals(this.expectedSetOfStringsReader.readAsSet(expectedResource, getClass()), new HashSet<>(actual));
    }
    
    public <V> void assertEqualsSetOfObjects(String expectedResource, Set<V> actual) throws IOException {
        Set<V> expected = new HashSet<>();
        try (XMLDecoder d = new XMLDecoder(
                new BufferedInputStream(
                        getClass().getResourceAsStream(expectedResource)))) {
            Integer size = (Integer) d.readObject();
            for (int i = 0; i < size; i++) {
                expected.add((V) d.readObject());
            }
        }
        assertEquals(expected, actual);
    }
    
    public <V> void assertEqualsObjects(String expectedResource, V actual) throws IOException {
        try (XMLDecoder d = new XMLDecoder(
                new BufferedInputStream(
                        getClass().getResourceAsStream(expectedResource)))) {
            V expected = (V) d.readObject();
            assertEquals(expected, actual);
        }
    }
    
}
