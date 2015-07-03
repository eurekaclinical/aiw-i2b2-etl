package edu.emory.cci.aiw.i2b2etl.dest;

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
import edu.emory.cci.aiw.i2b2etl.ProtempaFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 *
 * @author Andrew Post
 */
public abstract class AbstractI2b2DestTest extends AbstractTest {

    private static ProtempaFactory protempaFactory;

    @BeforeClass
    public static void setUpClsCreateProtempaFactory() throws Exception {
        protempaFactory = new ProtempaFactory(getConfigFactory());
    }

    @AfterClass
    public static void tearDownClsCloseProtempaFactory() throws Exception {
        if (protempaFactory != null) {
            protempaFactory.close();
        }
    }

    public static ProtempaFactory getProtempaFactory() {
        return protempaFactory;
    }

}
