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
package edu.emory.cci.aiw.i2b2etl;

import java.io.File;
import java.io.IOException;
import org.arp.javautil.io.IOUtil;
import org.protempa.Protempa;
import org.protempa.ProtempaException;
import org.protempa.SourceFactory;
import org.protempa.backend.Configurations;
import org.protempa.bconfigs.ini4j.INIConfigurations;

/**
 *
 * @author Andrew Post
 */
public class ProtempaFactory {
    public Protempa newInstance() throws IOException, ProtempaException {
        File config = IOUtil.resourceToFile(
                "/protempa-config/protege-h2-test-config", 
                "protege-h2-test-config", null);
        Configurations configurations = 
                new INIConfigurations(config.getParentFile());
        SourceFactory sourceFactory = 
                new SourceFactory(configurations, config.getName());
        
        // force the use of the H2 driver so we don't bother trying to load
        // others
        System.setProperty("protempa.dsb.relationaldatabase.sqlgenerator",
                "org.protempa.backend.dsb.relationaldb.H2SQLGenerator");
        
        return Protempa.newInstance(sourceFactory);
    }
}
