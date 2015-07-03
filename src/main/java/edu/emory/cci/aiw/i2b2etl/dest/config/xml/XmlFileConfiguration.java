package edu.emory.cci.aiw.i2b2etl.dest.config.xml;

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

import edu.emory.cci.aiw.i2b2etl.dest.config.Concepts;
import edu.emory.cci.aiw.i2b2etl.dest.config.Configuration;
import edu.emory.cci.aiw.i2b2etl.dest.config.ConfigurationInitException;
import edu.emory.cci.aiw.i2b2etl.dest.config.Data;
import edu.emory.cci.aiw.i2b2etl.dest.config.Database;
import edu.emory.cci.aiw.i2b2etl.dest.config.Settings;
import java.io.File;

/**
 *
 * @author Andrew Post
 */
public class XmlFileConfiguration implements Configuration {
    private final ConfigurationReader configurationReader;
    private final String name;
    
    public XmlFileConfiguration(File xmlFile) throws ConfigurationInitException {
        this.name = xmlFile.getName();
        this.configurationReader = new ConfigurationReader(xmlFile);
        this.configurationReader.read();
    }
    
    @Override
    public Concepts getConcepts() {
        return this.configurationReader.getConceptsSection();
    }

    @Override
    public Data getData() {
        return this.configurationReader.getDataSection();
    }

    @Override
    public Database getDatabase() {
        return this.configurationReader.getDatabaseSection();
    }

    @Override
    public Settings getSettings() {
        return this.configurationReader.getDictionarySection();
    }

    @Override
    public String getName() {
        return this.name;
    }

}
