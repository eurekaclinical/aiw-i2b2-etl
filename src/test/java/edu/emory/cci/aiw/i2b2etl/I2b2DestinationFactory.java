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

import edu.emory.cci.aiw.i2b2etl.dest.I2b2Destination;
import edu.emory.cci.aiw.i2b2etl.dest.config.ConfigurationInitException;
import edu.emory.cci.aiw.i2b2etl.dest.config.xml.XmlFileConfiguration;
import java.io.File;
import java.io.IOException;
import org.protempa.dest.DestinationInitException;

/**
 *
 * @author Andrew Post
 */
public class I2b2DestinationFactory {
    
    private final File confXML;

    public I2b2DestinationFactory(String configResource) throws IOException {
        this.confXML = new I2b2ETLConfAsFile(configResource).getFile();
    }
    
    public I2b2Destination getInstance() throws DestinationInitException {
        return getInstance(false);
    }
    
    public I2b2Destination getInstance(boolean inferSupportedPropositionIds) throws DestinationInitException {
        try {
            return new I2b2Destination(new XmlFileConfiguration(this.confXML), inferSupportedPropositionIds);
        } catch (ConfigurationInitException ex) {
            throw new DestinationInitException(ex);
        }
    }
    
}
